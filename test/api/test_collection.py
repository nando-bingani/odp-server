import re
import uuid
from datetime import datetime
from random import randint

import pytest
from sqlalchemy import select

from odp.const import DOI_REGEX, ODPScope
from odp.db.models import Collection, CollectionAudit, User
from test import TestSession
from test.api import all_scopes, all_scopes_excluding
from test.api.assertions import assert_conflict, assert_forbidden, assert_new_timestamp, assert_not_found, assert_ok_null, assert_unprocessable
from test.api.assertions.tags import (
    assert_tag_instance_audit_log,
    assert_tag_instance_audit_log_empty,
    assert_tag_instance_db_state,
    assert_tag_instance_output,
    keyword_tag_args,
    new_generic_tag,
)
from test.api.conftest import try_skip_collection_constraint
from test.factories import (
    CollectionFactory,
    CollectionTagFactory,
    FactorySession,
    ProviderFactory,
    RecordFactory,
)


@pytest.fixture
def collection_batch():
    """Create and commit a batch of Collection instances."""
    return [CollectionFactory() for _ in range(randint(3, 5))]


def collection_build(**id):
    """Build and return an uncommitted Collection instance.
    Referenced provider is however committed."""
    return CollectionFactory.build(
        **id,
        provider=(provider := ProviderFactory()),
        provider_id=provider.id,
    )


def role_ids(collection):
    return tuple(sorted(role.id for role in collection.roles if role.id != 'odp.test.role'))


def assert_db_state(collections):
    """Verify that the DB collection table contains the given collection batch."""
    result = TestSession.execute(select(Collection)).scalars().all()
    result.sort(key=lambda c: c.id)
    collections.sort(key=lambda c: c.id)
    assert len(result) == len(collections)
    for n, row in enumerate(result):
        assert row.id == collections[n].id
        assert row.key == collections[n].key
        assert row.name == collections[n].name
        assert row.doi_key == collections[n].doi_key
        assert row.provider_id == collections[n].provider_id
        assert_new_timestamp(row.timestamp)
        assert role_ids(row) == role_ids(collections[n])


def assert_audit_log(command, collection, grant_type):
    result = TestSession.execute(select(CollectionAudit)).scalar_one()
    assert result.client_id == 'odp.test.client'
    assert result.user_id == ('odp.test.user' if grant_type == 'authorization_code' else None)
    assert result.command == command
    assert_new_timestamp(result.timestamp)
    assert result._id == collection.id
    assert result._key == collection.key
    assert result._name == collection.name
    assert result._doi_key == collection.doi_key
    assert result._provider_id == collection.provider_id


def assert_no_audit_log():
    assert TestSession.execute(select(CollectionAudit)).first() is None


def assert_json_collection_result(response, json, collection):
    """Verify that the API result matches the given collection object."""
    assert response.status_code == 200
    assert json['id'] == collection.id
    assert json['key'] == collection.key
    assert json['name'] == collection.name
    assert json['doi_key'] == collection.doi_key
    assert json['provider_id'] == collection.provider_id
    assert json['provider_key'] == collection.provider.key
    assert tuple(sorted(r for r in json['role_ids'] if r != 'odp.test.role')) == role_ids(collection)
    assert_new_timestamp(datetime.fromisoformat(json['timestamp']))


def assert_json_collection_results(response, json, collections):
    """Verify that the API result list matches the given collection batch."""
    items = json['items']
    assert json['total'] == len(items) == len(collections)
    items.sort(key=lambda i: i['id'])
    collections.sort(key=lambda c: c.id)
    for n, collection in enumerate(collections):
        assert_json_collection_result(response, items[n], collection)


def assert_doi_result(response, collection):
    assert response.status_code == 200
    assert re.match(DOI_REGEX, doi := response.json()) is not None
    prefix, _, suffix = doi.rpartition('.')
    assert prefix == f'10.15493/{collection.doi_key}'
    assert re.match(r'^\d{8}$', suffix) is not None


@pytest.mark.require_scope(ODPScope.COLLECTION_READ)
def test_list_collections(api, collection_batch, scopes, collection_constraint):
    authorized = ODPScope.COLLECTION_READ in scopes

    try_skip_collection_constraint(api.grant_type, collection_constraint)

    if collection_constraint == 'collection_any':
        authorized_collections = None  # => all
        expected_result_batch = collection_batch
    elif collection_constraint == 'collection_match':
        authorized_collections = collection_batch[1:3]
        expected_result_batch = authorized_collections
    elif collection_constraint == 'collection_mismatch':
        authorized_collections = [CollectionFactory()]
        expected_result_batch = authorized_collections
        collection_batch += authorized_collections

    r = api(scopes, user_collections=authorized_collections).get('/collection/')

    if authorized:
        assert_json_collection_results(r, r.json(), expected_result_batch)
    else:
        assert_forbidden(r)

    assert_db_state(collection_batch)
    assert_no_audit_log()


@pytest.mark.require_scope(ODPScope.COLLECTION_READ)
def test_get_collection(api, collection_batch, scopes, collection_constraint):
    authorized = ODPScope.COLLECTION_READ in scopes and collection_constraint in ('collection_any', 'collection_match')

    try_skip_collection_constraint(api.grant_type, collection_constraint)

    if collection_constraint == 'collection_any':
        authorized_collections = None  # => all
    elif collection_constraint == 'collection_match':
        authorized_collections = collection_batch[1:3]
    elif collection_constraint == 'collection_mismatch':
        authorized_collections = collection_batch[0:2]

    r = api(scopes, user_collections=authorized_collections).get(f'/collection/{collection_batch[2].id}')

    if authorized:
        assert_json_collection_result(r, r.json(), collection_batch[2])
    else:
        assert_forbidden(r)

    assert_db_state(collection_batch)
    assert_no_audit_log()


def test_get_collection_not_found(api, collection_batch, collection_constraint):
    scopes = [ODPScope.COLLECTION_READ]
    authorized = collection_constraint == 'collection_any'

    try_skip_collection_constraint(api.grant_type, collection_constraint)

    if collection_constraint == 'collection_any':
        authorized_collections = None  # => all
    else:
        authorized_collections = collection_batch[1:3]

    r = api(scopes, user_collections=authorized_collections).get('/collection/foo')

    if authorized:
        assert_not_found(r)
    else:
        assert_forbidden(r)

    assert_db_state(collection_batch)
    assert_no_audit_log()


@pytest.mark.require_scope(ODPScope.COLLECTION_ADMIN)
def test_create_collection(api, collection_batch, scopes, collection_constraint):
    # note that collection-specific auth will never allow creating a new collection
    authorized = ODPScope.COLLECTION_ADMIN in scopes and collection_constraint == 'collection_any'

    try_skip_collection_constraint(api.grant_type, collection_constraint)

    if collection_constraint == 'collection_any':
        authorized_collections = None  # => all
    else:
        authorized_collections = collection_batch[1:3]

    modified_collection_batch = collection_batch + [collection := collection_build()]

    r = api(scopes, user_collections=authorized_collections).post('/collection/', json=dict(
        key=collection.key,
        name=collection.name,
        doi_key=collection.doi_key,
        provider_id=collection.provider_id,
    ))

    if authorized:
        collection.id = r.json().get('id')
        assert_json_collection_result(r, r.json(), collection)
        assert_db_state(modified_collection_batch)
        assert_audit_log('insert', collection, api.grant_type)
    else:
        assert_forbidden(r)
        assert_db_state(collection_batch)
        assert_no_audit_log()


def test_create_collection_conflict(api, collection_batch, collection_constraint):
    scopes = [ODPScope.COLLECTION_ADMIN]
    authorized = collection_constraint == 'collection_any'

    try_skip_collection_constraint(api.grant_type, collection_constraint)

    if collection_constraint == 'collection_any':
        authorized_collections = None  # => all
    else:
        authorized_collections = collection_batch[1:3]

    collection = collection_build(key=collection_batch[2].key)

    r = api(scopes, user_collections=authorized_collections).post('/collection/', json=dict(
        key=collection.key,
        name=collection.name,
        doi_key=collection.doi_key,
        provider_id=collection.provider_id,
    ))

    if authorized:
        assert_conflict(r, 'Collection key is already in use')
    else:
        assert_forbidden(r)

    assert_db_state(collection_batch)
    assert_no_audit_log()


@pytest.mark.require_scope(ODPScope.COLLECTION_ADMIN)
def test_update_collection(api, collection_batch, scopes, collection_constraint):
    authorized = ODPScope.COLLECTION_ADMIN in scopes and collection_constraint in ('collection_any', 'collection_match')

    try_skip_collection_constraint(api.grant_type, collection_constraint)

    if collection_constraint == 'collection_any':
        authorized_collections = None  # => all
    elif collection_constraint == 'collection_match':
        authorized_collections = collection_batch[1:3]
    elif collection_constraint == 'collection_mismatch':
        authorized_collections = collection_batch[0:2]

    modified_collection_batch = collection_batch.copy()
    modified_collection_batch[2] = (collection := collection_build(
        id=collection_batch[2].id,
    ))

    r = api(scopes, user_collections=authorized_collections).put(f'/collection/{collection.id}', json=dict(
        key=collection.key,
        name=collection.name,
        doi_key=collection.doi_key,
        provider_id=collection.provider_id,
    ))

    if authorized:
        assert_ok_null(r)
        assert_db_state(modified_collection_batch)
        assert_audit_log('update', collection, api.grant_type)
    else:
        assert_forbidden(r)
        assert_db_state(collection_batch)
        assert_no_audit_log()


def test_update_collection_not_found(api, collection_batch, collection_constraint):
    scopes = [ODPScope.COLLECTION_ADMIN]
    authorized = collection_constraint == 'collection_any'

    try_skip_collection_constraint(api.grant_type, collection_constraint)

    if collection_constraint == 'collection_any':
        authorized_collections = None  # => all
    else:
        authorized_collections = collection_batch[1:3]

    collection = collection_build(id=str(uuid.uuid4()))

    r = api(scopes, user_collections=authorized_collections).put(f'/collection/{collection.id}', json=dict(
        key=collection.key,
        name=collection.name,
        doi_key=collection.doi_key,
        provider_id=collection.provider_id,
    ))

    if authorized:
        assert_not_found(r)
    else:
        assert_forbidden(r)

    assert_db_state(collection_batch)
    assert_no_audit_log()


def test_update_collection_conflict(api, collection_batch, collection_constraint):
    scopes = [ODPScope.COLLECTION_ADMIN]
    authorized = collection_constraint in ('collection_any', 'collection_match')

    try_skip_collection_constraint(api.grant_type, collection_constraint)

    if collection_constraint == 'collection_any':
        authorized_collections = None  # => all
    elif collection_constraint == 'collection_match':
        authorized_collections = collection_batch[1:3]
    elif collection_constraint == 'collection_mismatch':
        authorized_collections = collection_batch[0:2]

    collection = collection_build(
        id=collection_batch[2].id,
        key=collection_batch[0].key,
    )

    r = api(scopes, user_collections=authorized_collections).put(f'/collection/{collection.id}', json=dict(
        key=collection.key,
        name=collection.name,
        doi_key=collection.doi_key,
        provider_id=collection.provider_id,
    ))

    if authorized:
        assert_conflict(r, 'Collection key is already in use')
    else:
        assert_forbidden(r)

    assert_db_state(collection_batch)
    assert_no_audit_log()


@pytest.fixture(params=[True, False])
def has_record(request):
    return request.param


@pytest.mark.require_scope(ODPScope.COLLECTION_ADMIN)
def test_delete_collection(api, collection_batch, scopes, collection_constraint, has_record):
    authorized = ODPScope.COLLECTION_ADMIN in scopes and collection_constraint in ('collection_any', 'collection_match')

    try_skip_collection_constraint(api.grant_type, collection_constraint)

    if collection_constraint == 'collection_any':
        authorized_collections = None  # => all
    elif collection_constraint == 'collection_match':
        authorized_collections = collection_batch[1:3]
    elif collection_constraint == 'collection_mismatch':
        authorized_collections = collection_batch[0:2]

    modified_collection_batch = collection_batch.copy()
    deleted_collection = modified_collection_batch[2]
    del modified_collection_batch[2]

    if has_record:
        RecordFactory(collection=collection_batch[2])

    r = api(scopes, user_collections=authorized_collections).delete(f'/collection/{collection_batch[2].id}')

    if authorized:
        if has_record:
            assert_unprocessable(r, 'A non-empty collection cannot be deleted.')
            assert_db_state(collection_batch)
            assert_no_audit_log()
        else:
            assert_ok_null(r)
            assert_db_state(modified_collection_batch)
            assert_audit_log('delete', deleted_collection, api.grant_type)
    else:
        assert_forbidden(r)
        assert_db_state(collection_batch)
        assert_no_audit_log()


def test_delete_collection_not_found(api, collection_batch, collection_constraint):
    scopes = [ODPScope.COLLECTION_ADMIN]
    authorized = collection_constraint == 'collection_any'

    try_skip_collection_constraint(api.grant_type, collection_constraint)

    if collection_constraint == 'collection_any':
        authorized_collections = None  # => all
    else:
        authorized_collections = collection_batch[1:3]

    r = api(scopes, user_collections=authorized_collections).delete('/collection/foo')

    if authorized:
        assert_not_found(r)
    else:
        assert_forbidden(r)

    assert_db_state(collection_batch)
    assert_no_audit_log()


@pytest.mark.require_scope(ODPScope.COLLECTION_READ)
def test_get_new_doi(api, collection_batch, scopes, collection_constraint):
    authorized = ODPScope.COLLECTION_READ in scopes and collection_constraint in ('collection_any', 'collection_match')

    try_skip_collection_constraint(api.grant_type, collection_constraint)

    if collection_constraint == 'collection_any':
        authorized_collections = None  # => all
    elif collection_constraint == 'collection_match':
        authorized_collections = collection_batch[1:3]
    elif collection_constraint == 'collection_mismatch':
        authorized_collections = collection_batch[0:2]

    r = api(scopes, user_collections=authorized_collections).get(f'/collection/{(collection := collection_batch[2]).id}/doi/new')

    if authorized:
        if collection.doi_key:
            assert_doi_result(r, collection)
        else:
            assert_unprocessable(r, 'The collection does not have a DOI key')
    else:
        assert_forbidden(r)

    assert_db_state(collection_batch)
    assert_no_audit_log()


@pytest.mark.require_scope(ODPScope.COLLECTION_FREEZE)  # scope associated with the generic tag
def test_tag_collection(api, collection_batch, scopes, collection_constraint, tag_cardinality):
    authorized = ODPScope.COLLECTION_FREEZE in scopes and collection_constraint in ('collection_any', 'collection_match')

    try_skip_collection_constraint(api.grant_type, collection_constraint)

    if collection_constraint == 'collection_any':
        authorized_collections = None  # => all
    elif collection_constraint == 'collection_match':
        authorized_collections = collection_batch[1:3]
    elif collection_constraint == 'collection_mismatch':
        authorized_collections = collection_batch[0:2]

    client = api(scopes, user_collections=authorized_collections)
    tag = new_generic_tag('collection', tag_cardinality)

    # TAG 1
    r = client.post(
        f'/collection/{(collection_id := collection_batch[2].id)}/tag',
        json=(collection_tag_1 := dict(
            tag_id=tag.id,
            data={'comment': 'test1'},
            cardinality=tag_cardinality,
            public=tag.public,
        ) | keyword_tag_args(tag.vocabulary, 0)))

    if authorized:
        assert_tag_instance_output(r, collection_tag_1, api.grant_type)
        assert_tag_instance_db_state('collection', api.grant_type, collection_id, collection_tag_1)
        assert_tag_instance_audit_log(
            'collection', api.grant_type,
            dict(command='insert', object_id=collection_id, tag_instance=collection_tag_1),
        )

        # TAG 2
        r = client.post(
            f'/collection/{(collection_id := collection_batch[2].id)}/tag',
            json=(collection_tag_2 := dict(
                tag_id=tag.id,
                data=collection_tag_1['data'] if tag.vocabulary else {'comment': 'test2'},
                cardinality=tag_cardinality,
                public=tag.public,
            ) | keyword_tag_args(tag.vocabulary, 1)))

        assert_tag_instance_output(r, collection_tag_2, api.grant_type)
        if tag_cardinality in ('one', 'user'):
            assert_tag_instance_db_state('collection', api.grant_type, collection_id, collection_tag_2)
            assert_tag_instance_audit_log(
                'collection', api.grant_type,
                dict(command='insert', object_id=collection_id, tag_instance=collection_tag_1),
                dict(command='update', object_id=collection_id, tag_instance=collection_tag_2),
            )
        elif tag_cardinality == 'multi':
            assert_tag_instance_db_state('collection', api.grant_type, collection_id, collection_tag_1, collection_tag_2)
            assert_tag_instance_audit_log(
                'collection', api.grant_type,
                dict(command='insert', object_id=collection_id, tag_instance=collection_tag_1),
                dict(command='insert', object_id=collection_id, tag_instance=collection_tag_2),
            )

        # TAG 3 - different client/user
        client = api(
            scopes,
            user_collections=authorized_collections,
            client_id='testclient2',
            role_id='testrole2',
            user_id='testuser2',
            user_email='test2@saeon.ac.za',
        )
        r = client.post(
            f'/collection/{(collection_id := collection_batch[2].id)}/tag',
            json=(collection_tag_3 := dict(
                tag_id=tag.id,
                data=collection_tag_1['data'] if tag.vocabulary else {'comment': 'test3'},
                cardinality=tag_cardinality,
                public=tag.public,
                auth_client_id='testclient2',
                auth_user_id='testuser2' if api.grant_type == 'authorization_code' else None,
                user_id='testuser2' if api.grant_type == 'authorization_code' else None,
                user_email='test2@saeon.ac.za' if api.grant_type == 'authorization_code' else None,
            ) | keyword_tag_args(tag.vocabulary, 2)))

        assert_tag_instance_output(r, collection_tag_3, api.grant_type)
        if tag_cardinality == 'one':
            assert_tag_instance_db_state('collection', api.grant_type, collection_id, collection_tag_3)
            assert_tag_instance_audit_log(
                'collection', api.grant_type,
                dict(command='insert', object_id=collection_id, tag_instance=collection_tag_1),
                dict(command='update', object_id=collection_id, tag_instance=collection_tag_2),
                dict(command='update', object_id=collection_id, tag_instance=collection_tag_3),
            )
        elif tag_cardinality == 'user':
            if api.grant_type == 'client_credentials':
                # user_id is null so it's an update
                collection_tags = (collection_tag_3,)
                tag3_command = 'update'
            else:
                collection_tags = (collection_tag_2, collection_tag_3,)
                tag3_command = 'insert'

            assert_tag_instance_db_state('collection', api.grant_type, collection_id, *collection_tags)
            assert_tag_instance_audit_log(
                'collection', api.grant_type,
                dict(command='insert', object_id=collection_id, tag_instance=collection_tag_1),
                dict(command='update', object_id=collection_id, tag_instance=collection_tag_2),
                dict(command=tag3_command, object_id=collection_id, tag_instance=collection_tag_3),
            )
        elif tag_cardinality == 'multi':
            assert_tag_instance_db_state('collection', api.grant_type, collection_id, collection_tag_1, collection_tag_2, collection_tag_3)
            assert_tag_instance_audit_log(
                'collection', api.grant_type,
                dict(command='insert', object_id=collection_id, tag_instance=collection_tag_1),
                dict(command='insert', object_id=collection_id, tag_instance=collection_tag_2),
                dict(command='insert', object_id=collection_id, tag_instance=collection_tag_3),
            )

    else:  # not authorized
        assert_forbidden(r)
        assert_tag_instance_db_state('collection', api.grant_type, collection_id)
        assert_tag_instance_audit_log_empty('collection')

    assert_db_state(collection_batch)
    assert_no_audit_log()


@pytest.fixture(params=[True, False])
def same_user(request):
    return request.param


@pytest.mark.parametrize('admin_route, scopes', [
    (False, [ODPScope.COLLECTION_FREEZE]),  # the scope we've associated with the generic tag
    (False, []),
    (False, all_scopes),
    (False, all_scopes_excluding(ODPScope.COLLECTION_FREEZE)),
    (True, [ODPScope.COLLECTION_ADMIN]),
    (True, []),
    (True, all_scopes),
    (True, all_scopes_excluding(ODPScope.COLLECTION_ADMIN)),
])
def test_untag_collection(api, collection_batch, admin_route, scopes, collection_constraint, same_user):
    route = '/collection/admin/' if admin_route else '/collection/'

    authorized = admin_route and ODPScope.COLLECTION_ADMIN in scopes or \
                 not admin_route and ODPScope.COLLECTION_FREEZE in scopes
    authorized = authorized and collection_constraint in ('collection_any', 'collection_match')

    try_skip_collection_constraint(api.grant_type, collection_constraint)

    if collection_constraint == 'collection_any':
        authorized_collections = None  # => all
    elif collection_constraint == 'collection_match':
        authorized_collections = collection_batch[1:3]
    elif collection_constraint == 'collection_mismatch':
        authorized_collections = collection_batch[0:2]

    client = api(scopes, user_collections=authorized_collections)
    collection = collection_batch[2]
    collection_tags = CollectionTagFactory.create_batch(randint(1, 3), collection=collection)

    tag = new_generic_tag('collection')
    if same_user:
        collection_tag_1 = CollectionTagFactory(
            collection=collection,
            tag=tag,
            keyword=tag.vocabulary.keywords[2] if tag.vocabulary else None,
            user=FactorySession.get(User, 'odp.test.user') if api.grant_type == 'authorization_code' else None,
        )
    else:
        collection_tag_1 = CollectionTagFactory(
            collection=collection,
            tag=tag,
            keyword=tag.vocabulary.keywords[2] if tag.vocabulary else None,
        )
    collection_tag_1_dict = {
        'tag_id': collection_tag_1.tag_id,
        'user_id': collection_tag_1.user_id,
        'data': collection_tag_1.data,
        'keyword_id': collection_tag_1.keyword_id,
    }

    r = client.delete(f'{route}{collection.id}/tag/{collection_tag_1.id}')

    if authorized:
        if not admin_route and not same_user:
            assert_forbidden(r)
            assert_tag_instance_db_state('collection', api.grant_type, collection.id, *collection_tags, collection_tag_1)
            assert_tag_instance_audit_log_empty('collection')
        else:
            assert_ok_null(r)
            assert_tag_instance_db_state('collection', api.grant_type, collection.id, *collection_tags)
            assert_tag_instance_audit_log(
                'collection', api.grant_type,
                dict(command='delete', object_id=collection.id, tag_instance=collection_tag_1_dict),
            )
    else:
        assert_forbidden(r)
        assert_tag_instance_db_state('collection', api.grant_type, collection.id, *collection_tags, collection_tag_1)
        assert_tag_instance_audit_log_empty('collection')

    assert_db_state(collection_batch)
    assert_no_audit_log()
