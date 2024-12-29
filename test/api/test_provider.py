import uuid
from datetime import datetime
from random import randint

import pytest
from sqlalchemy import select

from odp.const import ODPScope
from odp.db.models import Provider, ProviderAudit, ProviderUser
from test import TestSession
from test.api import (
    assert_conflict, assert_empty_result, assert_forbidden, assert_new_timestamp,
    assert_not_found, assert_unprocessable,
)
from test.api.conftest import try_skip_user_provider_constraint
from test.factories import ClientFactory, CollectionFactory, PackageFactory, ProviderFactory, RecordFactory, ResourceFactory, UserFactory


@pytest.fixture
def provider_batch():
    """Create and commit a batch of Provider instances,
    with associated users, clients and collections."""
    providers = [
        ProviderFactory(users=UserFactory.create_batch(randint(0, 3)))
        for _ in range(randint(3, 5))
    ]
    for provider in providers:
        ClientFactory.create_batch(randint(0, 3), provider=provider)
        CollectionFactory.create_batch(randint(0, 3), provider=provider)
        provider.user_names = {user.id: user.name for user in provider.users}
        provider.user_ids = [user.id for user in provider.users]
        provider.client_ids = [client.id for client in provider.clients]
        provider.collection_keys = {collection.id: collection.key for collection in provider.collections}

    return providers


def provider_build(**id):
    """Build and return an uncommitted Provider instance.
    Associated users are however committed."""
    provider = ProviderFactory.build(
        **id,
        users=UserFactory.create_batch(randint(0, 3)),
    )
    provider.user_names = {user.id: user.name for user in provider.users}
    provider.user_ids = [user.id for user in provider.users]
    provider.client_ids = []
    provider.collection_keys = {}
    return provider


def assert_db_state(providers):
    """Verify that the provider table contains the given provider batch,
    and that the provider_user table contains the associated user references."""
    result = TestSession.execute(select(Provider)).scalars().all()
    result.sort(key=lambda p: p.id)
    providers.sort(key=lambda p: p.id)
    assert len(result) == len(providers)
    for n, row in enumerate(result):
        assert row.id == providers[n].id
        assert row.key == providers[n].key
        assert row.name == providers[n].name
        assert_new_timestamp(row.timestamp)

    result = TestSession.execute(select(ProviderUser.provider_id, ProviderUser.user_id)).all()
    result.sort(key=lambda pu: (pu.provider_id, pu.user_id))
    provider_users = []
    for provider in providers:
        for user_id in provider.user_ids:
            provider_users += [(provider.id, user_id)]
    provider_users.sort()
    assert result == provider_users


def assert_audit_log(command, provider, grant_type):
    result = TestSession.execute(select(ProviderAudit)).scalar_one()
    assert result.client_id == 'odp.test.client'
    assert result.user_id == ('odp.test.user' if grant_type == 'authorization_code' else None)
    assert result.command == command
    assert_new_timestamp(result.timestamp)
    assert result._id == provider.id
    assert result._key == provider.key
    assert result._name == provider.name
    assert sorted(result._users) == sorted(provider.user_ids)


def assert_no_audit_log():
    assert TestSession.execute(select(ProviderAudit)).first() is None


def assert_json_result(response, json, provider, detail=False):
    """Verify that the API result matches the given provider object."""
    assert response.status_code == 200
    assert json['id'] == provider.id
    assert json['key'] == provider.key
    assert json['name'] == provider.name
    assert json['collection_keys'] == provider.collection_keys
    assert_new_timestamp(datetime.fromisoformat(json['timestamp']))
    if detail:
        assert json['user_names'] == provider.user_names
        assert sorted(json['user_ids']) == sorted(provider.user_ids)
        assert sorted(json['client_ids']) == sorted(provider.client_ids)


def assert_json_results(response, json, providers):
    """Verify that the API result list matches the given provider batch."""
    items = json['items']
    assert json['total'] == len(items) == len(providers)
    items.sort(key=lambda i: i['id'])
    providers.sort(key=lambda p: p.id)
    for n, provider in enumerate(providers):
        assert_json_result(response, items[n], provider)


def parameterize_api_fixture(
        providers,
        grant_type,
        client_provider_constraint,
        user_provider_constraint,
):
    """Return tuple(client_provider, user_providers) for parameterizing the
    api fixture, based on constraint params and batch-generated providers.

    In the list tests, match and mismatch have the same behaviour.
    """
    try_skip_user_provider_constraint(grant_type, user_provider_constraint)

    if client_provider_constraint == 'client_provider_any':
        client_provider = None
    elif client_provider_constraint == 'client_provider_match':
        client_provider = providers[2]
    elif client_provider_constraint == 'client_provider_mismatch':
        client_provider = providers[0]

    if client_provider:
        client_provider.client_ids += ['odp.test.client']

    if user_provider_constraint == 'user_provider_none':
        user_providers = None
    elif user_provider_constraint == 'user_provider_match':
        user_providers = providers[1:3]
    elif user_provider_constraint == 'user_provider_mismatch':
        user_providers = providers[0:2]

    if user_providers:
        for user_provider in user_providers:
            user_provider.user_ids += ['odp.test.user']
            user_provider.user_names['odp.test.user'] = 'Test User'

    return dict(client_provider=client_provider, user_providers=user_providers)


@pytest.mark.require_scope(ODPScope.PROVIDER_READ)
def test_list_providers(
        api,
        scopes,
        provider_batch,
        client_provider_constraint,
        user_provider_constraint,
):
    api_kwargs = parameterize_api_fixture(
        provider_batch,
        api.grant_type,
        client_provider_constraint,
        user_provider_constraint,
    )
    authorized = ODPScope.PROVIDER_READ in scopes

    if client_provider_constraint == 'client_provider_any':
        expected_result_batch = provider_batch
    elif client_provider_constraint == 'client_provider_match':
        expected_result_batch = [provider_batch[2]]
    elif client_provider_constraint == 'client_provider_mismatch':
        expected_result_batch = [provider_batch[0]]

    if api.grant_type == 'authorization_code':
        if user_provider_constraint == 'user_provider_none':
            expected_result_batch = []
        elif user_provider_constraint == 'user_provider_match':
            expected_result_batch = list(set(provider_batch[1:3]).intersection(expected_result_batch))
        elif user_provider_constraint == 'user_provider_mismatch':
            expected_result_batch = list(set(provider_batch[0:2]).intersection(expected_result_batch))

    r = api(scopes, **api_kwargs).get('/provider/')

    if authorized:
        assert_json_results(r, r.json(), expected_result_batch)
    else:
        assert_forbidden(r)

    assert_db_state(provider_batch)
    assert_no_audit_log()


@pytest.mark.require_scope(ODPScope.PROVIDER_READ_ALL)
def test_list_all_providers(
        api,
        scopes,
        provider_batch,
        client_provider_constraint,
        user_provider_constraint,
):
    """Configured as for test_list_providers, but for this scope+endpoint
    all providers can always be read unconditionally."""
    api_kwargs = parameterize_api_fixture(
        provider_batch,
        api.grant_type,
        client_provider_constraint,
        user_provider_constraint,
    )
    authorized = ODPScope.PROVIDER_READ_ALL in scopes
    expected_result_batch = provider_batch

    r = api(scopes, **api_kwargs).get('/provider/all/')

    if authorized:
        assert_json_results(r, r.json(), expected_result_batch)
    else:
        assert_forbidden(r)

    assert_db_state(provider_batch)
    assert_no_audit_log()


@pytest.mark.require_scope(ODPScope.PROVIDER_READ)
def test_get_provider(
        api,
        scopes,
        provider_batch,
        client_provider_constraint,
        user_provider_constraint,
):
    api_kwargs = parameterize_api_fixture(
        provider_batch,
        api.grant_type,
        client_provider_constraint,
        user_provider_constraint,
    )
    authorized = (
            ODPScope.PROVIDER_READ in scopes and
            client_provider_constraint in ('client_provider_any', 'client_provider_match') and
            (api.grant_type == 'client_credentials' or user_provider_constraint == 'user_provider_match')
    )

    r = api(scopes, **api_kwargs).get(f'/provider/{provider_batch[2].id}')

    if authorized:
        assert_json_result(r, r.json(), provider_batch[2], detail=True)
    else:
        assert_forbidden(r)

    assert_db_state(provider_batch)
    assert_no_audit_log()


@pytest.mark.require_scope(ODPScope.PROVIDER_READ_ALL)
def test_get_any_provider(
        api,
        scopes,
        provider_batch,
        client_provider_constraint,
        user_provider_constraint,
):
    """Configured as for test_get_provider, but for this scope+endpoint
    any provider can always be read unconditionally."""
    api_kwargs = parameterize_api_fixture(
        provider_batch,
        api.grant_type,
        client_provider_constraint,
        user_provider_constraint,
    )
    authorized = ODPScope.PROVIDER_READ_ALL in scopes

    r = api(scopes, **api_kwargs).get(f'/provider/all/{provider_batch[2].id}')

    if authorized:
        assert_json_result(r, r.json(), provider_batch[2], detail=True)
    else:
        assert_forbidden(r)

    assert_db_state(provider_batch)
    assert_no_audit_log()


@pytest.mark.parametrize('route', ['/provider/', '/provider/all/'])
def test_get_provider_not_found(
        api,
        route,
        provider_batch,
        client_provider_constraint,
        user_provider_constraint,
):
    scopes = [ODPScope.PROVIDER_READ_ALL] if 'all' in route else [ODPScope.PROVIDER_READ]
    api_kwargs = parameterize_api_fixture(
        provider_batch,
        api.grant_type,
        client_provider_constraint,
        user_provider_constraint,
    )
    # for the constrainable scope 'odp.provider:read', auth will only ever
    # succeed for client_credentials with a non-provider-specific client;
    # all other 'odp.provider:read' cases will fail with a 403 because 'foo'
    # will never appear in the authorized set of providers
    authorized = ODPScope.PROVIDER_READ_ALL in scopes or (
            client_provider_constraint == 'client_provider_any' and
            api.grant_type == 'client_credentials'
    )

    r = api(scopes, **api_kwargs).get(f'{route}foo')

    if authorized:
        assert_not_found(r)
    else:
        assert_forbidden(r)

    assert_db_state(provider_batch)
    assert_no_audit_log()


@pytest.mark.require_scope(ODPScope.PROVIDER_ADMIN)
def test_create_provider(api, provider_batch, scopes):
    authorized = ODPScope.PROVIDER_ADMIN in scopes
    provider = provider_build()

    r = api(scopes).post('/provider/', json=dict(
        key=provider.key,
        name=provider.name,
        user_ids=provider.user_ids,
    ))

    if authorized:
        provider.id = r.json().get('id')
        assert_json_result(r, r.json(), provider)
        assert_db_state(provider_batch + [provider])
        assert_audit_log('insert', provider, api.grant_type)
    else:
        assert_forbidden(r)
        assert_db_state(provider_batch)
        assert_no_audit_log()


def test_create_provider_conflict(api, provider_batch):
    scopes = [ODPScope.PROVIDER_ADMIN]
    provider = provider_build(
        key=provider_batch[2].key,
    )

    r = api(scopes).post('/provider/', json=dict(
        key=provider.key,
        name=provider.name,
        user_ids=provider.user_ids,
    ))

    assert_conflict(r, 'Provider key is already in use')
    assert_db_state(provider_batch)
    assert_no_audit_log()


@pytest.mark.require_scope(ODPScope.PROVIDER_ADMIN)
def test_update_provider(api, provider_batch, scopes):
    authorized = ODPScope.PROVIDER_ADMIN in scopes
    provider = provider_build(
        id=provider_batch[2].id,
    )

    r = api(scopes).put(f'/provider/{provider.id}', json=dict(
        key=provider.key,
        name=provider.name,
        user_ids=provider.user_ids,
    ))

    if authorized:
        assert_empty_result(r)
        assert_db_state(provider_batch[:2] + [provider] + provider_batch[3:])
        assert_audit_log('update', provider, api.grant_type)
    else:
        assert_forbidden(r)
        assert_db_state(provider_batch)
        assert_no_audit_log()


def test_update_provider_not_found(api, provider_batch):
    scopes = [ODPScope.PROVIDER_ADMIN]
    provider = provider_build(
        id=str(uuid.uuid4()),
    )

    r = api(scopes).put(f'/provider/{provider.id}', json=dict(
        key=provider.key,
        name=provider.name,
        user_ids=provider.user_ids,
    ))

    assert_not_found(r)
    assert_db_state(provider_batch)
    assert_no_audit_log()


def test_update_provider_conflict(api, provider_batch):
    scopes = [ODPScope.PROVIDER_ADMIN]
    provider = provider_build(
        id=provider_batch[2].id,
        key=provider_batch[0].key,
    )

    r = api(scopes).put(f'/provider/{provider.id}', json=dict(
        key=provider.key,
        name=provider.name,
        user_ids=provider.user_ids,
    ))

    assert_conflict(r, 'Provider key is already in use')
    assert_db_state(provider_batch)
    assert_no_audit_log()


@pytest.fixture(params=[False, True])
def has_record(request):
    return request.param


@pytest.fixture(params=[False, True])
def has_package(request):
    return request.param


@pytest.mark.require_scope(ODPScope.PROVIDER_ADMIN)
def test_delete_provider(api, provider_batch, scopes, has_record, has_package):
    authorized = ODPScope.PROVIDER_ADMIN in scopes
    deleted_provider = provider_batch[2]

    if has_record:
        if collection := next((c for c in deleted_provider.collections), None):
            RecordFactory(collection=collection)
        else:
            has_record = False

    if has_package:
        PackageFactory(provider=deleted_provider)

    r = api(scopes).delete(f'/provider/{deleted_provider.id}')

    if authorized:
        if has_record or has_package:
            # TODO:
            #  provider-collection relationship is deprecated;
            #  has_record should eventually be removed
            assert_unprocessable(r, 'A provider with associated packages and/or resources cannot be deleted.')
            assert_db_state(provider_batch)
            assert_no_audit_log()
        else:
            assert_empty_result(r)
            assert_db_state(provider_batch[:2] + provider_batch[3:])
            assert_audit_log('delete', deleted_provider, api.grant_type)
    else:
        assert_forbidden(r)
        assert_db_state(provider_batch)
        assert_no_audit_log()


def test_delete_provider_not_found(api, provider_batch):
    scopes = [ODPScope.PROVIDER_ADMIN]
    r = api(scopes).delete('/provider/foo')
    assert_not_found(r)
    assert_db_state(provider_batch)
    assert_no_audit_log()
