import uuid
from datetime import datetime
from random import randint

import pytest
from sqlalchemy import select

from odp.const import ODPScope
from odp.db.models import Provider, ProviderAudit
from test import TestSession
from test.api import (
    assert_conflict, assert_empty_result, assert_forbidden, assert_new_timestamp,
    assert_not_found, assert_unprocessable,
)
from test.factories import ClientFactory, CollectionFactory, PackageFactory, ProviderFactory, RecordFactory, ResourceFactory, UserFactory


@pytest.fixture
def provider_batch():
    """Create and commit a batch of Provider instances,
    with associated collections, users and clients."""
    providers = [
        ProviderFactory()
        for _ in range(randint(3, 5))
    ]
    for provider in providers:
        CollectionFactory.create_batch(randint(0, 3), provider=provider)
        UserFactory.create_batch(randint(0, 3), providers=[provider])
        ClientFactory.create_batch(randint(0, 3), provider=provider)
    return providers


def provider_build(**id):
    """Build and return an uncommitted Provider instance."""
    return ProviderFactory.build(**id)


def collection_keys(provider):
    return {collection.id: collection.key for collection in provider.collections}


def user_names(provider):
    return {user.id: user.name for user in provider.users}


def client_ids(provider):
    return tuple(sorted(client.id for client in provider.clients))


def assert_db_state(providers):
    """Verify that the DB provider table contains the given provider batch."""
    result = TestSession.execute(select(Provider)).scalars().all()
    result.sort(key=lambda p: p.id)
    providers.sort(key=lambda p: p.id)
    assert len(result) == len(providers)
    for n, row in enumerate(result):
        assert row.id == providers[n].id
        assert row.key == providers[n].key
        assert row.name == providers[n].name
        assert_new_timestamp(row.timestamp)


def assert_audit_log(command, provider, grant_type):
    result = TestSession.execute(select(ProviderAudit)).scalar_one()
    assert result.client_id == 'odp.test.client'
    assert result.user_id == ('odp.test.user' if grant_type == 'authorization_code' else None)
    assert result.command == command
    assert_new_timestamp(result.timestamp)
    assert result._id == provider.id
    assert result._key == provider.key
    assert result._name == provider.name


def assert_no_audit_log():
    assert TestSession.execute(select(ProviderAudit)).first() is None


def assert_json_result(response, json, provider):
    """Verify that the API result matches the given provider object."""
    assert response.status_code == 200
    assert json['id'] == provider.id
    assert json['key'] == provider.key
    assert json['name'] == provider.name
    assert json['collection_keys'] == collection_keys(provider)
    assert json['user_names'] == user_names(provider)
    assert tuple(sorted(json['client_ids'])) == client_ids(provider)
    assert_new_timestamp(datetime.fromisoformat(json['timestamp']))


def assert_json_results(response, json, providers):
    """Verify that the API result list matches the given provider batch."""
    items = json['items']
    assert json['total'] == len(items) == len(providers)
    items.sort(key=lambda i: i['id'])
    providers.sort(key=lambda p: p.id)
    for n, provider in enumerate(providers):
        assert_json_result(response, items[n], provider)


@pytest.mark.require_scope(ODPScope.PROVIDER_READ)
def test_list_providers(api, provider_batch, scopes):
    authorized = ODPScope.PROVIDER_READ in scopes
    r = api(scopes).get('/provider/')
    if authorized:
        assert_json_results(r, r.json(), provider_batch)
    else:
        assert_forbidden(r)
    assert_db_state(provider_batch)
    assert_no_audit_log()


@pytest.mark.require_scope(ODPScope.PROVIDER_READ)
def test_get_provider(api, provider_batch, scopes):
    authorized = ODPScope.PROVIDER_READ in scopes
    r = api(scopes).get(f'/provider/{provider_batch[2].id}')
    if authorized:
        assert_json_result(r, r.json(), provider_batch[2])
    else:
        assert_forbidden(r)
    assert_db_state(provider_batch)
    assert_no_audit_log()


def test_get_provider_not_found(api, provider_batch):
    scopes = [ODPScope.PROVIDER_READ]
    r = api(scopes).get('/provider/foo')
    assert_not_found(r)
    assert_db_state(provider_batch)
    assert_no_audit_log()


@pytest.mark.require_scope(ODPScope.PROVIDER_ADMIN)
def test_create_provider(api, provider_batch, scopes):
    authorized = ODPScope.PROVIDER_ADMIN in scopes
    modified_provider_batch = provider_batch + [provider := provider_build()]
    r = api(scopes).post('/provider/', json=dict(
        key=provider.key,
        name=provider.name,
    ))
    if authorized:
        provider.id = r.json().get('id')
        assert_json_result(r, r.json(), provider)
        assert_db_state(modified_provider_batch)
        assert_audit_log('insert', provider, api.grant_type)
    else:
        assert_forbidden(r)
        assert_db_state(provider_batch)
        assert_no_audit_log()


def test_create_provider_conflict(api, provider_batch):
    scopes = [ODPScope.PROVIDER_ADMIN]
    provider = provider_build(key=provider_batch[2].key)
    r = api(scopes).post('/provider/', json=dict(
        key=provider.key,
        name=provider.name,
    ))
    assert_conflict(r, 'Provider key is already in use')
    assert_db_state(provider_batch)
    assert_no_audit_log()


@pytest.mark.require_scope(ODPScope.PROVIDER_ADMIN)
def test_update_provider(api, provider_batch, scopes):
    authorized = ODPScope.PROVIDER_ADMIN in scopes
    modified_provider_batch = provider_batch.copy()
    modified_provider_batch[2] = (provider := provider_build(
        id=provider_batch[2].id,
    ))
    r = api(scopes).put(f'/provider/{provider.id}', json=dict(
        key=provider.key,
        name=provider.name,
    ))
    if authorized:
        assert_empty_result(r)
        assert_db_state(modified_provider_batch)
        assert_audit_log('update', provider, api.grant_type)
    else:
        assert_forbidden(r)
        assert_db_state(provider_batch)
        assert_no_audit_log()


def test_update_provider_not_found(api, provider_batch):
    scopes = [ODPScope.PROVIDER_ADMIN]
    provider = provider_build(id=str(uuid.uuid4()))
    r = api(scopes).put(f'/provider/{provider.id}', json=dict(
        key=provider.key,
        name=provider.name,
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
    ))
    assert_conflict(r, 'Provider key is already in use')
    assert_db_state(provider_batch)
    assert_no_audit_log()


@pytest.fixture(params=[False, True])
def has_record(request):
    return request.param


@pytest.fixture(params=[False, True])
def has_resource(request):
    return request.param


@pytest.fixture(params=[False, True])
def has_package(request):
    return request.param


@pytest.mark.require_scope(ODPScope.PROVIDER_ADMIN)
def test_delete_provider(api, provider_batch, scopes, has_record, has_resource, has_package):
    authorized = ODPScope.PROVIDER_ADMIN in scopes
    modified_provider_batch = provider_batch.copy()
    deleted_provider = modified_provider_batch[2]
    del modified_provider_batch[2]

    if has_record:
        if collection := next((c for c in provider_batch[2].collections), None):
            RecordFactory(collection=collection)
        else:
            has_record = False

    if has_resource:
        ResourceFactory(provider=provider_batch[2])

    if has_package:
        PackageFactory(provider=provider_batch[2])

    r = api(scopes).delete(f'/provider/{provider_batch[2].id}')

    if authorized:
        if has_record or has_resource or has_package:
            # TODO:
            #  provider-collection relationship is deprecated;
            #  has_record should eventually be removed
            assert_unprocessable(r, 'A provider with associated packages and/or resources cannot be deleted.')
            assert_db_state(provider_batch)
            assert_no_audit_log()
        else:
            assert_empty_result(r)
            assert_db_state(modified_provider_batch)
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
