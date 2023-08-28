from random import randint

import pytest
from sqlalchemy import select

from odp.const import ODPScope
from odp.const.hydra import TokenEndpointAuthMethod
from odp.db import Session
from odp.db.models import Client
from test.api import CollectionAuth, assert_conflict, assert_empty_result, assert_forbidden, assert_not_found
from test.factories import ClientFactory, CollectionFactory, ScopeFactory, fake


@pytest.fixture
def client_batch(hydra_admin_api):
    """Create and commit a batch of Client instances, and create
    an OAuth2 client config on Hydra for each."""
    clients = []
    for n in range(randint(3, 5)):
        clients += [client := ClientFactory(
            scopes=(scopes := ScopeFactory.create_batch(randint(1, 3))),
            collection_specific=n in (1, 2) or randint(0, 1),
            collections=CollectionFactory.create_batch(randint(1, 2)) if n > 1 else None,
        )]
        hydra_admin_api.create_or_update_client(
            client.id,
            name=fake.catch_phrase(),
            secret=fake.password(),
            scope_ids=[s.id for s in scopes],
            grant_types=[],
        )

    return clients


@pytest.fixture(autouse=True)
def delete_hydra_clients(hydra_admin_api):
    """Delete Hydra client configs after each test."""
    try:
        yield
    finally:
        for hydra_client in hydra_admin_api.list_clients():
            if hydra_client.id != 'odp.test':
                hydra_admin_api.delete_client(hydra_client.id)


def client_build(collections=None, **id):
    """Build and return an uncommitted Client instance.
    Referenced scopes and/or collections are however committed."""
    return ClientFactory.build(
        **id,
        scopes=ScopeFactory.create_batch(randint(1, 3)),
        collections=collections,
        collection_specific=collections is not None,
    )


def scope_ids(client):
    return tuple(sorted(scope.id for scope in client.scopes))


def collection_ids(client):
    return tuple(sorted(collection.id for collection in client.collections))


def collection_keys(client):
    return {
        collection.key: collection.id for collection in client.collections
    } if client.collection_specific else {}


def assert_db_state(clients):
    """Verify that the DB client table contains the given client batch."""
    Session.expire_all()
    result = Session.execute(select(Client).where(Client.id != 'odp.test')).scalars().all()
    assert set((row.id, scope_ids(row), collection_ids(row)) for row in result) \
           == set((client.id, scope_ids(client), collection_ids(client)) for client in clients)


def assert_json_result(response, json, client):
    """Verify that the API result matches the given client object.

    TODO: test Hydra client config values
    """
    assert response.status_code == 200
    assert json['id'] == client.id
    assert json['collection_specific'] == client.collection_specific
    assert json['collection_keys'] == collection_keys(client)
    assert tuple(sorted(json['scope_ids'])) == scope_ids(client)


def assert_json_results(response, json, clients):
    """Verify that the API result list matches the given client batch."""
    items = [j for j in json['items'] if j['id'] != 'odp.test']
    assert json['total'] - 1 == len(items) == len(clients)
    items.sort(key=lambda i: i['id'])
    clients.sort(key=lambda c: c.id)
    for n, client in enumerate(clients):
        assert_json_result(response, items[n], client)


@pytest.mark.require_scope(ODPScope.CLIENT_READ)
def test_list_clients(api, client_batch, scopes):
    authorized = ODPScope.CLIENT_READ in scopes
    r = api(scopes).get('/client/')
    if authorized:
        assert_json_results(r, r.json(), client_batch)
    else:
        assert_forbidden(r)
    assert_db_state(client_batch)


@pytest.mark.require_scope(ODPScope.CLIENT_READ)
def test_get_client(api, client_batch, scopes):
    authorized = ODPScope.CLIENT_READ in scopes
    r = api(scopes).get(f'/client/{client_batch[2].id}')
    if authorized:
        assert_json_result(r, r.json(), client_batch[2])
    else:
        assert_forbidden(r)
    assert_db_state(client_batch)


def test_get_client_not_found(api, client_batch):
    scopes = [ODPScope.CLIENT_READ]
    r = api(scopes).get('/client/foo')
    assert_not_found(r)
    assert_db_state(client_batch)


@pytest.mark.require_scope(ODPScope.CLIENT_ADMIN)
def test_create_client(api, client_batch, scopes, collection_auth):
    authorized = ODPScope.CLIENT_ADMIN in scopes and \
                 collection_auth in (CollectionAuth.NONE, CollectionAuth.MATCH)

    if collection_auth == CollectionAuth.MATCH:
        api_client_collections = client_batch[2].collections
    elif collection_auth == CollectionAuth.MISMATCH:
        api_client_collections = client_batch[1].collections
    else:
        api_client_collections = None

    if collection_auth in (CollectionAuth.MATCH, CollectionAuth.MISMATCH):
        new_client_collections = client_batch[2].collections
    else:
        new_client_collections = None

    modified_client_batch = client_batch + [client := client_build(
        collections=new_client_collections
    )]

    r = api(scopes, api_client_collections).post('/client/', json=dict(
        id=client.id,
        name=fake.catch_phrase(),
        secret=fake.password(length=16),
        scope_ids=scope_ids(client),
        collection_specific=client.collection_specific,
        collection_ids=[c.id for c in client.collections],
        grant_types=[],
        response_types=[],
        redirect_uris=[],
        post_logout_redirect_uris=[],
        token_endpoint_auth_method=TokenEndpointAuthMethod.CLIENT_SECRET_BASIC,
        allowed_cors_origins=[],
    ))

    if authorized:
        assert_empty_result(r)
        assert_db_state(modified_client_batch)
    else:
        assert_forbidden(r)
        assert_db_state(client_batch)


def test_create_client_conflict(api, client_batch, collection_auth):
    scopes = [ODPScope.CLIENT_ADMIN]
    authorized = collection_auth in (CollectionAuth.NONE, CollectionAuth.MATCH)

    if collection_auth == CollectionAuth.MATCH:
        api_client_collections = client_batch[2].collections
    elif collection_auth == CollectionAuth.MISMATCH:
        api_client_collections = client_batch[1].collections
    else:
        api_client_collections = None

    if collection_auth in (CollectionAuth.MATCH, CollectionAuth.MISMATCH):
        new_client_collections = client_batch[2].collections
    else:
        new_client_collections = None

    client = client_build(
        id=client_batch[2].id,
        collections=new_client_collections,
    )

    r = api(scopes, api_client_collections).post('/client/', json=dict(
        id=client.id,
        name=fake.catch_phrase(),
        secret=fake.password(length=16),
        scope_ids=scope_ids(client),
        collection_specific=client.collection_specific,
        collection_ids=[c.id for c in client.collections],
        grant_types=[],
        response_types=[],
        redirect_uris=[],
        post_logout_redirect_uris=[],
        token_endpoint_auth_method=TokenEndpointAuthMethod.CLIENT_SECRET_BASIC,
        allowed_cors_origins=[],
    ))

    if authorized:
        assert_conflict(r, 'Client id is already in use')
    else:
        assert_forbidden(r)

    assert_db_state(client_batch)


@pytest.mark.require_scope(ODPScope.CLIENT_ADMIN)
def test_update_client(api, client_batch, scopes, collection_auth):
    authorized = ODPScope.CLIENT_ADMIN in scopes and \
                 collection_auth in (CollectionAuth.NONE, CollectionAuth.MATCH)

    if collection_auth == CollectionAuth.MATCH:
        api_client_collections = client_batch[2].collections
    elif collection_auth == CollectionAuth.MISMATCH:
        api_client_collections = client_batch[1].collections
    else:
        api_client_collections = None

    if collection_auth in (CollectionAuth.MATCH, CollectionAuth.MISMATCH):
        modified_client_collections = client_batch[2].collections
    else:
        modified_client_collections = None

    modified_client_batch = client_batch.copy()
    modified_client_batch[2] = (client := client_build(
        id=client_batch[2].id,
        collections=modified_client_collections,
    ))

    r = api(scopes, api_client_collections).put('/client/', json=dict(
        id=client.id,
        name=fake.catch_phrase(),
        secret=fake.password(length=16),
        scope_ids=scope_ids(client),
        collection_specific=client.collection_specific,
        collection_ids=[c.id for c in client.collections],
        grant_types=[],
        response_types=[],
        redirect_uris=[],
        post_logout_redirect_uris=[],
        token_endpoint_auth_method=TokenEndpointAuthMethod.CLIENT_SECRET_BASIC,
        allowed_cors_origins=[],
    ))

    if authorized:
        assert_empty_result(r)
        assert_db_state(modified_client_batch)
    else:
        assert_forbidden(r)
        assert_db_state(client_batch)


def test_update_client_not_found(api, client_batch, collection_auth):
    scopes = [ODPScope.CLIENT_ADMIN]
    authorized = collection_auth in (CollectionAuth.NONE, CollectionAuth.MATCH)

    if collection_auth == CollectionAuth.MATCH:
        api_client_collections = client_batch[2].collections
    elif collection_auth == CollectionAuth.MISMATCH:
        api_client_collections = client_batch[1].collections
    else:
        api_client_collections = None

    if collection_auth in (CollectionAuth.MATCH, CollectionAuth.MISMATCH):
        modified_client_collections = client_batch[2].collections
    else:
        modified_client_collections = None

    client = client_build(
        id='foo',
        collections=modified_client_collections,
    )

    r = api(scopes, api_client_collections).put('/client/', json=dict(
        id=client.id,
        name=fake.catch_phrase(),
        secret=fake.password(length=16),
        scope_ids=scope_ids(client),
        collection_specific=client.collection_specific,
        collection_ids=[c.id for c in client.collections],
        grant_types=[],
        response_types=[],
        redirect_uris=[],
        post_logout_redirect_uris=[],
        token_endpoint_auth_method=TokenEndpointAuthMethod.CLIENT_SECRET_BASIC,
        allowed_cors_origins=[],
    ))

    if authorized:
        assert_not_found(r)
    else:
        assert_forbidden(r)

    assert_db_state(client_batch)


@pytest.mark.require_scope(ODPScope.CLIENT_ADMIN)
def test_delete_client(api, client_batch, scopes, collection_auth):
    authorized = ODPScope.CLIENT_ADMIN in scopes and \
                 collection_auth in (CollectionAuth.NONE, CollectionAuth.MATCH)

    if collection_auth == CollectionAuth.MATCH:
        api_client_collections = client_batch[2].collections
    elif collection_auth == CollectionAuth.MISMATCH:
        api_client_collections = client_batch[1].collections
    else:
        api_client_collections = None

    modified_client_batch = client_batch.copy()
    del modified_client_batch[2]

    r = api(scopes, api_client_collections).delete(f'/client/{client_batch[2].id}')

    if authorized:
        assert_empty_result(r)
        assert_db_state(modified_client_batch)
    else:
        assert_forbidden(r)
        assert_db_state(client_batch)


def test_delete_client_not_found(api, client_batch, collection_auth):
    scopes = [ODPScope.CLIENT_ADMIN]

    if collection_auth == CollectionAuth.NONE:
        api_client_collections = None
    else:
        api_client_collections = client_batch[2].collections

    r = api(scopes, api_client_collections).delete('/client/foo')

    # we can't get a forbidden, regardless of collection auth, because
    # if the client is not found, there are no collections to compare with
    assert_not_found(r)
    assert_db_state(client_batch)
