from enum import Enum
from random import choice, randint, sample
from urllib.parse import urljoin

import pytest
from sqlalchemy import select

from odp.const import ODPScope
from odp.const.hydra import GrantType, ResponseType, TokenEndpointAuthMethod
from odp.db import Session
from odp.db.models import Client
from test.api import assert_conflict, assert_empty_result, assert_forbidden, assert_not_found, assert_unprocessable
from test.factories import ClientFactory, ProviderFactory, ScopeFactory, fake


def enum_choice(e: type[Enum]) -> Enum:
    return choice(list(e.__members__.values()))


def enum_sample(e: type[Enum]) -> list[Enum]:
    return sample(list(e.__members__.values()), randint(1, len(e)))


def fake_hydra_client_config():
    return dict(
        name=fake.catch_phrase(),
        secret=fake.password(length=16),
        grant_types=enum_sample(GrantType),
        response_types=enum_sample(ResponseType),
        redirect_uris=(redirect_uris := [fake.uri() for _ in range(randint(1, 3))]),
        post_logout_redirect_uris=[urljoin(base, 'logout') for base in redirect_uris],
        token_endpoint_auth_method=enum_choice(TokenEndpointAuthMethod),
        allowed_cors_origins=[fake.url().rstrip('/') for _ in range(randint(0, 2))],
        client_credentials_grant_access_token_lifespan='1h30m0s',
    )


@pytest.fixture
def client_batch(hydra_admin_api):
    """Create and commit a batch of Client instances, and create
    an OAuth2 client config on Hydra for each."""
    clients = []
    for n in range(randint(3, 5)):
        clients += [client := ClientFactory(
            scopes=(scopes := ScopeFactory.create_batch(randint(1, 3))),
        )]
        client.hydra_config = fake_hydra_client_config()
        hydra_admin_api.create_or_update_client(
            client.id,
            scope_ids=[s.id for s in scopes],
            **client.hydra_config,
        )

    return clients


def client_build(provider_specific=None, **attrs):
    """Build and return an uncommitted Client instance.
    Referenced scopes and provider are however committed."""
    if provider_specific is None:
        provider_specific = randint(0, 1)

    return ClientFactory.build(
        **attrs,
        scopes=ScopeFactory.create_batch(randint(1, 3)),
        provider_specific=provider_specific,
        provider=(provider := ProviderFactory()) if provider_specific else None,
        provider_id=provider.id if provider_specific else None,
    )


def scope_ids(client):
    return tuple(sorted(scope.id for scope in client.scopes))


def assert_db_state(clients):
    """Verify that the DB client table contains the given client batch."""
    Session.expire_all()
    result = Session.execute(select(Client).where(Client.id != 'odp.test')).scalars().all()
    assert set((row.id, scope_ids(row), row.provider_specific, row.provider_id) for row in result) \
           == set((client.id, scope_ids(client), client.provider_specific, client.provider_id) for client in clients)


def assert_json_result(response, json, client):
    """Verify that the API result matches the given client object."""
    assert response.status_code == 200
    assert json['id'] == client.id
    assert json['provider_specific'] == client.provider_specific
    assert json['provider_id'] == client.provider_id
    assert json['provider_key'] == (client.provider.key if client.provider_id else None)
    assert tuple(sorted(json['scope_ids'])) == scope_ids(client)
    # the following API values are returned from Hydra
    assert json['name'] == client.hydra_config['name']
    assert json['grant_types'] == client.hydra_config['grant_types']
    assert json['response_types'] == client.hydra_config['response_types']
    assert json['redirect_uris'] == client.hydra_config['redirect_uris']
    assert json['post_logout_redirect_uris'] == client.hydra_config['post_logout_redirect_uris']
    assert json['token_endpoint_auth_method'] == client.hydra_config['token_endpoint_auth_method']
    assert json['allowed_cors_origins'] == client.hydra_config['allowed_cors_origins']
    assert json['client_credentials_grant_access_token_lifespan'] == client.hydra_config['client_credentials_grant_access_token_lifespan']


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
def test_create_client(api, client_batch, scopes):
    authorized = ODPScope.CLIENT_ADMIN in scopes
    modified_client_batch = client_batch + [client := client_build()]
    client.hydra_config = fake_hydra_client_config()

    r = api(scopes).post('/client/', json=dict(
        id=client.id,
        scope_ids=scope_ids(client),
        provider_specific=client.provider_specific,
        provider_id=client.provider_id,
        **client.hydra_config,
    ))

    if authorized:
        assert_empty_result(r)
        assert_db_state(modified_client_batch)
    else:
        assert_forbidden(r)
        assert_db_state(client_batch)


def test_create_client_conflict(api, client_batch):
    scopes = [ODPScope.CLIENT_ADMIN]
    client = client_build(id=client_batch[2].id)
    client.hydra_config = fake_hydra_client_config()

    r = api(scopes).post('/client/', json=dict(
        id=client.id,
        scope_ids=scope_ids(client),
        provider_specific=client.provider_specific,
        provider_id=client.provider_id,
        **client.hydra_config,
    ))

    assert_conflict(r, 'Client id is already in use')
    assert_db_state(client_batch)


def test_create_client_admin_provider_specific(api, client_batch):
    scopes = [ODPScope.CLIENT_ADMIN]
    client = client_build(provider_specific=True)
    client.hydra_config = fake_hydra_client_config()

    r = api(scopes).post('/client/', json=dict(
        id=client.id,
        scope_ids=list(scope_ids(client)) + [ODPScope.CLIENT_ADMIN],
        provider_specific=client.provider_specific,
        provider_id=client.provider_id,
        **client.hydra_config,
    ))

    assert_unprocessable(r, "Scope 'odp.client:admin' cannot be granted to a provider-specific client.")
    assert_db_state(client_batch)


@pytest.mark.require_scope(ODPScope.CLIENT_ADMIN)
def test_update_client(api, client_batch, scopes):
    authorized = ODPScope.CLIENT_ADMIN in scopes
    modified_client_batch = client_batch.copy()
    modified_client_batch[2] = (client := client_build(id=client_batch[2].id))
    client.hydra_config = fake_hydra_client_config()

    r = api(scopes).put('/client/', json=dict(
        id=client.id,
        scope_ids=scope_ids(client),
        provider_specific=client.provider_specific,
        provider_id=client.provider_id,
        **client.hydra_config,
    ))

    if authorized:
        assert_empty_result(r)
        assert_db_state(modified_client_batch)
    else:
        assert_forbidden(r)
        assert_db_state(client_batch)


def test_update_client_not_found(api, client_batch):
    scopes = [ODPScope.CLIENT_ADMIN]
    client = client_build(id='foo')
    client.hydra_config = fake_hydra_client_config()

    r = api(scopes).put('/client/', json=dict(
        id=client.id,
        scope_ids=scope_ids(client),
        provider_specific=client.provider_specific,
        provider_id=client.provider_id,
        **client.hydra_config,
    ))

    assert_not_found(r)
    assert_db_state(client_batch)


def test_update_client_admin_provider_specific(api, client_batch):
    scopes = [ODPScope.CLIENT_ADMIN]
    client = client_build(id=client_batch[2].id, provider_specific=True)
    client.hydra_config = fake_hydra_client_config()

    r = api(scopes).put('/client/', json=dict(
        id=client.id,
        scope_ids=list(scope_ids(client)) + [ODPScope.CLIENT_ADMIN],
        provider_specific=client.provider_specific,
        provider_id=client.provider_id,
        **client.hydra_config,
    ))

    assert_unprocessable(r, "Scope 'odp.client:admin' cannot be granted to a provider-specific client.")
    assert_db_state(client_batch)


@pytest.mark.require_scope(ODPScope.CLIENT_ADMIN)
def test_delete_client(api, client_batch, scopes):
    authorized = ODPScope.CLIENT_ADMIN in scopes
    modified_client_batch = client_batch.copy()
    del modified_client_batch[2]

    r = api(scopes).delete(f'/client/{client_batch[2].id}')

    if authorized:
        assert_empty_result(r)
        assert_db_state(modified_client_batch)
    else:
        assert_forbidden(r)
        assert_db_state(client_batch)


def test_delete_client_not_found(api, client_batch):
    scopes = [ODPScope.CLIENT_ADMIN]
    r = api(scopes).delete('/client/foo')
    assert_not_found(r)
    assert_db_state(client_batch)
