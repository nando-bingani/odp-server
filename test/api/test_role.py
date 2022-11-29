from random import choice, randint

import pytest
from sqlalchemy import select

from odp.const import ODPScope
from odp.db import Session
from odp.db.models import Role
from test.api import CollectionAuth, all_scopes, all_scopes_excluding, assert_conflict, assert_empty_result, assert_forbidden, assert_not_found
from test.factories import CollectionFactory, RoleFactory, ScopeFactory


@pytest.fixture
def role_batch():
    """Create and commit a batch of Role instances."""
    return [
        RoleFactory(
            scopes=ScopeFactory.create_batch(randint(0, 3), type=choice(('odp', 'client'))),
            collection_specific=n in (1, 2) or randint(0, 1),
            collections=CollectionFactory.create_batch(randint(1, 2)) if n in (1, 2) else None,
        )
        for n in range(randint(3, 5))
    ]


def role_build(collections=None, **id):
    """Build and return an uncommitted Role instance.
    Referenced scopes and/or collections are however committed."""
    return RoleFactory.build(
        **id,
        scopes=ScopeFactory.create_batch(randint(0, 3), type=choice(('odp', 'client'))),
        collections=collections,
        collection_specific=collections is not None,
    )


def scope_ids(role):
    return tuple(sorted(scope.id for scope in role.scopes))


def collection_ids(client):
    return tuple(sorted(collection.id for collection in client.collections))


def assert_db_state(roles):
    """Verify that the DB role table contains the given role batch."""
    Session.expire_all()
    result = Session.execute(select(Role)).scalars().all()
    assert set((row.id, scope_ids(row), collection_ids(row)) for row in result) \
           == set((role.id, scope_ids(role), collection_ids(role)) for role in roles)


def assert_json_result(response, json, role):
    """Verify that the API result matches the given role object."""
    assert response.status_code == 200
    assert json['id'] == role.id
    assert json['collection_specific'] == role.collection_specific
    assert tuple(sorted(json['collection_ids'])) == collection_ids(role)
    assert tuple(sorted(json['scope_ids'])) == scope_ids(role)


def assert_json_results(response, json, roles):
    """Verify that the API result list matches the given role batch."""
    items = json['items']
    assert json['total'] == len(items) == len(roles)
    items.sort(key=lambda i: i['id'])
    roles.sort(key=lambda r: r.id)
    for n, role in enumerate(roles):
        assert_json_result(response, items[n], role)


@pytest.mark.parametrize('scopes', [
    [ODPScope.ROLE_READ],
    [],
    all_scopes,
    all_scopes_excluding(ODPScope.ROLE_READ),
])
def test_list_roles(api, role_batch, scopes):
    authorized = ODPScope.ROLE_READ in scopes
    r = api(scopes).get('/role/')
    if authorized:
        assert_json_results(r, r.json(), role_batch)
    else:
        assert_forbidden(r)
    assert_db_state(role_batch)


@pytest.mark.parametrize('scopes', [
    [ODPScope.ROLE_READ],
    [],
    all_scopes,
    all_scopes_excluding(ODPScope.ROLE_READ),
])
def test_get_role(api, role_batch, scopes):
    authorized = ODPScope.ROLE_READ in scopes
    r = api(scopes).get(f'/role/{role_batch[2].id}')
    if authorized:
        assert_json_result(r, r.json(), role_batch[2])
    else:
        assert_forbidden(r)
    assert_db_state(role_batch)


def test_get_role_not_found(api, role_batch):
    scopes = [ODPScope.ROLE_READ]
    r = api(scopes).get('/role/foo')
    assert_not_found(r)
    assert_db_state(role_batch)


@pytest.mark.parametrize('scopes', [
    [ODPScope.ROLE_ADMIN],
    [],
    all_scopes,
    all_scopes_excluding(ODPScope.ROLE_ADMIN),
])
def test_create_role(api, role_batch, scopes, collection_auth):
    authorized = ODPScope.ROLE_ADMIN in scopes and \
                 collection_auth in (CollectionAuth.NONE, CollectionAuth.MATCH)

    if collection_auth == CollectionAuth.MATCH:
        api_client_collections = role_batch[2].collections
    elif collection_auth == CollectionAuth.MISMATCH:
        api_client_collections = role_batch[1].collections
    else:
        api_client_collections = None

    if collection_auth in (CollectionAuth.MATCH, CollectionAuth.MISMATCH):
        new_role_collections = role_batch[2].collections
    else:
        new_role_collections = None

    modified_role_batch = role_batch + [role := role_build(
        collections=new_role_collections
    )]

    r = api(scopes, api_client_collections).post('/role/', json=dict(
        id=role.id,
        scope_ids=scope_ids(role),
        collection_specific=role.collection_specific,
        collection_ids=[c.id for c in role.collections],
    ))

    if authorized:
        assert_empty_result(r)
        assert_db_state(modified_role_batch)
    else:
        assert_forbidden(r)
        assert_db_state(role_batch)


def test_create_role_conflict(api, role_batch, collection_auth):
    scopes = [ODPScope.ROLE_ADMIN]
    authorized = collection_auth in (CollectionAuth.NONE, CollectionAuth.MATCH)

    if collection_auth == CollectionAuth.MATCH:
        api_client_collections = role_batch[2].collections
    elif collection_auth == CollectionAuth.MISMATCH:
        api_client_collections = role_batch[1].collections
    else:
        api_client_collections = None

    if collection_auth in (CollectionAuth.MATCH, CollectionAuth.MISMATCH):
        new_role_collections = role_batch[2].collections
    else:
        new_role_collections = None

    role = role_build(
        id=role_batch[2].id,
        collections=new_role_collections,
    )

    r = api(scopes, api_client_collections).post('/role/', json=dict(
        id=role.id,
        scope_ids=scope_ids(role),
        collection_specific=role.collection_specific,
        collection_ids=[c.id for c in role.collections],
    ))

    if authorized:
        assert_conflict(r, 'Role id is already in use')
    else:
        assert_forbidden(r)

    assert_db_state(role_batch)


@pytest.mark.parametrize('scopes', [
    [ODPScope.ROLE_ADMIN],
    [],
    all_scopes,
    all_scopes_excluding(ODPScope.ROLE_ADMIN),
])
def test_update_role(api, role_batch, scopes, collection_auth):
    authorized = ODPScope.ROLE_ADMIN in scopes and \
                 collection_auth in (CollectionAuth.NONE, CollectionAuth.MATCH)

    if collection_auth == CollectionAuth.MATCH:
        api_client_collections = role_batch[2].collections
    elif collection_auth == CollectionAuth.MISMATCH:
        api_client_collections = role_batch[1].collections
    else:
        api_client_collections = None

    if collection_auth in (CollectionAuth.MATCH, CollectionAuth.MISMATCH):
        modified_role_collections = role_batch[2].collections
    else:
        modified_role_collections = None

    modified_role_batch = role_batch.copy()
    modified_role_batch[2] = (role := role_build(
        id=role_batch[2].id,
        collections=modified_role_collections,
    ))

    r = api(scopes, api_client_collections).put('/role/', json=dict(
        id=role.id,
        scope_ids=scope_ids(role),
        collection_specific=role.collection_specific,
        collection_ids=[c.id for c in role.collections],
    ))

    if authorized:
        assert_empty_result(r)
        assert_db_state(modified_role_batch)
    else:
        assert_forbidden(r)
        assert_db_state(role_batch)


def test_update_role_not_found(api, role_batch, collection_auth):
    scopes = [ODPScope.ROLE_ADMIN]
    authorized = collection_auth in (CollectionAuth.NONE, CollectionAuth.MATCH)

    if collection_auth == CollectionAuth.MATCH:
        api_client_collections = role_batch[2].collections
    elif collection_auth == CollectionAuth.MISMATCH:
        api_client_collections = role_batch[1].collections
    else:
        api_client_collections = None

    if collection_auth in (CollectionAuth.MATCH, CollectionAuth.MISMATCH):
        modified_role_collections = role_batch[2].collections
    else:
        modified_role_collections = None

    role = role_build(
        id='foo',
        collections=modified_role_collections,
    )

    r = api(scopes, api_client_collections).put('/role/', json=dict(
        id=role.id,
        scope_ids=scope_ids(role),
        collection_specific=role.collection_specific,
        collection_ids=[c.id for c in role.collections],
    ))

    if authorized:
        assert_not_found(r)
    else:
        assert_forbidden(r)

    assert_db_state(role_batch)


@pytest.mark.parametrize('scopes', [
    [ODPScope.ROLE_ADMIN],
    [],
    all_scopes,
    all_scopes_excluding(ODPScope.ROLE_ADMIN),
])
def test_delete_role(api, role_batch, scopes, collection_auth):
    authorized = ODPScope.ROLE_ADMIN in scopes and \
                 collection_auth in (CollectionAuth.NONE, CollectionAuth.MATCH)

    if collection_auth == CollectionAuth.MATCH:
        api_client_collections = role_batch[2].collections
    elif collection_auth == CollectionAuth.MISMATCH:
        api_client_collections = role_batch[1].collections
    else:
        api_client_collections = None

    modified_role_batch = role_batch.copy()
    del modified_role_batch[2]

    r = api(scopes, api_client_collections).delete(f'/role/{role_batch[2].id}')

    if authorized:
        assert_empty_result(r)
        assert_db_state(modified_role_batch)
    else:
        assert_forbidden(r)
        assert_db_state(role_batch)


def test_delete_role_not_found(api, role_batch, collection_auth):
    scopes = [ODPScope.ROLE_ADMIN]

    if collection_auth == CollectionAuth.NONE:
        api_client_collections = None
    else:
        api_client_collections = role_batch[2].collections

    r = api(scopes, api_client_collections).delete('/role/foo')

    assert_not_found(r)
    assert_db_state(role_batch)
