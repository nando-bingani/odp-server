from random import choice, randint

import pytest
from sqlalchemy import select

from odp.const import ODPScope
from odp.db import Session
from odp.db.models import Role
from test.api import assert_conflict, assert_empty_result, assert_forbidden, assert_not_found, assert_unprocessable
from test.factories import CollectionFactory, RoleFactory, ScopeFactory


@pytest.fixture
def role_batch():
    """Create and commit a batch of Role instances."""
    return [
        RoleFactory(
            scopes=ScopeFactory.create_batch(randint(0, 3), type=choice(('odp', 'client'))),
            collection_specific=(collection_specific := randint(0, 1)),
            collections=CollectionFactory.create_batch(randint(0, 3)) if collection_specific else None,
        )
        for _ in range(randint(3, 5))
    ]


def role_build(collection_specific=None, **id):
    """Build and return an uncommitted Role instance.
    Referenced scopes and collections are however committed."""
    if collection_specific is None:
        collection_specific = randint(0, 1)

    return RoleFactory.build(
        **id,
        scopes=ScopeFactory.create_batch(randint(0, 3), type=choice(('odp', 'client'))),
        collection_specific=collection_specific,
        collections=CollectionFactory.create_batch(randint(0, 3)) if collection_specific else None,
    )


def scope_ids(role):
    return tuple(sorted(scope.id for scope in role.scopes))


def collection_ids(role):
    return tuple(sorted(collection.id for collection in role.collections))


def collection_keys(role):
    return {
        collection.id: collection.key for collection in role.collections
    } if role.collection_specific else {}


def assert_db_state(roles):
    """Verify that the DB role table contains the given role batch."""
    Session.expire_all()
    result = Session.execute(select(Role).where(Role.id != 'odp.test.role')).scalars().all()
    assert set((row.id, scope_ids(row), row.collection_specific, collection_ids(row)) for row in result) \
           == set((role.id, scope_ids(role), role.collection_specific, collection_ids(role)) for role in roles)


def assert_json_result(response, json, role):
    """Verify that the API result matches the given role object."""
    assert response.status_code == 200
    assert json['id'] == role.id
    assert json['collection_specific'] == role.collection_specific
    assert json['collection_keys'] == collection_keys(role)
    assert tuple(sorted(json['scope_ids'])) == scope_ids(role)


def assert_json_results(response, json, roles):
    """Verify that the API result list matches the given role batch."""
    items = [j for j in json['items'] if j['id'] != 'odp.test.role']
    assert len(items) == len(roles)
    items.sort(key=lambda i: i['id'])
    roles.sort(key=lambda r: r.id)
    for n, role in enumerate(roles):
        assert_json_result(response, items[n], role)


@pytest.mark.require_scope(ODPScope.ROLE_READ)
def test_list_roles(api, role_batch, scopes):
    authorized = ODPScope.ROLE_READ in scopes
    r = api(scopes).get('/role/')
    if authorized:
        assert_json_results(r, r.json(), role_batch)
    else:
        assert_forbidden(r)
    assert_db_state(role_batch)


@pytest.mark.require_scope(ODPScope.ROLE_READ)
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


@pytest.mark.require_scope(ODPScope.ROLE_ADMIN)
def test_create_role(api, role_batch, scopes):
    authorized = ODPScope.ROLE_ADMIN in scopes
    modified_role_batch = role_batch + [role := role_build()]

    r = api(scopes).post('/role/', json=dict(
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


def test_create_role_conflict(api, role_batch):
    scopes = [ODPScope.ROLE_ADMIN]
    role = role_build(id=role_batch[2].id)

    r = api(scopes).post('/role/', json=dict(
        id=role.id,
        scope_ids=scope_ids(role),
        collection_specific=role.collection_specific,
        collection_ids=[c.id for c in role.collections],
    ))

    assert_conflict(r, 'Role id is already in use')
    assert_db_state(role_batch)


def test_create_role_admin_collection_specific(api, role_batch):
    scopes = [ODPScope.ROLE_ADMIN]
    role = role_build(collection_specific=True)

    r = api(scopes).post('/role/', json=dict(
        id=role.id,
        scope_ids=list(scope_ids(role)) + [ODPScope.ROLE_ADMIN],
        collection_specific=role.collection_specific,
        collection_ids=[c.id for c in role.collections],
    ))

    assert_unprocessable(r, "Scope 'odp.role:admin' cannot be granted to a collection-specific role.")
    assert_db_state(role_batch)


@pytest.mark.require_scope(ODPScope.ROLE_ADMIN)
def test_update_role(api, role_batch, scopes):
    authorized = ODPScope.ROLE_ADMIN in scopes
    modified_role_batch = role_batch.copy()
    modified_role_batch[2] = (role := role_build(id=role_batch[2].id))

    r = api(scopes).put('/role/', json=dict(
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


def test_update_role_not_found(api, role_batch):
    scopes = [ODPScope.ROLE_ADMIN]
    role = role_build(id='foo')

    r = api(scopes).put('/role/', json=dict(
        id=role.id,
        scope_ids=scope_ids(role),
        collection_specific=role.collection_specific,
        collection_ids=[c.id for c in role.collections],
    ))

    assert_not_found(r)
    assert_db_state(role_batch)


def test_update_role_admin_collection_specific(api, role_batch):
    scopes = [ODPScope.ROLE_ADMIN]
    role = role_build(id=role_batch[2].id, collection_specific=True)

    r = api(scopes).put('/role/', json=dict(
        id=role.id,
        scope_ids=list(scope_ids(role)) + [ODPScope.ROLE_ADMIN],
        collection_specific=role.collection_specific,
        collection_ids=[c.id for c in role.collections],
    ))

    assert_unprocessable(r, "Scope 'odp.role:admin' cannot be granted to a collection-specific role.")
    assert_db_state(role_batch)


@pytest.mark.require_scope(ODPScope.ROLE_ADMIN)
def test_delete_role(api, role_batch, scopes):
    authorized = ODPScope.ROLE_ADMIN in scopes
    modified_role_batch = role_batch.copy()
    del modified_role_batch[2]

    r = api(scopes).delete(f'/role/{role_batch[2].id}')

    if authorized:
        assert_empty_result(r)
        assert_db_state(modified_role_batch)
    else:
        assert_forbidden(r)
        assert_db_state(role_batch)


def test_delete_role_not_found(api, role_batch):
    scopes = [ODPScope.ROLE_ADMIN]
    r = api(scopes).delete('/role/foo')
    assert_not_found(r)
    assert_db_state(role_batch)
