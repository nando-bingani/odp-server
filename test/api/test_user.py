from random import randint

import pytest
from sqlalchemy import select

from odp.const import ODPScope
from odp.db import Session
from odp.db.models import IdentityAudit, User
from test.api import (
    all_scopes, assert_empty_result, assert_forbidden, assert_method_not_allowed, assert_new_timestamp,
    assert_not_found, assert_unprocessable,
)
from test.factories import CollectionTagFactory, ProviderFactory, RecordTagFactory, RoleFactory, UserFactory


@pytest.fixture
def user_batch():
    """Create and commit a batch of User instances."""
    return [
        UserFactory(
            roles=RoleFactory.create_batch(randint(0, 3)),
            providers=ProviderFactory.create_batch(randint(0, 3)),
        )
        for _ in range(randint(3, 5))
    ]


def role_ids(user):
    return tuple(sorted(role.id for role in user.roles))


def provider_ids(user):
    return tuple(sorted(provider.id for provider in user.providers))


def provider_keys(user):
    return {provider.id: provider.key for provider in user.providers}


def assert_db_state(users):
    """Verify that the DB user table contains the given user batch."""
    Session.expire_all()
    result = Session.execute(select(User).where(User.id != 'odp.test.user')).scalars().all()
    result.sort(key=lambda u: u.id)
    users.sort(key=lambda u: u.id)
    assert len(result) == len(users)
    for n, row in enumerate(result):
        assert row.id == users[n].id
        assert row.name == users[n].name
        assert row.email == users[n].email
        assert row.active == users[n].active
        assert row.verified == users[n].verified
        assert role_ids(row) == role_ids(users[n])
        assert provider_ids(row) == provider_ids(users[n])


def assert_audit_log(command, user, user_role_ids, user_provider_ids, grant_type):
    """Verify that the identity audit table contains the given entry."""
    result = Session.execute(select(IdentityAudit)).scalar_one()
    assert result.client_id == 'odp.test.client'
    assert result.user_id == ('odp.test.user' if grant_type == 'authorization_code' else None)
    assert result.command == command
    assert result.completed is True
    assert result.error is None
    assert_new_timestamp(result.timestamp)
    assert result._id == user.id
    assert result._email == user.email
    assert result._active == user.active
    assert tuple(sorted(result._roles)) == user_role_ids
    assert tuple(sorted(result._providers)) == user_provider_ids


def assert_no_audit_log():
    """Verify that no audit log entries have been created."""
    assert Session.execute(select(IdentityAudit)).first() is None


def assert_json_result(response, json, user):
    """Verify that the API result matches the given user object."""
    assert response.status_code == 200
    assert json['id'] == user.id
    assert json['name'] == user.name
    assert json['email'] == user.email
    assert json['active'] == user.active
    assert json['verified'] == user.verified
    assert tuple(sorted(json['role_ids'])) == role_ids(user)
    assert json['provider_keys'] == provider_keys(user)


def assert_json_results(response, json, users):
    """Verify that the API result list matches the given user batch."""
    items = [j for j in json['items'] if j['id'] != 'odp.test.user']
    assert len(items) == len(users)
    items.sort(key=lambda i: i['id'])
    users.sort(key=lambda u: u.id)
    for n, user in enumerate(users):
        assert_json_result(response, items[n], user)


@pytest.mark.require_scope(ODPScope.USER_READ)
def test_list_users(api, user_batch, scopes):
    authorized = ODPScope.USER_READ in scopes
    r = api(scopes).get('/user/')
    if authorized:
        assert_json_results(r, r.json(), user_batch)
    else:
        assert_forbidden(r)
    assert_db_state(user_batch)
    assert_no_audit_log()


@pytest.mark.require_scope(ODPScope.USER_READ)
def test_get_user(api, user_batch, scopes):
    authorized = ODPScope.USER_READ in scopes
    r = api(scopes).get(f'/user/{user_batch[2].id}')
    if authorized:
        assert_json_result(r, r.json(), user_batch[2])
    else:
        assert_forbidden(r)
    assert_db_state(user_batch)
    assert_no_audit_log()


def test_get_user_not_found(api, user_batch):
    scopes = [ODPScope.USER_READ]
    r = api(scopes).get('/user/foo')
    assert_not_found(r)
    assert_db_state(user_batch)
    assert_no_audit_log()


def test_create_user(api):
    r = api(all_scopes).post('/user/')
    assert_method_not_allowed(r)
    assert_no_audit_log()


@pytest.mark.require_scope(ODPScope.USER_ADMIN)
def test_update_user(api, user_batch, scopes):
    authorized = ODPScope.USER_ADMIN in scopes
    modified_user_batch = user_batch.copy()
    modified_user_batch[2] = (user := UserFactory.build(
        id=user_batch[2].id,
        name=user_batch[2].name,
        email=user_batch[2].email,
        verified=user_batch[2].verified,
        roles=RoleFactory.create_batch(randint(0, 3)),
        providers=ProviderFactory.create_batch(randint(0, 3)),
    ))
    r = api(scopes).put('/user/', json=dict(
        id=user.id,
        active=user.active,
        role_ids=role_ids(user),
        provider_ids=provider_ids(user),
    ))
    if authorized:
        assert_empty_result(r)
        assert_db_state(modified_user_batch)
        assert_audit_log('edit', user, role_ids(user), provider_ids(user), api.grant_type)
    else:
        assert_forbidden(r)
        assert_db_state(user_batch)
        assert_no_audit_log()


def test_update_user_not_found(api, user_batch):
    scopes = [ODPScope.USER_ADMIN]
    user = UserFactory.build(
        id='foo',
        name=user_batch[2].name,
        email=user_batch[2].email,
        verified=user_batch[2].verified,
        roles=RoleFactory.create_batch(randint(0, 3)),
        providers=ProviderFactory.create_batch(randint(0, 3)),
    )
    r = api(scopes).put('/user/', json=dict(
        id=user.id,
        active=user.active,
        role_ids=role_ids(user),
        provider_ids=provider_ids(user),
    ))
    assert_not_found(r)
    assert_db_state(user_batch)
    assert_no_audit_log()


@pytest.fixture(params=['none', 'collection', 'record', 'both'])
def has_tag_instance(request):
    return request.param


@pytest.mark.require_scope(ODPScope.USER_ADMIN)
def test_delete_user(api, user_batch, scopes, has_tag_instance):
    authorized = ODPScope.USER_ADMIN in scopes
    modified_user_batch = user_batch.copy()
    deleted_user = modified_user_batch[2]
    deleted_user_role_ids = role_ids(deleted_user)
    deleted_user_provider_ids = provider_ids(deleted_user)
    del modified_user_batch[2]

    if has_tag_instance in ('collection', 'both'):
        CollectionTagFactory(user=user_batch[2])
    if has_tag_instance in ('record', 'both'):
        RecordTagFactory(user=user_batch[2])

    r = api(scopes).delete(f'/user/{user_batch[2].id}')

    if authorized:
        if has_tag_instance in ('collection', 'record', 'both'):
            assert_unprocessable(r, 'The user cannot be deleted due to associated tag instance data.')
            assert_db_state(user_batch)
            assert_no_audit_log()
        else:
            assert_empty_result(r)
            # check audit log first because assert_db_state expires the deleted item
            assert_audit_log('delete', deleted_user, deleted_user_role_ids, deleted_user_provider_ids, api.grant_type)
            assert_db_state(modified_user_batch)
    else:
        assert_forbidden(r)
        assert_db_state(user_batch)
        assert_no_audit_log()


def test_delete_user_not_found(api, user_batch):
    scopes = [ODPScope.USER_ADMIN]
    r = api(scopes).delete('/user/foo')
    assert_not_found(r)
    assert_db_state(user_batch)
    assert_no_audit_log()
