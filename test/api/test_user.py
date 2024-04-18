from random import randint

import pytest
from sqlalchemy import select

from odp.const import ODPScope
from odp.db.models import IdentityAudit, User, UserRole
from test import TestSession
from test.api import (
    all_scopes, assert_empty_result, assert_forbidden, assert_method_not_allowed, assert_new_timestamp,
    assert_not_found, assert_unprocessable,
)
from test.factories import CollectionTagFactory, ProviderFactory, RecordTagFactory, RoleFactory, UserFactory


@pytest.fixture
def user_batch():
    """Create and commit a batch of User instances, with
    associated roles and providers."""
    users = [
        UserFactory(roles=RoleFactory.create_batch(randint(0, 3)))
        for _ in range(randint(3, 5))
    ]
    for user in users:
        ProviderFactory.create_batch(randint(0, 3), users=[user]),
        user.role_ids = [role.id for role in user.roles]
        user.provider_keys = {provider.id: provider.key for provider in user.providers}

    return users


def user_build(**attr):
    """Build and return an uncommitted User instance.
    Associated roles are however committed."""
    user = UserFactory.build(
        **attr,
        roles=RoleFactory.create_batch(randint(0, 3)),
    )
    user.role_ids = [role.id for role in user.roles]
    user.provider_keys = {}
    return user


def assert_db_state(users):
    """Verify that the user table contains the given user batch, and
    that the user_role table contains the associated role references."""
    result = TestSession.execute(
        select(User).where(User.id != 'odp.test.user')).scalars().all()
    result.sort(key=lambda u: u.id)
    users.sort(key=lambda u: u.id)
    assert len(result) == len(users)
    for n, row in enumerate(result):
        assert row.id == users[n].id
        assert row.name == users[n].name
        assert row.email == users[n].email
        assert row.active == users[n].active
        assert row.verified == users[n].verified
        assert row.picture == users[n].picture

    result = TestSession.execute(
        select(UserRole.user_id, UserRole.role_id).where(UserRole.user_id != 'odp.test.user')).all()
    result.sort(key=lambda ur: (ur.user_id, ur.role_id))
    user_roles = []
    for user in users:
        for role_id in user.role_ids:
            user_roles += [(user.id, role_id)]
    user_roles.sort()
    assert result == user_roles


def assert_audit_log(command, user, grant_type):
    """Verify that the identity audit table contains the given entry."""
    result = TestSession.execute(select(IdentityAudit)).scalar_one()
    assert result.client_id == 'odp.test.client'
    assert result.user_id == ('odp.test.user' if grant_type == 'authorization_code' else None)
    assert result.command == command
    assert result.completed is True
    assert result.error is None
    assert_new_timestamp(result.timestamp)
    assert result._id == user.id
    assert result._email == user.email
    assert result._active == user.active
    assert sorted(result._roles) == sorted(user.role_ids)


def assert_no_audit_log():
    """Verify that no audit log entries have been created."""
    assert TestSession.execute(select(IdentityAudit)).first() is None


def assert_json_result(response, json, user):
    """Verify that the API result matches the given user object."""
    assert response.status_code == 200
    assert json['id'] == user.id
    assert json['name'] == user.name
    assert json['email'] == user.email
    assert json['active'] == user.active
    assert json['verified'] == user.verified
    assert json['picture'] == user.picture
    assert sorted(json['role_ids']) == sorted(user.role_ids)
    assert json['provider_keys'] == user.provider_keys


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
    # the user update API can only modify `active` and `role_ids`;
    # everything else must stay the same on the rebuilt user object
    user = user_build(
        id=user_batch[2].id,
        name=user_batch[2].name,
        email=user_batch[2].email,
        verified=user_batch[2].verified,
        picture=user_batch[2].picture,
    )

    r = api(scopes).put('/user/', json=dict(
        id=user.id,
        active=user.active,
        role_ids=user.role_ids,
    ))

    if authorized:
        assert_empty_result(r)
        assert_db_state(user_batch[:2] + [user] + user_batch[3:])
        assert_audit_log('edit', user, api.grant_type)
    else:
        assert_forbidden(r)
        assert_db_state(user_batch)
        assert_no_audit_log()


def test_update_user_not_found(api, user_batch):
    scopes = [ODPScope.USER_ADMIN]
    user = user_build(
        id='foo',
        name=user_batch[2].name,
        email=user_batch[2].email,
        verified=user_batch[2].verified,
        picture=user_batch[2].picture,
    )
    r = api(scopes).put('/user/', json=dict(
        id=user.id,
        active=user.active,
        role_ids=user.role_ids,
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
    deleted_user = user_batch[2]

    if has_tag_instance in ('collection', 'both'):
        CollectionTagFactory(user=deleted_user)
    if has_tag_instance in ('record', 'both'):
        RecordTagFactory(user=deleted_user)

    r = api(scopes).delete(f'/user/{deleted_user.id}')

    if authorized:
        if has_tag_instance in ('collection', 'record', 'both'):
            assert_unprocessable(r, 'The user cannot be deleted due to associated tag instance data.')
            assert_db_state(user_batch)
            assert_no_audit_log()
        else:
            assert_empty_result(r)
            assert_db_state(user_batch[:2] + user_batch[3:])
            assert_audit_log('delete', deleted_user, api.grant_type)
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
