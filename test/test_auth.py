import random

import pytest

import migrate.systemdata
from odp.api.models.auth import Permissions, UserInfo
from odp.const import ODPScope
from odp.db import Session
from odp.db.models import Scope
from odp.lib.auth import get_client_permissions, get_role_permissions, get_user_info, get_user_permissions
from test.factories import ClientFactory, CollectionFactory, RoleFactory, UserFactory


@pytest.fixture
def scopes():
    """Return a random sample of size 15 of ODP scope DB objects."""
    migrate.systemdata.init_system_scopes()
    scope_ids = [s.value for s in random.sample(list(ODPScope.__members__.values()), 15)]
    return [Session.get(Scope, (scope_id, 'odp')) for scope_id in scope_ids]


def assert_compare(actual: Permissions, expected: Permissions):
    """Compare actual and expected permissions results. The order of
    object ids does not matter."""
    if actual == expected:
        return

    actual = {
        scope_id: set(object_ids) if object_ids != '*' else '*'
        for scope_id, object_ids in actual.items()
    }
    expected = {
        scope_id: set(object_ids) if object_ids != '*' else '*'
        for scope_id, object_ids in expected.items()
    }
    assert actual == expected


def test_unconstrained_roles(scopes):
    """Test permissions calculations for unconstrained (not collection- or provider-
    specific) roles.

    The client is not collection-specific. Any client-collection and role-collection
    associations should be ignored.

    The expected user permissions result contains the intersection of client and
    role scopes, with a value of '*' for every scope.
    """
    client = ClientFactory(scopes=scopes[1:14], collection_specific=False, collections=[CollectionFactory()])
    role1 = RoleFactory(scopes=scopes[:5], collection_specific=False, collections=[CollectionFactory()])
    role2 = RoleFactory(scopes=scopes[10:], collection_specific=False, collections=[CollectionFactory()])
    user = UserFactory(roles=(role1, role2))

    actual_client_perm = get_client_permissions(client.id)
    expected_client_perm = {
        scope.id: '*'
        for scope in scopes[1:14]
    }
    assert_compare(actual_client_perm, expected_client_perm)

    actual_role1_perm = get_role_permissions(role1.id)
    expected_role1_perm = {
        scope.id: '*'
        for scope in scopes[:5]
    }
    assert_compare(actual_role1_perm, expected_role1_perm)

    actual_role2_perm = get_role_permissions(role2.id)
    expected_role2_perm = {
        scope.id: '*'
        for scope in scopes[10:]
    }
    assert_compare(actual_role2_perm, expected_role2_perm)

    actual_user_perm = get_user_permissions(user.id, client.id)
    expected_user_perm = {
        scope.id: '*'
        for n, scope in enumerate(scopes)
        if (1 <= n < 5) or (10 <= n < 14)
    }
    assert_compare(actual_user_perm, expected_user_perm)


def test_collection_specific_roles(scopes):
    """Test permissions calculations for collection-specific roles.

    The client is not collection-specific. Any client-collection associations
    should be ignored.

    The expected user permissions result contains the intersection of client and
    role scopes, with the value for constrainable scopes being sets of role
    collections, union'd where the roles overlap.
    """
    client = ClientFactory(scopes=scopes[1:14], collection_specific=False, collections=[CollectionFactory()])
    role1 = RoleFactory(scopes=scopes[:10], collection_specific=True, collections=CollectionFactory.create_batch(3))
    role2 = RoleFactory(scopes=scopes[5:], collection_specific=True, collections=CollectionFactory.create_batch(3))
    user = UserFactory(roles=(role1, role2))

    actual_client_perm = get_client_permissions(client.id)
    expected_client_perm = {
        scope.id: '*'
        for scope in scopes[1:14]
    }
    assert_compare(actual_client_perm, expected_client_perm)

    actual_role1_perm = get_role_permissions(role1.id)
    expected_role1_perm = {
        scope.id: [c.id for c in role1.collections]
        if ODPScope(scope.id).constrainable_by == 'collection' else '*'
        for scope in scopes[:10]
    }
    assert_compare(actual_role1_perm, expected_role1_perm)

    actual_role2_perm = get_role_permissions(role2.id)
    expected_role2_perm = {
        scope.id: [c.id for c in role2.collections]
        if ODPScope(scope.id).constrainable_by == 'collection' else '*'
        for scope in scopes[5:]
    }
    assert_compare(actual_role2_perm, expected_role2_perm)

    actual_user_perm = get_user_permissions(user.id, client.id)
    expected_user_perm = {
                             scope.id: [c.id for c in role1.collections]
                             if ODPScope(scope.id).constrainable_by == 'collection' else '*'
                             for n, scope in enumerate(scopes)
                             if 1 <= n < 5
                         } | {
                             scope.id: [c.id for c in role1.collections] + [c.id for c in role2.collections]
                             if ODPScope(scope.id).constrainable_by == 'collection' else '*'
                             for n, scope in enumerate(scopes)
                             if 5 <= n < 10
                         } | {
                             scope.id: [c.id for c in role2.collections]
                             if ODPScope(scope.id).constrainable_by == 'collection' else '*'
                             for n, scope in enumerate(scopes)
                             if 10 <= n < 14
                         }
    assert_compare(actual_user_perm, expected_user_perm)


def test_unconstrained_and_collection_specific_role_mix(scopes):
    """Test permissions calculations for a mix of unconstrained and collection-specific roles.

    The client is not collection-specific. Any client-collection associations - and
    role-collection associations for unconstrained roles - should be ignored.

    The expected user permissions result contains the intersection of client and
    role scopes, with a value of '*' for unconstrainable scopes and scopes allowed by
    unconstrained roles, and union'd sets of role collections for remaining scopes
    allowed by collection-specific roles.
    """
    client = ClientFactory(scopes=scopes[1:14], collection_specific=False, collections=[CollectionFactory()])
    role1 = RoleFactory(scopes=scopes[:7], collection_specific=False, collections=[CollectionFactory()])
    role2 = RoleFactory(scopes=scopes[3:12], collection_specific=True, collections=CollectionFactory.create_batch(3))
    role3 = RoleFactory(scopes=scopes[9:], collection_specific=True, collections=CollectionFactory.create_batch(3))
    user = UserFactory(roles=(role1, role2, role3))

    actual_client_perm = get_client_permissions(client.id)
    expected_client_perm = {
        scope.id: '*'
        for scope in scopes[1:14]
    }
    assert_compare(actual_client_perm, expected_client_perm)

    actual_role1_perm = get_role_permissions(role1.id)
    expected_role1_perm = {
        scope.id: '*'
        for scope in scopes[:7]
    }
    assert_compare(actual_role1_perm, expected_role1_perm)

    actual_role2_perm = get_role_permissions(role2.id)
    expected_role2_perm = {
        scope.id: [c.id for c in role2.collections]
        if ODPScope(scope.id).constrainable_by == 'collection' else '*'
        for scope in scopes[3:12]
    }
    assert_compare(actual_role2_perm, expected_role2_perm)

    actual_role3_perm = get_role_permissions(role3.id)
    expected_role3_perm = {
        scope.id: [c.id for c in role3.collections]
        if ODPScope(scope.id).constrainable_by == 'collection' else '*'
        for scope in scopes[9:]
    }
    assert_compare(actual_role3_perm, expected_role3_perm)

    actual_user_perm = get_user_permissions(user.id, client.id)
    expected_user_perm = {
                             scope.id: '*'
                             for n, scope in enumerate(scopes)
                             if 1 <= n < 7
                         } | {
                             scope.id: [c.id for c in role2.collections]
                             if ODPScope(scope.id).constrainable_by == 'collection' else '*'
                             for n, scope in enumerate(scopes)
                             if 7 <= n < 9
                         } | {
                             scope.id: [c.id for c in role2.collections] + [c.id for c in role3.collections]
                             if ODPScope(scope.id).constrainable_by == 'collection' else '*'
                             for n, scope in enumerate(scopes)
                             if 9 <= n < 12
                         } | {
                             scope.id: [c.id for c in role3.collections]
                             if ODPScope(scope.id).constrainable_by == 'collection' else '*'
                             for n, scope in enumerate(scopes)
                             if 12 <= n < 14
                         }
    assert_compare(actual_user_perm, expected_user_perm)


def test_collection_specific_client(scopes):
    """Test permissions calculations for a collection-specific client.

    The roles are not collection- or provider-specific. Any role-collection
    associations should be ignored.

    The expected user permissions result contains the intersection of client and
    role scopes, with the value for constrainable scopes being the set of client
    collections.
    """
    client = ClientFactory(scopes=scopes[1:14], collection_specific=True, collections=CollectionFactory.create_batch(3))
    role1 = RoleFactory(scopes=scopes[:5], collection_specific=False, collections=[CollectionFactory()])
    role2 = RoleFactory(scopes=scopes[10:], collection_specific=False, collections=[CollectionFactory()])
    user = UserFactory(roles=(role1, role2))

    actual_client_perm = get_client_permissions(client.id)
    expected_client_perm = {
        scope.id: [c.id for c in client.collections]
        if ODPScope(scope.id).constrainable_by == 'collection' else '*'
        for scope in scopes[1:14]
    }
    assert_compare(actual_client_perm, expected_client_perm)

    actual_role1_perm = get_role_permissions(role1.id)
    expected_role1_perm = {
        scope.id: '*'
        for scope in scopes[:5]
    }
    assert_compare(actual_role1_perm, expected_role1_perm)

    actual_role2_perm = get_role_permissions(role2.id)
    expected_role2_perm = {
        scope.id: '*'
        for scope in scopes[10:]
    }
    assert_compare(actual_role2_perm, expected_role2_perm)

    actual_user_perm = get_user_permissions(user.id, client.id)
    expected_user_perm = {
        scope.id: [c.id for c in client.collections]
        if ODPScope(scope.id).constrainable_by == 'collection' else '*'
        for n, scope in enumerate(scopes)
        if (1 <= n < 5) or (10 <= n < 14)
    }
    assert_compare(actual_user_perm, expected_user_perm)


def test_collection_specific_client_with_unconstrained_and_collection_specific_role_mix(scopes):
    """Test permissions calculations for a collection-specific client, with a mix
    of unconstrained and collection-specific roles.

    Any role-collection associations for unconstrained roles should be ignored.

    The expected user permissions result contains the intersection of client and
    role scopes. The value for each constrainable scope is the set of client collections
    for scopes allowed by unconstrained roles, and the intersection of client and role
    collections for scopes allowed by collection-specific roles. If the latter
    intersection is empty, the scope should not appear in the result.
    """
    client = ClientFactory(scopes=scopes[1:14], collection_specific=True, collections=CollectionFactory.create_batch(3))
    role1 = RoleFactory(scopes=scopes[:5], collection_specific=True, collections=client.collections[1:] + [CollectionFactory()])
    role2 = RoleFactory(scopes=scopes[4:11], collection_specific=True, collections=CollectionFactory.create_batch(3))
    role3 = RoleFactory(scopes=scopes[10:], collection_specific=False, collections=[CollectionFactory()])
    user = UserFactory(roles=(role1, role2, role3))

    actual_client_perm = get_client_permissions(client.id)
    expected_client_perm = {
        scope.id: [c.id for c in client.collections]
        if ODPScope(scope.id).constrainable_by == 'collection' else '*'
        for scope in scopes[1:14]
    }
    assert_compare(actual_client_perm, expected_client_perm)

    actual_role1_perm = get_role_permissions(role1.id)
    expected_role1_perm = {
        scope.id: [c.id for c in role1.collections]
        if ODPScope(scope.id).constrainable_by == 'collection' else '*'
        for scope in scopes[:5]
    }
    assert_compare(actual_role1_perm, expected_role1_perm)

    actual_role2_perm = get_role_permissions(role2.id)
    expected_role2_perm = {
        scope.id: [c.id for c in role2.collections]
        if ODPScope(scope.id).constrainable_by == 'collection' else '*'
        for scope in scopes[4:11]
    }
    assert_compare(actual_role2_perm, expected_role2_perm)

    actual_role3_perm = get_role_permissions(role3.id)
    expected_role3_perm = {
        scope.id: '*'
        for scope in scopes[10:]
    }
    assert_compare(actual_role3_perm, expected_role3_perm)

    actual_user_perm = get_user_permissions(user.id, client.id)
    expected_user_perm = {
                             scope.id: [c.id for c in client.collections[1:]]
                             if ODPScope(scope.id).constrainable_by == 'collection' else '*'
                             for n, scope in enumerate(scopes)
                             if 1 <= n < 5
                         } | {
                             # exclude collection-constrainable scopes in the 'middle' section
                             # because intersection between client and role2 collections is empty
                             scope.id: '*'
                             for n, scope in enumerate(scopes)
                             if 5 <= n < 10 and ODPScope(scope.id).constrainable_by != 'collection'
                         } | {
                             scope.id: [c.id for c in client.collections]
                             if ODPScope(scope.id).constrainable_by == 'collection' else '*'
                             for n, scope in enumerate(scopes)
                             if 10 <= n < 14
                         }
    assert_compare(actual_user_perm, expected_user_perm)


def test_user_info():
    """Test user info result with non-collection-specific client and roles."""
    client = ClientFactory(collection_specific=False, collections=[CollectionFactory()])
    role1 = RoleFactory(collection_specific=False, collections=[CollectionFactory()])
    role2 = RoleFactory(collection_specific=False, collections=[CollectionFactory()])
    user = UserFactory(roles=(role1, role2))

    actual_user_info = get_user_info(user.id, client.id)
    expected_user_info = UserInfo(
        sub=user.id,
        email=user.email,
        email_verified=user.verified,
        name=user.name,
        picture=None,
        roles=[role1.id, role2.id],
    )
    assert expected_user_info == actual_user_info


def test_user_info_collection_roles():
    """Test user info result with a non-collection-specific client and a mix
    of platform and collection-specific roles."""
    client = ClientFactory(collection_specific=False, collections=[CollectionFactory()])
    role1 = RoleFactory(collection_specific=False, collections=[CollectionFactory()])
    role2 = RoleFactory(collection_specific=True, collections=CollectionFactory.create_batch(3))
    role3 = RoleFactory(collection_specific=True, collections=CollectionFactory.create_batch(3))
    user = UserFactory(roles=(role1, role2, role3))

    actual_user_info = get_user_info(user.id, client.id)
    expected_user_info = UserInfo(
        sub=user.id,
        email=user.email,
        email_verified=user.verified,
        name=user.name,
        picture=None,
        roles=[role1.id, role2.id, role3.id],
    )
    assert expected_user_info == actual_user_info


def test_user_info_collection_roles_and_client():
    """Test user info result with a collection-specific client and a mix
    of platform and collection-specific roles.

    Collection-specific roles that do not share any collections with the
    client should not appear in the user's role list.
    """
    client = ClientFactory(collection_specific=True, collections=CollectionFactory.create_batch(3))
    role1 = RoleFactory(collection_specific=False, collections=[CollectionFactory()])
    role2 = RoleFactory(collection_specific=True, collections=client.collections[:2])
    role3 = RoleFactory(collection_specific=True, collections=client.collections[2:])
    role4 = RoleFactory(collection_specific=True, collections=CollectionFactory.create_batch(3))
    user = UserFactory(roles=(role1, role2, role3, role4))

    actual_user_info = get_user_info(user.id, client.id)
    expected_user_info = UserInfo(
        sub=user.id,
        email=user.email,
        email_verified=user.verified,
        name=user.name,
        picture=None,
        roles=[role1.id, role2.id, role3.id],
    )
    assert expected_user_info == actual_user_info
