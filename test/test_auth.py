from odp.lib.auth import Permissions, UserInfo, get_client_permissions, get_user_info, get_user_permissions
from test.factories import ClientFactory, CollectionFactory, RoleFactory, ScopeFactory, UserFactory


def assert_compare(expected: Permissions, actual: Permissions):
    """Compare expected and actual permissions results. The order of
    collection ids does not matter."""
    if expected == actual:
        return

    expected = {
        scope_id: set(collection_ids) if collection_ids != '*' else '*'
        for scope_id, collection_ids in expected.items()
    }
    actual = {
        scope_id: set(collection_ids) if collection_ids != '*' else '*'
        for scope_id, collection_ids in actual.items()
    }
    assert expected == actual


def test_platform_roles():
    """Test permissions calculations for 'platform' (not collection-specific) roles.

    The client is not collection-specific. Any client-collection and role-collection
    associations should be ignored.

    The expected user permissions result contains the intersection of client and
    role scopes, with a value of '*' for each scope.
    """
    scopes = ScopeFactory.create_batch(8, type='odp')
    client = ClientFactory(scopes=scopes[1:7], collection_specific=False, collections=[CollectionFactory()])
    role1 = RoleFactory(scopes=scopes[:3], collection_specific=False, collections=[CollectionFactory()])
    role2 = RoleFactory(scopes=scopes[5:], collection_specific=False, collections=[CollectionFactory()])
    user = UserFactory(roles=(role1, role2))

    actual_user_perm = get_user_permissions(user.id, client.id)
    expected_user_perm = {
        scope.id: '*'
        for n, scope in enumerate(scopes)
        if n in (1, 2, 5, 6)
    }
    assert_compare(expected_user_perm, actual_user_perm)

    actual_client_perm = get_client_permissions(client.id)
    expected_client_perm = {
        scope.id: '*'
        for scope in scopes[1:7]
    }
    assert_compare(expected_client_perm, actual_client_perm)


def test_collection_roles():
    """Test permissions calculations for collection-specific roles.

    The client is not collection-specific. Any client-collection associations
    should be ignored.

    The expected user permissions result contains the intersection of client and
    role scopes, with the value for each scope being the set of applicable role
    collections.
    """
    scopes = ScopeFactory.create_batch(8, type='odp')
    client = ClientFactory(scopes=scopes[1:7], collection_specific=False, collections=[CollectionFactory()])
    role1 = RoleFactory(scopes=scopes[:5], collection_specific=True, collections=CollectionFactory.create_batch(3))
    role2 = RoleFactory(scopes=scopes[3:], collection_specific=True, collections=CollectionFactory.create_batch(3))
    user = UserFactory(roles=(role1, role2))

    actual_user_perm = get_user_permissions(user.id, client.id)
    expected_user_perm = {
                             scope.id: [c.id for c in role1.collections]
                             for n, scope in enumerate(scopes)
                             if n in (1, 2)
                         } | {
                             scope.id: [c.id for c in role1.collections] + [c.id for c in role2.collections]
                             for n, scope in enumerate(scopes)
                             if n in (3, 4)
                         } | {
                             scope.id: [c.id for c in role2.collections]
                             for n, scope in enumerate(scopes)
                             if n in (5, 6)
                         }
    assert_compare(expected_user_perm, actual_user_perm)

    actual_client_perm = get_client_permissions(client.id)
    expected_client_perm = {
        scope.id: '*'
        for scope in scopes[1:7]
    }
    assert_compare(expected_client_perm, actual_client_perm)


def test_platform_collection_role_mix():
    """Test permissions calculations for a mix of platform and collection-specific roles.

    The client is not collection-specific. Any client-collection associations - and
    role-collection associations for platform roles - should be ignored.

    The expected user permissions result contains the intersection of client and
    role scopes, with a value of '*' for scopes allowed by platform roles, and the
    set of applicable role collections for remaining scopes allowed by collection-specific
    roles.
    """
    scopes = ScopeFactory.create_batch(8, type='odp')
    client = ClientFactory(scopes=scopes[1:7], collection_specific=False, collections=[CollectionFactory()])
    role1 = RoleFactory(scopes=scopes[:4], collection_specific=False, collections=[CollectionFactory()])
    role2 = RoleFactory(scopes=scopes[3:], collection_specific=True, collections=CollectionFactory.create_batch(3))
    role3 = RoleFactory(scopes=scopes[5:], collection_specific=True, collections=CollectionFactory.create_batch(3))
    user = UserFactory(roles=(role1, role2, role3))

    actual_user_perm = get_user_permissions(user.id, client.id)
    expected_user_perm = {
                             scope.id: '*'
                             for n, scope in enumerate(scopes)
                             if n in (1, 2, 3)
                         } | {
                             scope.id: [c.id for c in role2.collections]
                             for n, scope in enumerate(scopes)
                             if n == 4
                         } | {
                             scope.id: [c.id for c in role2.collections] + [c.id for c in role3.collections]
                             for n, scope in enumerate(scopes)
                             if n in (5, 6)
                         }
    assert_compare(expected_user_perm, actual_user_perm)

    actual_client_perm = get_client_permissions(client.id)
    expected_client_perm = {
        scope.id: '*'
        for scope in scopes[1:7]
    }
    assert_compare(expected_client_perm, actual_client_perm)


def test_collection_client():
    """Test permissions calculations for a collection-specific client.

    The roles are not collection-specific. Any role-collection associations
    should be ignored.

    The expected user permissions result contains the intersection of client and
    role scopes, with the value for each scope being the set of client collections.
    """
    scopes = ScopeFactory.create_batch(8, type='odp')
    client = ClientFactory(scopes=scopes[1:7], collection_specific=True, collections=CollectionFactory.create_batch(3))
    role1 = RoleFactory(scopes=scopes[:3], collection_specific=False, collections=[CollectionFactory()])
    role2 = RoleFactory(scopes=scopes[5:], collection_specific=False, collections=[CollectionFactory()])
    user = UserFactory(roles=(role1, role2))

    actual_user_perm = get_user_permissions(user.id, client.id)
    expected_user_perm = {
        scope.id: [c.id for c in client.collections]
        for n, scope in enumerate(scopes)
        if n in (1, 2, 5, 6)
    }
    assert_compare(expected_user_perm, actual_user_perm)

    actual_client_perm = get_client_permissions(client.id)
    expected_client_perm = {
        scope.id: [c.id for c in client.collections]
        for scope in scopes[1:7]
    }
    assert_compare(expected_client_perm, actual_client_perm)


def test_collection_client_platform_collection_role_mix():
    """Test permissions calculations for a collection-specific client, with a mix
    of platform and collection-specific roles.

    Any role-collection associations for platform roles should be ignored.

    The expected user permissions result contains the intersection of client and
    role scopes, with the value for each scope being the set of client collections
    for scopes allowed by platform roles, and the intersection of client and role
    collections for remaining scopes allowed by collection-specific roles. If the
    latter intersection is empty, the scope should not appear in the result.
    """
    scopes = ScopeFactory.create_batch(8, type='odp')
    client = ClientFactory(scopes=scopes[1:7], collection_specific=True, collections=CollectionFactory.create_batch(3))
    role1 = RoleFactory(scopes=scopes[:3], collection_specific=False, collections=[CollectionFactory()])
    role2 = RoleFactory(scopes=scopes[3:5], collection_specific=True, collections=CollectionFactory.create_batch(3))
    role3 = RoleFactory(scopes=scopes[5:], collection_specific=True, collections=client.collections[1:] + [CollectionFactory()])
    user = UserFactory(roles=(role1, role2, role3))

    actual_user_perm = get_user_permissions(user.id, client.id)
    expected_user_perm = {
                             scope.id: [c.id for c in client.collections]
                             for n, scope in enumerate(scopes)
                             if n in (1, 2)
                         } | {
                             scope.id: [c.id for c in client.collections[1:]]
                             for n, scope in enumerate(scopes)
                             if n in (5, 6)
                         }
    assert_compare(expected_user_perm, actual_user_perm)

    actual_client_perm = get_client_permissions(client.id)
    expected_client_perm = {
        scope.id: [c.id for c in client.collections]
        for scope in scopes[1:7]
    }
    assert_compare(expected_client_perm, actual_client_perm)


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
