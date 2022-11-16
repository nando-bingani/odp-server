from odp.lib.auth import Permissions, UserInfo, get_client_permissions, get_user_info, get_user_permissions
from test.factories import ClientFactory, CollectionFactory, RoleFactory, ScopeFactory, UserFactory


def assert_compare(expected: Permissions, actual: Permissions):
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
    scopes = ScopeFactory.create_batch(8, type='odp')
    client = ClientFactory(scopes=scopes[1:7], collection_specific=False)
    role1 = RoleFactory(scopes=scopes[:3], collection_specific=False)
    role2 = RoleFactory(scopes=scopes[5:], collection_specific=False)
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
    scopes = ScopeFactory.create_batch(8, type='odp')
    client = ClientFactory(scopes=scopes[1:7], collection_specific=False)
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
    scopes = ScopeFactory.create_batch(8, type='odp')
    client = ClientFactory(scopes=scopes[1:7], collection_specific=False)
    role1 = RoleFactory(scopes=scopes[:4], collection_specific=False)
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
    scopes = ScopeFactory.create_batch(8, type='odp')
    client = ClientFactory(scopes=scopes[1:7], collection_specific=True, collections=CollectionFactory.create_batch(3))
    role1 = RoleFactory(scopes=scopes[:3], collection_specific=False)
    role2 = RoleFactory(scopes=scopes[5:], collection_specific=False)
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
    scopes = ScopeFactory.create_batch(8, type='odp')
    client = ClientFactory(scopes=scopes[1:7], collection_specific=True, collections=CollectionFactory.create_batch(3))
    role1 = RoleFactory(scopes=scopes[:3], collection_specific=False)
    role2 = RoleFactory(scopes=scopes[3:5], collection_specific=True, collections=CollectionFactory.create_batch(3))
    role3 = RoleFactory(scopes=scopes[5:], collection_specific=True, collections=client.collections[1:])
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
    client = ClientFactory(collection_specific=False)
    role1 = RoleFactory(collection_specific=False)
    role2 = RoleFactory(collection_specific=False)
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
    client = ClientFactory(collection_specific=False)
    role1 = RoleFactory(collection_specific=False)
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
    client = ClientFactory(collection_specific=True, collections=CollectionFactory.create_batch(3))
    role1 = RoleFactory(collection_specific=False)
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
