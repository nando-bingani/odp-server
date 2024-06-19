from itertools import chain
from random import sample

import pytest

import migrate.systemdata
from odp.api.models.auth import Permission, Permissions, UserInfo
from odp.const import ODPScope
from odp.db.models import Collection, Provider, Scope
from odp.lib.auth import get_client_permissions, get_user_info, get_user_permissions
from test.factories import ClientFactory, CollectionFactory, FactorySession, ProviderFactory, RoleFactory, ScopeFactory, UserFactory


@pytest.fixture(params=['odp', 'any'])
def scopes(request):
    """Return a random sample of size 15 of scope DB objects.

    For the 'odp' parameterization, the sample consists only
    of ODP scopes.

    For the 'any' parameterization, the sample may include
    custom client scopes.
    """
    migrate.systemdata.init_system_scopes()
    migrate.systemdata.Session.commit()

    population = [(s.value, 'odp') for s in ODPScope]
    if request.param == 'any':
        population += [
            (custom_scope.id, 'client')
            for custom_scope in ScopeFactory.create_batch(20, type='client')
        ]

    return [
        FactorySession.get(Scope, scope_key)
        for scope_key in sample(population, 15)
    ]


@pytest.fixture(params=[0, 1, 3])
def role1_collection_count(request):
    return request.param


@pytest.fixture(params=[0, 1, 3])
def role2_collection_count(request):
    return request.param


@pytest.fixture(params=[0, 1, 3])
def user_provider_count(request):
    return request.param


@pytest.fixture(params=[False, True])
def user_has_client_provider(request):
    return request.param


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


def applicability(
        scope: Scope,
        provider_list: list[Provider],
        *collection_lists: list[Collection],
) -> Permission:
    """Return the applicable object ids for a constrainable scope from
    a provider list or series of collection lists (if given), otherwise
    the generally appliable '*'."""
    try:
        if provider_list is not None and ODPScope(scope.id).constrainable_by == 'provider':
            return [p.id for p in provider_list]

        if collection_lists and ODPScope(scope.id).constrainable_by == 'collection':
            return [c.id for c in chain(*collection_lists)]

    except ValueError:
        pass  # non-ODP scope

    return '*'


def test_unconstrained_roles(scopes, user_provider_count):
    """Test permissions calculations for unconstrained (not collection-specific) roles.

    The client is not provider-specific. Any client-provider and role-collection
    associations should be ignored.

    The expected user permissions result contains the intersection of client and
    role scopes. The value for provider-constrainable scopes is the set of providers
    associated with the user, and '*' for every other scope.
    """
    client = ClientFactory(scopes=scopes[1:14], provider_specific=False, provider=ProviderFactory())
    role1 = RoleFactory(scopes=scopes[:5], collection_specific=False, collections=[CollectionFactory()])
    role2 = RoleFactory(scopes=scopes[10:], collection_specific=False, collections=[CollectionFactory()])
    user = UserFactory(roles=(role1, role2))
    providers = ProviderFactory.create_batch(user_provider_count, users=[user])

    actual_client_perm = get_client_permissions(client.id)
    expected_client_perm = {
        scope.id: '*'
        for scope in scopes[1:14]
    }
    assert_compare(actual_client_perm, expected_client_perm)

    actual_user_perm = get_user_permissions(user.id, client.id)
    expected_user_perm = {
        scope.id: applicability(scope, providers)
        for n, scope in enumerate(scopes)
        if (1 <= n < 5) or (10 <= n < 14)
    }
    assert_compare(actual_user_perm, expected_user_perm)


def test_collection_specific_roles(scopes, role1_collection_count, role2_collection_count, user_provider_count):
    """Test permissions calculations for collection-specific roles.

    The client is not provider-specific. Any client-provider association
    should be ignored.

    The expected user permissions result contains the intersection of client and
    role scopes, with the value for collection-constrainable scopes being sets
    of role collections, union'd where the roles overlap.
    """
    client = ClientFactory(scopes=scopes[1:14], provider_specific=False, provider=ProviderFactory())
    role1 = RoleFactory(scopes=scopes[:10], collection_specific=True, collections=CollectionFactory.create_batch(role1_collection_count))
    role2 = RoleFactory(scopes=scopes[5:], collection_specific=True, collections=CollectionFactory.create_batch(role2_collection_count))
    user = UserFactory(roles=(role1, role2))
    providers = ProviderFactory.create_batch(user_provider_count, users=[user])

    actual_client_perm = get_client_permissions(client.id)
    expected_client_perm = {
        scope.id: '*'
        for scope in scopes[1:14]
    }
    assert_compare(actual_client_perm, expected_client_perm)

    actual_user_perm = get_user_permissions(user.id, client.id)
    expected_user_perm = {
                             scope.id: applicability(scope, providers, role1.collections)
                             for n, scope in enumerate(scopes)
                             if 1 <= n < 5
                         } | {
                             scope.id: applicability(scope, providers, role1.collections, role2.collections)
                             for n, scope in enumerate(scopes)
                             if 5 <= n < 10
                         } | {
                             scope.id: applicability(scope, providers, role2.collections)
                             for n, scope in enumerate(scopes)
                             if 10 <= n < 14
                         }
    assert_compare(actual_user_perm, expected_user_perm)


def test_unconstrained_and_collection_specific_role_mix(scopes, role1_collection_count, role2_collection_count, user_provider_count):
    """Test permissions calculations for a mix of unconstrained and collection-specific roles.

    The client is not provider-specific. Any client-provider association - and
    role-collection associations for unconstrained roles - should be ignored.

    The expected user permissions result contains the intersection of client and
    role scopes, with a value of '*' for unconstrainable scopes and collection-constrainable
    scopes allowed by unconstrained roles, the set of user providers for provider-constrainable
    scopes, and union'd sets of role collections for remaining scopes allowed by collection-specific roles.
    """
    client = ClientFactory(scopes=scopes[1:14], provider_specific=False, provider=ProviderFactory())
    role1 = RoleFactory(scopes=scopes[:7], collection_specific=False, collections=[CollectionFactory()])
    role2 = RoleFactory(scopes=scopes[3:12], collection_specific=True, collections=CollectionFactory.create_batch(role1_collection_count))
    role3 = RoleFactory(scopes=scopes[9:], collection_specific=True, collections=CollectionFactory.create_batch(role2_collection_count))
    user = UserFactory(roles=(role1, role2, role3))
    providers = ProviderFactory.create_batch(user_provider_count, users=[user])

    actual_client_perm = get_client_permissions(client.id)
    expected_client_perm = {
        scope.id: '*'
        for scope in scopes[1:14]
    }
    assert_compare(actual_client_perm, expected_client_perm)

    actual_user_perm = get_user_permissions(user.id, client.id)
    expected_user_perm = {
                             scope.id: applicability(scope, providers)
                             for n, scope in enumerate(scopes)
                             if 1 <= n < 7
                         } | {
                             scope.id: applicability(scope, providers, role2.collections)
                             for n, scope in enumerate(scopes)
                             if 7 <= n < 9
                         } | {
                             scope.id: applicability(scope, providers, role2.collections, role3.collections)
                             for n, scope in enumerate(scopes)
                             if 9 <= n < 12
                         } | {
                             scope.id: applicability(scope, providers, role3.collections)
                             for n, scope in enumerate(scopes)
                             if 12 <= n < 14
                         }
    assert_compare(actual_user_perm, expected_user_perm)


def test_provider_specific_client(scopes, user_provider_count, user_has_client_provider):
    """Test permissions calculations for a provider-specific client.

    The roles are not collection-specific. Any role-collection
    associations should be ignored.

    The expected user permissions result contains the intersection of
    client and role scopes. The value for provider-constrainable scopes
    is a set of the client provider if the user is associated with that
    provider, otherwise the empty set regardless of any other provider-user
    associations.
    """
    client = ClientFactory(scopes=scopes[1:14], provider_specific=True)
    role1 = RoleFactory(scopes=scopes[:5], collection_specific=False, collections=[CollectionFactory()])
    role2 = RoleFactory(scopes=scopes[10:], collection_specific=False, collections=[CollectionFactory()])
    user = UserFactory(roles=(role1, role2))
    ProviderFactory.create_batch(user_provider_count, users=[user])
    if user_has_client_provider:
        client.provider.users += [user]
        FactorySession.commit()
        providers = [client.provider]
    else:
        providers = []

    actual_client_perm = get_client_permissions(client.id)
    expected_client_perm = {
        scope.id: applicability(scope, [client.provider])
        for scope in scopes[1:14]
    }
    assert_compare(actual_client_perm, expected_client_perm)

    actual_user_perm = get_user_permissions(user.id, client.id)
    expected_user_perm = {
        scope.id: applicability(scope, providers)
        for n, scope in enumerate(scopes)
        if (1 <= n < 5) or (10 <= n < 14)
    }
    assert_compare(actual_user_perm, expected_user_perm)


def test_provider_specific_client_with_unconstrained_and_collection_specific_role_mix(
        scopes, role1_collection_count, role2_collection_count, user_provider_count, user_has_client_provider):
    """Test permissions calculations for a provider-specific client, with a mix
    of unconstrained and collection-specific roles.

    Any role-collection associations for unconstrained roles should be ignored.

    The expected user permissions result contains the intersection of client and
    role scopes. The value for provider-constrainable scopes is a set of the
    client provider if the user is associated with that provider, otherwise the
    empty set. The value for collection-constrainable scopes is '*' for scopes
    allowed by unconstrained roles, and union'd sets of role collections for
    scopes allowed by collection-specific roles.
    """
    client = ClientFactory(scopes=scopes[1:14], provider_specific=True)
    role1 = RoleFactory(scopes=scopes[:7], collection_specific=True, collections=CollectionFactory.create_batch(role1_collection_count))
    role2 = RoleFactory(scopes=scopes[3:12], collection_specific=True, collections=CollectionFactory.create_batch(role2_collection_count))
    role3 = RoleFactory(scopes=scopes[9:], collection_specific=False, collections=[CollectionFactory()])
    user = UserFactory(roles=(role1, role2, role3))
    ProviderFactory.create_batch(user_provider_count, users=[user])
    if user_has_client_provider:
        client.provider.users += [user]
        FactorySession.commit()
        providers = [client.provider]
    else:
        providers = []

    actual_client_perm = get_client_permissions(client.id)
    expected_client_perm = {
        scope.id: applicability(scope, [client.provider])
        for scope in scopes[1:14]
    }
    assert_compare(actual_client_perm, expected_client_perm)

    actual_user_perm = get_user_permissions(user.id, client.id)
    expected_user_perm = {
                             scope.id: applicability(scope, providers, role1.collections)
                             for n, scope in enumerate(scopes)
                             if 1 <= n < 3
                         } | {
                             scope.id: applicability(scope, providers, role1.collections, role2.collections)
                             for n, scope in enumerate(scopes)
                             if 3 <= n < 7
                         } | {
                             scope.id: applicability(scope, providers, role2.collections)
                             for n, scope in enumerate(scopes)
                             if 7 <= n < 9
                         } | {
                             scope.id: applicability(scope, providers)
                             for n, scope in enumerate(scopes)
                             if 9 <= n < 14
                         }
    assert_compare(actual_user_perm, expected_user_perm)


def test_user_info():
    """Test user info response.

    Note that all of a user's roles should be returned regardless of
    how they are configured.
    """
    role1 = RoleFactory(collection_specific=False, collections=[CollectionFactory()])
    role2 = RoleFactory(collection_specific=True, collections=CollectionFactory.create_batch(3))
    role3 = RoleFactory(collection_specific=True, collections=role2.collections[2:])
    role4 = RoleFactory(collection_specific=True, collections=role2.collections[:2])
    user = UserFactory(roles=(role1, role2, role3, role4))

    actual_user_info = get_user_info(user.id)
    actual_user_info.roles = set(actual_user_info.roles)
    expected_user_info = UserInfo(
        sub=user.id,
        email=user.email,
        email_verified=user.verified,
        name=user.name,
        picture=user.picture,
        roles={role1.id, role2.id, role3.id, role4.id},
    )
    assert actual_user_info == expected_user_info
