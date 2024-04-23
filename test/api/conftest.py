from collections import namedtuple

import pytest
from starlette.testclient import TestClient

import migrate.systemdata
import odp.api.main
from odp.config import config
from odp.const import ODPScope
from odp.const.db import TagCardinality
from odp.db.models import Collection, Provider, Scope
from odp.lib.hydra import HydraAdminAPI
from test.api import all_scopes, all_scopes_excluding
from test.factories import ClientFactory, FactorySession, RoleFactory, UserFactory

MockToken = namedtuple('MockToken', ('active', 'client_id', 'sub'))


# TODO:
#   consider using pytest_generate_tests to parameterize interacting fixtures;
#   this would avoid the unnecessary overhead of test setup for invalid parameter
#   combinations, which are skipped in any case


@pytest.fixture(autouse=True)
def static_data():
    """Initialize static system data."""
    migrate.systemdata.init_system_scopes()
    migrate.systemdata.Session.commit()


@pytest.fixture(params=['client_credentials', 'authorization_code'])
def api(request, monkeypatch):
    """Fixture returning an API test client constructor. Example usages::

        r = api(scopes).get('/catalog/')

        r = api(scopes, user_collections=authorized_collections).post('/record/', json=dict(
            doi=record.doi,
            metadata=record.metadata_,
            ...,
        ))

    Each parameterization of the calling test is invoked twice: first
    to simulate a machine client with a client_credentials grant; second
    to simulate a UI client with an authorization_code grant.

    :param scopes: iterable of ODPScope granted to the test client/user
    :param client_provider: constrain the test client's package/resource access to the specified Provider
    :param user_providers: constrain the test user's package/resource access to the specified Providers
    :param user_collections: constrain the test user's collection/record access to the specified Collections
    """

    def api_test_client(
            scopes: list[ODPScope],
            *,
            client_provider: Provider = None,
            user_providers: list[Provider] = None,
            user_collections: list[Collection] = None,
    ):
        scope_objects = [FactorySession.get(Scope, (s.value, 'odp')) for s in scopes]

        if request.param == 'authorization_code':
            # for authorization_code we grant the test client all scopes
            all_scope_objects = [FactorySession.get(Scope, (s.value, 'odp')) for s in ODPScope]

            odp_user = UserFactory(
                id='odp.test.user',
                name='Test User',
                roles=[RoleFactory(
                    id='odp.test.role',
                    scopes=scope_objects,
                    collection_specific=user_collections is not None,
                    collections=user_collections,
                )])

            for provider in user_providers or ():
                provider.users += [odp_user]

        odp_client = ClientFactory(
            id='odp.test.client',
            scopes=scope_objects if request.param == 'client_credentials' else all_scope_objects,
            provider_specific=client_provider is not None,
            provider=client_provider,
        )

        monkeypatch.setattr(HydraAdminAPI, 'introspect_token', lambda *args: MockToken(
            active=True,
            client_id=odp_client.id,
            sub=odp_user.id if request.param == 'authorization_code' else odp_client.id,
        ))

        return TestClient(
            app=odp.api.main.app,
            headers={
                'Accept': 'application/json',
                'Authorization': 'Bearer t0k3n',
            }
        )

    api_test_client.grant_type = request.param
    return api_test_client


@pytest.fixture
def hydra_admin_api():
    """Returns a HydraAdminAPI instance providing access to the dockerized
    Hydra test server.

    A dummy Hydra client is created to correspond with the ODP test client,
    and all Hydra clients are deleted following the test.
    """
    try:
        hapi = HydraAdminAPI(config.HYDRA.ADMIN.URL)
        hapi.create_or_update_client('odp.test.client', name='foo', secret=None, scope_ids=['bar'], grant_types=[])
        yield hapi
    finally:
        for hydra_client in hapi.list_clients():
            hapi.delete_client(hydra_client.id)


@pytest.fixture(params=[
    'collection_any',
    'collection_match',
    'collection_mismatch',
])
def collection_constraint(request):
    """Indicate to the test function how to configure the collections
    associated with the test user's role (`user_collections` param of
    the `api` fixture) when using a collection-constrainable scope.

    'collection_any'      => The test role is not collection-specific
    'collection_match'    => The test role is associated with the collection
                             including the requested objects (usually the #2
                             collection of the batch)
    'collection_mismatch' => The test role is associated with arbitrary
                             collection(s) not associated with the requested
                             objects

    Note that collection access can only be constrained under the authorization_code
    flow, when we have a test user whose role can be made collection-specific.
    Under client_credentials, the test function should skip collection_match/mismatch
    by calling `try_skip_collection_constraint`.
    """
    return request.param


def try_skip_collection_constraint(grant_type, collection_constraint):
    """Tests which use the `collection_constraint` fixture should call this
    function to skip non-applicable combinations of grant_type and collection_constraint."""
    if grant_type == 'client_credentials' and collection_constraint != 'collection_any':
        pytest.skip('Collection access cannot be constrained under client_credentials as there is no test user/role')


@pytest.fixture(params=[
    'client_provider_any',
    'client_provider_match',
    'client_provider_mismatch',
])
def client_provider_constraint(request):
    """Indicate to the test function how to configure the test client
    (`client_provider` param of `api` fixture) when using a provider-
    constrainable scope.

    'client_provider_any'      => The test client is not provider-specific
    'client_provider_match'    => The test client is specific to the provider
                                  of the requested objects (usually the #2
                                  provider of the batch)
    'client_provider_mismatch' => The test client is specific to an arbitrary
                                  provider, not associated with the requested
                                  objects
    """
    return request.param


@pytest.fixture(params=[
    'user_provider_none',
    'user_provider_match',
    'user_provider_mismatch',
])
def user_provider_constraint(request):
    """Indicate to the test function how to configure the test user
    (`user_providers` param of `api` fixture) when using a provider-
    constrainable scope.

    'user_provider_none'     => The test user is not associated with any provider
    'user_provider_match'    => The test user is associated with the provider of
                                the requested objects (usually the #2 provider of
                                the batch)
    'user_provider_mismatch' => The test user is associated with arbitrary provider(s)
                                not associated with the requested objects

    Note that user-provider access can only be configured under the authorization_code
    flow, when we have a test user. Under client_credentials, the test function should
    skip user_provider_match/mismatch by calling `try_skip_user_provider_constraint`.
    """
    return request.param


def try_skip_user_provider_constraint(grant_type, user_provider_constraint):
    """Tests which use the `user_provider_constraint` fixture should call this
    function to skip non-applicable combinations of grant_type and user_provider_constraint."""
    if grant_type == 'client_credentials' and user_provider_constraint != 'user_provider_none':
        pytest.skip('User-provider configuration is irrelevant under client_credentials')


@pytest.fixture(params=TagCardinality)
def tag_cardinality(request):
    """Use for parameterizing the range of tag cardinalities."""
    return request.param


@pytest.fixture(params=['scope_match', 'scope_none', 'scope_all', 'scope_excl'])
def scopes(request):
    """Fixture for parameterizing the set of auth scopes
    to be associated with the API test client.

    The test function must be decorated to indicated the scope
    required by the API route::

        @pytest.mark.require_scope(ODPScope.CATALOG_READ)

    This has the same effect as parameterizing the test function
    as follows::

        @pytest.mark.parametrize('scopes', [
            [ODPScope.CATALOG_READ],
            [],
            all_scopes,
            all_scopes_excluding(ODPScope.CATALOG_READ),
        ])

    """
    scope = request.node.get_closest_marker('require_scope').args[0]

    if request.param == 'scope_match':
        return [scope]
    elif request.param == 'scope_none':
        return []
    elif request.param == 'scope_all':
        return all_scopes
    elif request.param == 'scope_excl':
        return all_scopes_excluding(scope)


def pytest_configure(config):
    config.addinivalue_line(
        'markers', 'require_scope(odpscope): mark API test with ODPScope required by API route'
    )
