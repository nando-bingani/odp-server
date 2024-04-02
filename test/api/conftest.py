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


@pytest.fixture(params=['collection_any', 'collection_match', 'collection_mismatch'])
def collection_constraint(request):
    """Fixture for parameterizing the three possible logic branches
    involving scopes that may be constrained to specific collections.

    'collection_any'      => The test user has a non-collection-specific role
    'collection_match'    => The test user has a collection-specific role, and is
                             requesting access to authorized collection(s)
    'collection_mismatch' => The test user has a collection-specific role, and is
                             requesting access to unauthorized collection(s)

    Note that collection access can only be constrained under the authorization_code
    flow, when we have a test user whose role can be made collection-specific.
    Under client_credentials, the calling test may skip collection_match/mismatch.
    """
    return request.param


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
