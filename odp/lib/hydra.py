from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from ory_hydra_client import ApiClient, Configuration
from ory_hydra_client.api.admin_api import AcceptConsentRequest, AcceptLoginRequest, AdminApi, OAuth2Client, RejectRequest
from ory_hydra_client.exceptions import ApiException
from ory_hydra_client.model.consent_request_session import ConsentRequestSession
from ory_hydra_client.model.o_auth2_token_introspection import OAuth2TokenIntrospection
from ory_hydra_client.model.string_slice_pipe_delimiter import StringSlicePipeDelimiter as StringArray

from odp.const.hydra import GrantType, ResponseType, TokenEndpointAuthMethod


@dataclass
class HydraClient:
    id: str
    name: str
    scope_ids: list[str]
    grant_types: list[GrantType]
    response_types: list[ResponseType]
    redirect_uris: list[str]
    post_logout_redirect_uris: list[str]
    token_endpoint_auth_method: TokenEndpointAuthMethod
    allowed_cors_origins: list[str]
    client_credentials_grant_access_token_lifespan: str | None

    @classmethod
    def from_oauth2_client(cls, oauth2_client: OAuth2Client) -> HydraClient:
        try:
            post_logout_redirect_uris = oauth2_client.post_logout_redirect_uris.value
        except AttributeError:
            # handle missing post_logout_redirect_uris on returned client credentials clients
            post_logout_redirect_uris = []

        return HydraClient(
            id=oauth2_client.client_id,
            name=oauth2_client.client_name,
            scope_ids=oauth2_client.scope.split(),
            grant_types=oauth2_client.grant_types.value,
            response_types=oauth2_client.response_types.value,
            redirect_uris=oauth2_client.redirect_uris.value,
            post_logout_redirect_uris=post_logout_redirect_uris,
            token_endpoint_auth_method=oauth2_client.token_endpoint_auth_method,
            allowed_cors_origins=oauth2_client.allowed_cors_origins.value,
            client_credentials_grant_access_token_lifespan=oauth2_client.client_credentials_grant_access_token_lifespan,
        )


class HydraAdminAPI:
    """A wrapper for the Hydra SDK's admin API."""

    def __init__(self, hydra_admin_url: str) -> None:
        self._api = AdminApi(ApiClient(Configuration(hydra_admin_url)))
        self._hydra_admin_url = hydra_admin_url

    def introspect_token(
            self,
            access_or_refresh_token: str,
            required_scope_ids: list[str] = None,
    ) -> OAuth2TokenIntrospection:
        """Check access/refresh token validity and return detailed
        token information."""
        kwargs = dict(token=access_or_refresh_token)
        if required_scope_ids is not None:
            kwargs |= dict(scope=' '.join(required_scope_ids))

        return self._api.introspect_o_auth2_token(**kwargs)

    def list_clients(self) -> list[HydraClient]:
        """Return a list of all OAuth2 clients from Hydra."""
        oauth2_clients = self._api.list_o_auth2_clients()
        return [HydraClient.from_oauth2_client(oauth2_client)
                for oauth2_client in oauth2_clients]

    def get_client(self, id: str) -> HydraClient:
        """Get an OAuth2 client configuration from Hydra."""
        oauth2_client = self._api.get_o_auth2_client(id=id)
        return HydraClient.from_oauth2_client(oauth2_client)

    def create_or_update_client(
            self,
            id: str,
            *,
            name: str,
            secret: str | None,
            scope_ids: Iterable[str],
            grant_types: Iterable[GrantType],
            response_types: Iterable[ResponseType] = (),
            redirect_uris: Iterable[str] = (),
            post_logout_redirect_uris: Iterable[str] = (),
            token_endpoint_auth_method: TokenEndpointAuthMethod = TokenEndpointAuthMethod.CLIENT_SECRET_BASIC,
            allowed_cors_origins: Iterable[str] = (),
            client_credentials_grant_access_token_lifespan: str = None,
    ) -> None:
        """Create or update an OAuth2 client configuration on Hydra.

        On update, pass `secret=None` to leave the client secret unchanged.
        """
        kwargs = dict(
            client_id=id,
            client_name=name,
            scope=' '.join(scope_ids),
            grant_types=StringArray(list(grant_types)),
            response_types=StringArray(list(response_types)),
            redirect_uris=StringArray(list(redirect_uris)),
            post_logout_redirect_uris=StringArray(list(post_logout_redirect_uris)),
            token_endpoint_auth_method=token_endpoint_auth_method,
            allowed_cors_origins=StringArray(list(allowed_cors_origins)),
            client_credentials_grant_access_token_lifespan=client_credentials_grant_access_token_lifespan,
            contacts=StringArray([]),
        )
        if secret is not None:
            kwargs |= dict(client_secret=secret)

        oauth2_client = OAuth2Client(**kwargs)
        try:
            self._api.create_o_auth2_client(oauth2_client)
        except ApiException as e:
            if e.status == 409:
                self._api.update_o_auth2_client(id, oauth2_client)
            else:
                raise  # todo: raise our own exception class here

    def delete_client(self, id: str) -> None:
        """Delete an OAuth2 client configuration from Hydra."""
        self._api.delete_o_auth2_client(id=id)

    def get_login_request(self, login_challenge: str) -> dict:
        """Get information about an active OAuth2 login request."""
        return self._api.get_login_request(login_challenge)

    def accept_login_request(
            self,
            login_challenge: str,
            user_id: str,
    ) -> str:
        """Inform Hydra that the user is authenticated, and return a redirect to Hydra."""
        r = self._api.accept_login_request(login_challenge, accept_login_request=AcceptLoginRequest(
            user_id,
            remember=True,
            remember_for=30 * 86400,  # remember login for 30 days
        ))
        return r['redirect_to']

    def reject_login_request(
            self,
            login_challenge: str,
            error_code: str,
            error_description: str,
    ) -> str:
        """Inform Hydra that the user is not authenticated, and return a redirect to Hydra."""
        r = self._api.reject_login_request(login_challenge, reject_request=RejectRequest(
            error=error_code,
            error_description=error_description,
        ))
        return r['redirect_to']

    def get_consent_request(self, consent_challenge: str) -> dict:
        """Get information about an active OAuth2 consent request."""
        return self._api.get_consent_request(consent_challenge)

    def accept_consent_request(
            self,
            consent_challenge: str,
            authorized_scope_ids: Iterable[str],
            authorized_api_uris: Iterable[str],
            access_token_data: dict,
            id_token_data: dict,
    ) -> str:
        """Inform Hydra that the user has authorized the OAuth2 client to
        interact - on the user's behalf - with the given APIs under the
        given scope, and return a redirect to Hydra."""
        r = self._api.accept_consent_request(consent_challenge, accept_consent_request=AcceptConsentRequest(
            grant_scope=StringArray(list(authorized_scope_ids)),
            grant_access_token_audience=StringArray(list(authorized_api_uris)),
            remember=True,
            remember_for=0,  # remember consent indefinitely
            session=ConsentRequestSession(
                access_token=access_token_data,
                id_token=id_token_data,
            )
        ))
        return r['redirect_to']

    def reject_consent_request(
            self,
            consent_challenge: str,
            error_code: str,
            error_description: str,
    ) -> str:
        """Inform Hydra that the user has not authorized the OAuth2 client,
        and return a redirect to Hydra."""
        r = self._api.reject_consent_request(consent_challenge, reject_request=RejectRequest(
            error=error_code,
            error_description=error_description,
        ))
        return r['redirect_to']

    def get_logout_request(self, logout_challenge: str) -> dict:
        """Get information about an active OAuth2 logout request."""
        return self._api.get_logout_request(logout_challenge)

    def accept_logout_request(self, logout_challenge: str) -> str:
        """Confirm a logout with Hydra, and return a redirect to Hydra."""
        r = self._api.accept_logout_request(logout_challenge)
        return r['redirect_to']

    def reject_logout_request(
            self,
            logout_challenge: str,
            error_code: str,
            error_description: str,
    ) -> str:
        """Deny a logout request with Hydra, and return a redirect to Hydra."""
        r = self._api.reject_logout_request(logout_challenge, reject_request=RejectRequest(
            error=error_code,
            error_description=error_description,
        ))
        return r['redirect_to']
