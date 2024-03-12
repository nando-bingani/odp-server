from datetime import datetime, timedelta, timezone

import pytest

from odp.const import ODPScope

all_scopes = [s for s in ODPScope]


def all_scopes_excluding(scope):
    return [s for s in ODPScope if s != scope]


def assert_empty_result(response):
    assert response.status_code == 200
    assert response.json() is None


def assert_forbidden(response):
    assert response.status_code == 403
    assert response.json() == {'detail': 'Forbidden'}


def assert_not_found(response):
    assert response.status_code == 404
    assert response.json() == {'detail': 'Not Found'}


def assert_method_not_allowed(response):
    assert response.status_code == 405
    assert response.json() == {'detail': 'Method Not Allowed'}


def assert_conflict(response, message):
    assert response.status_code == 409
    assert response.json() == {'detail': message}


def assert_unprocessable(response, message=None, **kwargs):
    # kwargs are key-value pairs expected within 'detail'
    assert response.status_code == 422
    error_detail = response.json()['detail']
    if message is not None:
        assert error_detail == message
    for k, v in kwargs.items():
        assert error_detail[k] == v


def assert_new_timestamp(timestamp):
    # 1 hour is a bit lenient, but handy for debugging
    assert (now := datetime.now(timezone.utc)) - timedelta(minutes=60) < timestamp < now


def assert_redirect(response, url):
    assert response.is_redirect
    assert response.next_request.url == url


def skip_client_credentials_collection_constraint(grant_type, collection_constraint):
    if grant_type == 'client_credentials' and collection_constraint != 'collection_any':
        pytest.skip('Collections cannot be constrained under client_credentials as there is no test user/role')
