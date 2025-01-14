from random import randint

import pytest
from sqlalchemy import select

from odp.const import ODPScope
from odp.db.models import Scope
from test import TestSession
from test.api.assertions import assert_forbidden
from test.factories import ScopeFactory


@pytest.fixture
def scope_batch():
    """Create and commit a batch of Scope instances."""
    return [
        ScopeFactory()
        for _ in range(randint(3, 5))
    ]


def assert_db_state(scopes):
    """Verify that the DB scope table contains the given scope batch."""
    result = TestSession.execute(select(Scope)).scalars().all()
    assert set((row.id, row.type) for row in result) \
           == set((scope.id, scope.type) for scope in scopes)


def assert_json_result(response, json, scope):
    """Verify that the API result matches the given scope object."""
    assert response.status_code == 200
    assert json['id'] == scope.id
    assert json['type'] == scope.type


def assert_json_results(response, json, scopes):
    """Verify that the API result list matches the given scope batch."""
    items = json['items']
    assert json['total'] == len(items) == len(scopes)
    items.sort(key=lambda i: i['id'])
    scopes.sort(key=lambda s: s.id)
    for n, scope in enumerate(scopes):
        assert_json_result(response, items[n], scope)


@pytest.mark.require_scope(ODPScope.SCOPE_READ)
def test_list_scopes(api, scope_batch, scopes):
    authorized = ODPScope.SCOPE_READ in scopes
    # add ODP scopes to the batch of expected scopes,
    # as they are created by the static_data fixture
    scope_batch += [ScopeFactory.build(id=s.value, type='odp') for s in ODPScope]
    r = api(scopes).get('/scope/?size=0')
    if authorized:
        assert_json_results(r, r.json(), scope_batch)
    else:
        assert_forbidden(r)
    assert_db_state(scope_batch)
