from random import randint

import pytest
from sqlalchemy import select

from odp.const import ODPScope
from odp.db.models import Keyword, KeywordAudit
from test import TestSession
from test.api import assert_forbidden, assert_new_timestamp, assert_not_found
from test.factories import KeywordFactory


@pytest.fixture
def keyword_batch(request):
    """Create and commit a batch of Keyword instances, which
    may include sub-keywords, recursively."""
    return KeywordFactory.create_batch(randint(3, 5))


def keyword_build(**id):
    """Build and return an uncommitted Keyword instance."""
    return KeywordFactory.build(**id)


def assert_db_state(keywords):
    """Verify that the keyword table contains the given keyword batch."""

    def assert_result(kw):
        row = result.pop(kw.key)
        assert (row.key, row.data, row.status, row.parent_key, row.child_schema_id, row.child_schema_type) == \
               (kw.key, kw.data, kw.status, kw.parent_key, kw.child_schema_id, 'keyword' if kw.child_schema_id else None)
        for child_kw in kw.children:
            assert_result(child_kw)

    result = {
        row.key: row
        for row in TestSession.execute(select(Keyword)).scalars()
    }
    for keyword in keywords:
        assert_result(keyword)
    assert not result  # should have popped all


def assert_audit_log(command, keyword, grant_type):
    result = TestSession.execute(select(KeywordAudit)).scalar_one()
    assert result.client_id == 'odp.test.client'
    assert result.user_id == ('odp.test.user' if grant_type == 'authorization_code' else None)
    assert result.command == command
    assert_new_timestamp(result.timestamp)
    assert result._key == keyword.key
    assert result._data == keyword.data
    assert result._status == keyword.status
    assert result._schema_id == keyword.schema_id


def assert_no_audit_log():
    assert TestSession.execute(select(KeywordAudit)).first() is None


def assert_json_result(response, json, keyword, recurse=False):
    """Verify that the API result matches the given keyword object."""
    assert response.status_code == 200
    assert json['key'] == keyword.key
    assert json['data'] == keyword.data
    assert json['status'] == keyword.status

    schema = None
    parent = keyword.parent
    while parent is not None:
        if schema := parent.child_schema:
            break
        parent = parent.parent

    assert json['schema_id'] == (schema.id if schema else None)

    if recurse:
        assert (child_keywords := json['child_keywords']) is not None
        assert len(child_keywords) == len(keyword.children)
        for i, child_keyword in enumerate(child_keywords):
            assert_json_result(response, child_keyword, keyword.children[i], True)
    else:
        assert 'child_keywords' not in json


def assert_json_results(response, json, keywords, recurse=False):
    """Verify that the API result list matches the given keyword batch."""
    items = json['items']
    assert json['total'] == len(items) == len(keywords)
    items.sort(key=lambda i: i['key'])
    keywords.sort(key=lambda k: k.key)
    for n, keyword in enumerate(keywords):
        assert_json_result(response, items[n], keyword, recurse)


@pytest.mark.require_scope(ODPScope.KEYWORD_READ)
def test_list_vocabularies(
        api,
        scopes,
        keyword_batch,
):
    authorized = ODPScope.KEYWORD_READ in scopes

    r = api(scopes).get('/keyword/')

    if authorized:
        assert_json_results(r, r.json(), keyword_batch)
    else:
        assert_forbidden(r)

    assert_db_state(keyword_batch)
    assert_no_audit_log()


@pytest.mark.require_scope(ODPScope.KEYWORD_READ)
@pytest.mark.parametrize('recurse', [False, True])
def test_list_keywords(
        api,
        scopes,
        keyword_batch,
        recurse,
):
    authorized = ODPScope.KEYWORD_READ in scopes
    client = api(scopes)

    r = client.get(f'/keyword/{keyword_batch[2].key}/?recurse={recurse}')
    if authorized:
        assert_json_results(r, r.json(), keyword_batch[2].children, recurse)
    else:
        assert_forbidden(r)

    try:
        r = client.get(f'/keyword/{keyword_batch[1].children[1].key}/?recurse={recurse}')
        if authorized:
            assert_json_results(r, r.json(), keyword_batch[1].children[1].children, recurse)
        else:
            assert_forbidden(r)
    except IndexError:
        pass

    try:
        r = client.get(f'/keyword/{keyword_batch[0].children[0].children[0].key}/?recurse={recurse}')
        if authorized:
            assert_json_results(r, r.json(), keyword_batch[0].children[0].children[0].children, recurse)
        else:
            assert_forbidden(r)
    except IndexError:
        pass

    assert_db_state(keyword_batch)
    assert_no_audit_log()


@pytest.mark.parametrize('recurse', [False, True])
def test_list_keywords_not_found(
        api,
        keyword_batch,
        recurse,
):
    scopes = [ODPScope.KEYWORD_READ]
    client = api(scopes)

    r = client.get(f'/keyword/foo/?recurse={recurse}')
    assert_not_found(r, "Parent keyword 'foo' does not exist")

    r = client.get(f'/keyword/{(key := keyword_batch[2].key)}/foo/?recurse={recurse}')
    assert_not_found(r, f"Parent keyword '{key}/foo' does not exist")

    try:
        r = client.get(f'/keyword/{(key := keyword_batch[1].children[1].key)}/foo/?recurse={recurse}')
        assert_not_found(r, f"Parent keyword '{key}/foo' does not exist")
    except IndexError:
        pass

    assert_db_state(keyword_batch)
    assert_no_audit_log()


@pytest.mark.require_scope(ODPScope.KEYWORD_READ)
@pytest.mark.parametrize('recurse', [False, True])
def test_get_keyword(
        api,
        scopes,
        keyword_batch,
        recurse,
):
    authorized = ODPScope.KEYWORD_READ in scopes
    client = api(scopes)

    r = client.get(f'/keyword/{keyword_batch[2].key}?recurse={recurse}')
    if authorized:
        assert_json_result(r, r.json(), keyword_batch[2], recurse)
    else:
        assert_forbidden(r)

    try:
        r = client.get(f'/keyword/{keyword_batch[1].children[1].key}?recurse={recurse}')
        if authorized:
            assert_json_result(r, r.json(), keyword_batch[1].children[1], recurse)
        else:
            assert_forbidden(r)
    except IndexError:
        pass

    try:
        r = client.get(f'/keyword/{keyword_batch[0].children[0].children[0].key}?recurse={recurse}')
        if authorized:
            assert_json_result(r, r.json(), keyword_batch[0].children[0].children[0], recurse)
        else:
            assert_forbidden(r)
    except IndexError:
        pass

    assert_db_state(keyword_batch)
    assert_no_audit_log()


@pytest.mark.parametrize('recurse', [False, True])
def test_get_keyword_not_found(
        api,
        keyword_batch,
        recurse,
):
    scopes = [ODPScope.KEYWORD_READ]
    client = api(scopes)

    r = client.get(f'/keyword/foo?recurse={recurse}')
    assert_not_found(r, "Keyword 'foo' does not exist")

    r = client.get(f'/keyword/{(key := keyword_batch[2].key)}/foo?recurse={recurse}')
    assert_not_found(r, f"Keyword '{key}/foo' does not exist")

    try:
        r = client.get(f'/keyword/{(key := keyword_batch[1].children[1].key)}/foo?recurse={recurse}')
        assert_not_found(r, f"Keyword '{key}/foo' does not exist")
    except IndexError:
        pass

    assert_db_state(keyword_batch)
    assert_no_audit_log()
