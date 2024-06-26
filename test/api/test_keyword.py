from random import randint

import pytest
from sqlalchemy import select

from odp.const import ODPScope
from odp.db.models import Keyword, KeywordAudit, Schema
from test import TestSession
from test.api import assert_conflict, assert_empty_result, assert_forbidden, assert_new_timestamp, assert_not_found, assert_unprocessable
from test.factories import FactorySession, KeywordFactory, fake


@pytest.fixture
def keyword_batch(request):
    """Create and commit a batch of Keyword instances, which
    may include sub-keywords, recursively. Return a tuple of
    (top-level keywords, all keywords).
    """
    keywords_top = KeywordFactory.create_batch(randint(3, 5))
    keywords_flat = FactorySession.execute(select(Keyword)).scalars().all()
    return keywords_top, keywords_flat


def keyword_build(**attr):
    """Build and return an uncommitted Keyword instance."""
    return KeywordFactory.build(
        children=[],
        **attr,
    )


def assert_db_state(keywords_flat):
    """Verify that the keyword table contains the given keyword batch."""
    result = TestSession.execute(select(Keyword)).scalars().all()
    result.sort(key=lambda k: k.key)
    keywords_flat.sort(key=lambda k: k.key)
    assert len(result) == len(keywords_flat)
    for n, row in enumerate(result):
        kw = keywords_flat[n]
        assert row.key == kw.key
        assert row.data == kw.data
        assert row.status == kw.status
        assert row.parent_key == kw.parent_key
        assert row.child_schema_id == kw.child_schema_id
        assert row.child_schema_type == ('keyword' if kw.child_schema_id else None)


def assert_audit_log(grant_type, *entries):
    result = TestSession.execute(select(KeywordAudit)).scalars().all()
    assert len(result) == len(entries)
    for n, row in enumerate(result):
        assert row.client_id == 'odp.test.client'
        assert row.user_id == ('odp.test.user' if grant_type == 'authorization_code' else None)
        assert row.command == entries[n]['command']
        assert_new_timestamp(row.timestamp)
        keyword = entries[n]['keyword']
        assert row._key == keyword.key
        assert row._data == keyword.data
        assert row._status == keyword.status
        assert row._child_schema_id == keyword.child_schema_id


def assert_no_audit_log():
    assert TestSession.execute(select(KeywordAudit)).first() is None


def assert_json_result(response, json, keyword, recurse=False):
    """Verify that the API result matches the given keyword object."""
    assert response.status_code == 200
    assert json['key'] == keyword.key
    assert json['data'] == keyword.data
    assert json['status'] == keyword.status

    # we have to get the parent using TestSession (rather than FactorySession)
    # because the parent's schema might have changed in an earlier iteration
    # in test_set_keyword
    schema = None
    parent = TestSession.get(Keyword, keyword.parent_key)
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
    keywords_top, keywords_flat = keyword_batch
    authorized = ODPScope.KEYWORD_READ in scopes

    r = api(scopes).get('/keyword/')

    if authorized:
        assert_json_results(r, r.json(), keywords_top)
    else:
        assert_forbidden(r)

    assert_db_state(keywords_flat)
    assert_no_audit_log()


@pytest.mark.require_scope(ODPScope.KEYWORD_READ)
@pytest.mark.parametrize('recurse', [False, True])
def test_list_keywords(
        api,
        scopes,
        keyword_batch,
        recurse,
):
    keywords_top, keywords_flat = keyword_batch
    authorized = ODPScope.KEYWORD_READ in scopes
    client = api(scopes)

    for n in range(4):
        for kw in reversed(keywords_flat):
            if kw.key.count('/') == n:
                r = client.get(f'/keyword/{kw.key}/?recurse={recurse}')
                if authorized:
                    assert_json_results(r, r.json(), kw.children, recurse)
                else:
                    assert_forbidden(r)
                break

    assert_db_state(keywords_flat)
    assert_no_audit_log()


@pytest.mark.require_scope(ODPScope.KEYWORD_READ)
@pytest.mark.parametrize('recurse', [False, True])
def test_list_keywords_not_found(
        api,
        scopes,
        keyword_batch,
        recurse,
):
    keywords_top, keywords_flat = keyword_batch
    authorized = ODPScope.KEYWORD_READ in scopes
    client = api(scopes)

    r = client.get(f'/keyword/foo/?recurse={recurse}')
    if authorized:
        assert_not_found(r, "Parent keyword 'foo' does not exist")
    else:
        assert_forbidden(r)

    for n in range(2):
        for kw in reversed(keywords_flat):
            if kw.key.count('/') == n:
                r = client.get(f'/keyword/{kw.key}/foo/?recurse={recurse}')
                if authorized:
                    assert_not_found(r, f"Parent keyword '{kw.key}/foo' does not exist")
                else:
                    assert_forbidden(r)
                break

    assert_db_state(keywords_flat)
    assert_no_audit_log()


@pytest.mark.require_scope(ODPScope.KEYWORD_READ)
@pytest.mark.parametrize('recurse', [False, True])
def test_get_keyword(
        api,
        scopes,
        keyword_batch,
        recurse,
):
    keywords_top, keywords_flat = keyword_batch
    authorized = ODPScope.KEYWORD_READ in scopes
    client = api(scopes)

    for n in range(4):
        for kw in reversed(keywords_flat):
            if kw.key.count('/') == n:
                r = client.get(f'/keyword/{kw.key}?recurse={recurse}')
                if authorized:
                    assert_json_result(r, r.json(), kw, recurse)
                else:
                    assert_forbidden(r)
                break

    assert_db_state(keywords_flat)
    assert_no_audit_log()


@pytest.mark.require_scope(ODPScope.KEYWORD_READ)
@pytest.mark.parametrize('recurse', [False, True])
def test_get_keyword_not_found(
        api,
        scopes,
        keyword_batch,
        recurse,
):
    keywords_top, keywords_flat = keyword_batch
    authorized = ODPScope.KEYWORD_READ in scopes
    client = api(scopes)

    r = client.get(f'/keyword/foo?recurse={recurse}')
    if authorized:
        assert_not_found(r, "Keyword 'foo' does not exist")
    else:
        assert_forbidden(r)

    for n in range(2):
        for kw in reversed(keywords_flat):
            if kw.key.count('/') == n:
                r = client.get(f'/keyword/{kw.key}/foo?recurse={recurse}')
                if authorized:
                    assert_not_found(r, f"Keyword '{kw.key}/foo' does not exist")
                else:
                    assert_forbidden(r)
                break

    assert_db_state(keywords_flat)
    assert_no_audit_log()


@pytest.mark.require_scope(ODPScope.KEYWORD_SUGGEST)
def test_suggest_keyword(
        api,
        scopes,
        keyword_batch,
):
    keywords_top, keywords_flat = keyword_batch
    authorized = ODPScope.KEYWORD_SUGGEST in scopes
    client = api(scopes)

    keyword_1 = keyword_build(
        parent_key=keywords_top[2].key,
        data={'abbr': fake.word(), 'title': fake.company()},
        status='proposed',
        child_schema=None,
    )
    r = client.post(f'/keyword/{keyword_1.key}', json=dict(
        data=keyword_1.data,
    ))

    if authorized:
        assert_json_result(r, r.json(), keyword_1)

        keyword_2 = keyword_build(
            parent_key=keyword_1.key,
            data={'abbr': fake.word(), 'title': fake.company()},
            status='proposed',
            child_schema=None,
        )
        r = client.post(f'/keyword/{keyword_2.key}', json=dict(
            data=keyword_2.data,
        ))
        assert_json_result(r, r.json(), keyword_2)

        assert_db_state(keywords_flat + [keyword_1, keyword_2])
        assert_audit_log(
            api.grant_type,
            dict(command='insert', keyword=keyword_1),
            dict(command='insert', keyword=keyword_2),
        )

    else:
        assert_forbidden(r)
        assert_db_state(keywords_flat)
        assert_no_audit_log()


@pytest.mark.require_scope(ODPScope.KEYWORD_SUGGEST)
def test_suggest_keyword_parent_not_found(
        api,
        scopes,
        keyword_batch,
):
    keywords_top, keywords_flat = keyword_batch
    authorized = ODPScope.KEYWORD_SUGGEST in scopes
    client = api(scopes)

    r = client.post('/keyword/foo/bar', json=dict(
        data={'abbr': fake.word(), 'title': fake.company()},
    ))
    if authorized:
        assert_not_found(r, "Parent keyword 'foo' does not exist")
    else:
        assert_forbidden(r)

    r = client.post(f'/keyword/{(key := keywords_top[2].key)}/foo/bar', json=dict(
        data={'abbr': fake.word(), 'title': fake.company()},
    ))
    if authorized:
        assert_not_found(r, f"Parent keyword '{key}/foo' does not exist")
    else:
        assert_forbidden(r)

    assert_db_state(keywords_flat)
    assert_no_audit_log()


@pytest.mark.require_scope(ODPScope.KEYWORD_SUGGEST)
def test_suggest_keyword_no_parent(
        api,
        scopes,
        keyword_batch,
):
    keywords_top, keywords_flat = keyword_batch
    authorized = ODPScope.KEYWORD_SUGGEST in scopes

    r = api(scopes).post('/keyword/foo', json=dict(
        data={'abbr': fake.word(), 'title': fake.company()},
    ))
    if authorized:
        assert_unprocessable(r, "key must be suffixed to a parent key")
    else:
        assert_forbidden(r)

    assert_db_state(keywords_flat)
    assert_no_audit_log()


@pytest.mark.require_scope(ODPScope.KEYWORD_SUGGEST)
def test_suggest_keyword_conflict(
        api,
        scopes,
        keyword_batch,
):
    keywords_top, keywords_flat = keyword_batch
    authorized = ODPScope.KEYWORD_SUGGEST in scopes
    client = api(scopes)

    for n in range(1, 4):
        for kw in reversed(keywords_flat):
            if kw.key.count('/') == n:
                r = client.post(f'/keyword/{kw.key}', json=dict(
                    data={'abbr': fake.word(), 'title': fake.company()},
                ))
                if authorized:
                    assert_conflict(r, f"Keyword '{kw.key}' already exists")
                else:
                    assert_forbidden(r)
                break

    assert_db_state(keywords_flat)
    assert_no_audit_log()


@pytest.mark.require_scope(ODPScope.KEYWORD_SUGGEST)
def test_suggest_keyword_invalid_data(
        api,
        scopes,
        keyword_batch,
):
    keywords_top, keywords_flat = keyword_batch
    authorized = ODPScope.KEYWORD_SUGGEST in scopes

    r = api(scopes).post(f'/keyword/{keywords_top[2].key}/foo', json=dict(
        data={'title': fake.company()},  # missing required property 'abbr'
    ))
    if authorized:
        assert_unprocessable(r, valid=False)
    else:
        assert_forbidden(r)

    assert_db_state(keywords_flat)
    assert_no_audit_log()


@pytest.mark.require_scope(ODPScope.KEYWORD_ADMIN)
def test_set_keyword(
        api,
        scopes,
        keyword_batch,
):
    keywords_top, keywords_flat = keyword_batch
    authorized = ODPScope.KEYWORD_ADMIN in scopes
    client = api(scopes)

    keywords_flat.reverse()
    audit = []
    for n in range(4):
        create = n == 0 or randint(0, 1)
        for i, kw in enumerate(keywords_flat):
            if kw.key.count('/') == n:
                data = {'abbr': fake.word(), 'title': fake.company()}
                child_schema = FactorySession.execute(select(Schema)).scalars().first() if randint(0, 1) else None

                if create:
                    kw_in = keyword_build(
                        parent_key=kw.key,
                        data=data,
                        child_schema=child_schema,
                    )
                    audit += [dict(command='insert', keyword=kw_in)]
                else:
                    kw_in = keyword_build(
                        parent_key=kw.parent_key,
                        key=kw.key,
                        data=data,
                        child_schema=child_schema,
                    )
                    audit += [dict(command='update', keyword=kw_in, replace=i)]

                r = client.put(f'/keyword/{kw_in.key}', json=dict(
                    data=kw_in.data,
                    status=kw_in.status,
                    child_schema_id=kw_in.child_schema_id,
                ))

                if authorized:
                    assert_json_result(r, r.json(), kw_in)
                else:
                    assert_forbidden(r)
                break

    if authorized:
        replace_indices = [entry.pop('replace', None) for entry in audit]
        for replace_index in reversed(sorted(filter(lambda i: i is not None, replace_indices))):
            keywords_flat.pop(replace_index)
        assert_db_state(keywords_flat + [entry.get('keyword') for entry in audit])
        assert_audit_log(api.grant_type, *audit)

    else:
        assert_db_state(keywords_flat)
        assert_no_audit_log()


@pytest.mark.require_scope(ODPScope.KEYWORD_ADMIN)
def test_set_keyword_parent_not_found(
        api,
        scopes,
        keyword_batch,
):
    keywords_top, keywords_flat = keyword_batch
    authorized = ODPScope.KEYWORD_ADMIN in scopes
    client = api(scopes)

    r = client.put('/keyword/foo/bar', json=dict(
        data={'abbr': fake.word(), 'title': fake.company()},
    ))
    if authorized:
        assert_not_found(r, "Parent keyword 'foo' does not exist")
    else:
        assert_forbidden(r)

    r = client.put(f'/keyword/{(key := keywords_top[2].key)}/foo/bar', json=dict(
        data={'abbr': fake.word(), 'title': fake.company()},
    ))
    if authorized:
        assert_not_found(r, f"Parent keyword '{key}/foo' does not exist")
    else:
        assert_forbidden(r)

    assert_db_state(keywords_flat)
    assert_no_audit_log()


@pytest.mark.require_scope(ODPScope.KEYWORD_ADMIN)
def test_set_keyword_no_parent(
        api,
        scopes,
        keyword_batch,
):
    keywords_top, keywords_flat = keyword_batch
    authorized = ODPScope.KEYWORD_ADMIN in scopes

    r = api(scopes).put('/keyword/foo', json=dict(
        data={'abbr': fake.word(), 'title': fake.company()},
    ))
    if authorized:
        assert_unprocessable(r, "key must be suffixed to a parent key")
    else:
        assert_forbidden(r)

    assert_db_state(keywords_flat)
    assert_no_audit_log()


@pytest.mark.require_scope(ODPScope.KEYWORD_ADMIN)
def test_set_keyword_invalid_data(
        api,
        scopes,
        keyword_batch,
):
    keywords_top, keywords_flat = keyword_batch
    authorized = ODPScope.KEYWORD_ADMIN in scopes

    r = api(scopes).put(f'/keyword/{keywords_top[2].key}/foo', json=dict(
        data={'abbr': fake.word()},  # missing required property 'title'
    ))
    if authorized:
        assert_unprocessable(r, valid=False)
    else:
        assert_forbidden(r)

    assert_db_state(keywords_flat)
    assert_no_audit_log()


@pytest.mark.require_scope(ODPScope.KEYWORD_ADMIN)
def test_delete_keyword(
        api,
        scopes,
        keyword_batch,
):
    keywords_top, keywords_flat = keyword_batch
    authorized = ODPScope.KEYWORD_ADMIN in scopes
    client = api(scopes)

    keywords_flat.reverse()
    audit = []
    for n in range(4):
        for i, kw in enumerate(keywords_flat):
            if kw.key.count('/') == n:
                if can_delete := not kw.children:
                    audit += [dict(command='delete', keyword=kw, index=i)]

                r = client.delete(f'/keyword/{kw.key}')

                if authorized:
                    if can_delete:
                        assert_empty_result(r)
                    else:
                        assert_unprocessable(r, f"Keyword '{kw.key}' cannot be deleted as it has sub-keywords")
                else:
                    assert_forbidden(r)
                break

    if authorized:
        delete_indices = [entry.pop('index') for entry in audit]
        for delete_index in reversed(sorted(delete_indices)):
            keywords_flat.pop(delete_index)
        assert_db_state(keywords_flat)
        assert_audit_log(api.grant_type, *audit)
    else:
        assert_db_state(keywords_flat)
        assert_no_audit_log()


@pytest.mark.require_scope(ODPScope.KEYWORD_ADMIN)
def test_delete_keyword_not_found(
        api,
        scopes,
        keyword_batch,
):
    keywords_top, keywords_flat = keyword_batch
    authorized = ODPScope.KEYWORD_ADMIN in scopes
    client = api(scopes)

    for n in range(2):
        for kw in reversed(keywords_flat):
            if kw.key.count('/') == n:
                r = client.delete(f'/keyword/{kw.key}/foo')
                if authorized:
                    assert_not_found(r, f"Keyword '{kw.key}/foo' does not exist")
                else:
                    assert_forbidden(r)
                break

    assert_db_state(keywords_flat)
    assert_no_audit_log()
