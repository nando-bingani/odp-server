from random import randint

import pytest
from sqlalchemy import select

from odp.const import ODPScope
from odp.db.models import Archive
from test import TestSession
from test.api.assertions import assert_forbidden, assert_not_found
from test.factories import ArchiveFactory, ArchiveResourceFactory


@pytest.fixture
def archive_batch():
    """Create and commit a batch of Archive instances."""
    archives = [
        ArchiveFactory()
        for _ in range(randint(3, 5))
    ]
    for archive in archives:
        archive.resource_count = randint(0, 3)
        ArchiveResourceFactory.create_batch(archive.resource_count, archive=archive)
    return archives


def assert_db_state(archives):
    """Verify that the DB archive table contains the given archive batch."""
    result = TestSession.execute(select(Archive)).scalars().all()
    result.sort(key=lambda a: a.id)
    archives.sort(key=lambda a: a.id)
    assert len(result) == len(archives)
    for n, row in enumerate(result):
        assert row.id == archives[n].id
        assert row.type == archives[n].type
        assert row.download_url == archives[n].download_url
        assert row.upload_url == archives[n].upload_url
        assert row.scope_id == archives[n].scope_id
        assert row.scope_type == archives[n].scope_type == 'odp'


def assert_json_result(response, json, archive):
    """Verify that the API result matches the given archive object."""
    assert response.status_code == 200
    assert json['id'] == archive.id
    assert json['type'] == archive.type
    assert json['download_url'] == archive.download_url
    assert json['upload_url'] == archive.upload_url
    assert json['scope_id'] == archive.scope_id
    assert json['resource_count'] == archive.resource_count


def assert_json_results(response, json, archives):
    """Verify that the API result list matches the given archive batch."""
    items = json['items']
    assert json['total'] == len(items) == len(archives)
    items.sort(key=lambda i: i['id'])
    archives.sort(key=lambda a: a.id)
    for n, archive in enumerate(archives):
        assert_json_result(response, items[n], archive)


@pytest.mark.require_scope(ODPScope.ARCHIVE_READ)
def test_list_archives(api, archive_batch, scopes):
    authorized = ODPScope.ARCHIVE_READ in scopes
    r = api(scopes).get('/archive/')
    if authorized:
        assert_json_results(r, r.json(), archive_batch)
    else:
        assert_forbidden(r)
    assert_db_state(archive_batch)


@pytest.mark.require_scope(ODPScope.ARCHIVE_READ)
def test_get_archive(api, archive_batch, scopes):
    authorized = ODPScope.ARCHIVE_READ in scopes
    r = api(scopes).get(f'/archive/{archive_batch[2].id}')
    if authorized:
        assert_json_result(r, r.json(), archive_batch[2])
    else:
        assert_forbidden(r)
    assert_db_state(archive_batch)


def test_get_archive_not_found(api, archive_batch):
    scopes = [ODPScope.ARCHIVE_READ]
    r = api(scopes).get('/archive/foo')
    assert_not_found(r)
    assert_db_state(archive_batch)
