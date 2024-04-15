from datetime import datetime
from random import randint

import pytest
from sqlalchemy import select

from odp.const import ODPScope
from odp.db.models import Archive, ArchiveResource, Resource
from test import TestSession
from test.api import assert_conflict, assert_forbidden, assert_new_timestamp, assert_not_found
from test.factories import ArchiveFactory, ArchiveResourceFactory, ProviderFactory, ResourceFactory, fake


@pytest.fixture
def resource_batch():
    """Create and commit a batch of Archive, Resource and
    ArchiveResource instances."""
    archives = ArchiveFactory.create_batch(3)
    resources = ResourceFactory.create_batch(randint(3, 5))

    # optionally put each resource in one or more of the archives
    for resource in resources:
        resource.archive_urls = {}
        for archive in archives:
            if randint(0, 1):
                ar = ArchiveResourceFactory(
                    archive=archive,
                    resource=resource,
                )
                resource.archive_urls[archive.id] = archive.url + ar.path

    return resources


def resource_build(archive=None, archive_path=None):
    """Build and return an uncommitted Resource instance.
    Referenced archive and provider are however committed."""
    archive = archive or ArchiveFactory()
    resource = ResourceFactory.build(
        provider=(provider := ProviderFactory()),
        provider_id=provider.id,
    )
    path = archive_path or f'{fake.uri_path(deep=randint(1, 5))}/{resource.filename}'
    resource.archive_urls = {
        archive.id: archive.url + path
    }
    return resource


def assert_db_state(resources):
    """Verify that the DB resource table contains the given resource batch."""
    result = TestSession.execute(select(Resource)).scalars().all()
    result.sort(key=lambda r: r.id)
    resources.sort(key=lambda r: r.id)
    assert len(result) == len(resources)
    for n, row in enumerate(result):
        assert row.id == resources[n].id
        assert row.title == resources[n].title
        assert row.description == resources[n].description
        assert row.filename == resources[n].filename
        assert row.mimetype == resources[n].mimetype
        assert row.size == resources[n].size
        assert row.md5 == resources[n].md5
        assert row.provider_id == resources[n].provider_id
        assert_new_timestamp(row.timestamp)


def assert_db_ar_state(resources):
    """Verify that the archive_resource table is consistent with the
    archive urls per the resource batch."""
    result = TestSession.execute(select(ArchiveResource)).scalars().all()
    result.sort(key=lambda ar: (ar.archive_id, ar.resource_id))

    archive_resources = []
    for resource in resources:
        for archive_id, resource_url in resource.archive_urls.items():
            archive_resources += [ArchiveResourceFactory.stub(
                archive_id=archive_id,
                resource_id=resource.id,
                path=resource_url.removeprefix(TestSession.get(Archive, archive_id).url),
            )]
    archive_resources.sort(key=lambda ar: (ar.archive_id, ar.resource_id))

    assert len(result) == len(archive_resources)
    for n, row in enumerate(result):
        assert row.archive_id == archive_resources[n].archive_id
        assert row.resource_id == archive_resources[n].resource_id
        assert row.path == archive_resources[n].path
        assert_new_timestamp(row.timestamp)


def assert_json_result(response, json, resource):
    """Verify that the API result matches the given resource object."""
    assert response.status_code == 200
    assert json['id'] == resource.id
    assert json['title'] == resource.title
    assert json['description'] == resource.description
    assert json['filename'] == resource.filename
    assert json['mimetype'] == resource.mimetype
    assert json['size'] == resource.size
    assert json['md5'] == resource.md5
    assert json['provider_id'] == resource.provider_id
    assert json['provider_key'] == resource.provider.key
    assert json['archive_urls'] == resource.archive_urls
    assert_new_timestamp(datetime.fromisoformat(json['timestamp']))


def assert_json_results(response, json, resources):
    """Verify that the API result list matches the given resource batch."""
    items = json['items']
    assert json['total'] == len(items) == len(resources)
    items.sort(key=lambda i: i['id'])
    resources.sort(key=lambda r: r.id)
    for n, resource in enumerate(resources):
        assert_json_result(response, items[n], resource)


@pytest.mark.require_scope(ODPScope.RESOURCE_READ)
def test_list_resources(api, resource_batch, scopes):
    # todo: test the various list parameterizations
    authorized = ODPScope.RESOURCE_READ in scopes
    r = api(scopes).get('/resource/')
    if authorized:
        assert_json_results(r, r.json(), resource_batch)
    else:
        assert_forbidden(r)
    assert_db_state(resource_batch)
    assert_db_ar_state(resource_batch)


@pytest.mark.require_scope(ODPScope.RESOURCE_READ)
def test_get_resource(api, resource_batch, scopes):
    authorized = ODPScope.RESOURCE_READ in scopes
    r = api(scopes).get(f'/resource/{resource_batch[2].id}')
    if authorized:
        assert_json_result(r, r.json(), resource_batch[2])
    else:
        assert_forbidden(r)
    assert_db_state(resource_batch)
    assert_db_ar_state(resource_batch)


def test_get_resource_not_found(api, resource_batch):
    scopes = [ODPScope.RESOURCE_READ]
    r = api(scopes).get('/resource/foo')
    assert_not_found(r)
    assert_db_state(resource_batch)
    assert_db_ar_state(resource_batch)


@pytest.mark.require_scope(ODPScope.RESOURCE_WRITE)
def test_create_resource(api, resource_batch, scopes):
    authorized = ODPScope.RESOURCE_WRITE in scopes
    resource = resource_build()
    archive = TestSession.get(Archive, list(resource.archive_urls)[0])

    r = api(scopes).post('/resource/', json=dict(
        title=resource.title,
        description=resource.description,
        filename=resource.filename,
        mimetype=resource.mimetype,
        size=resource.size,
        md5=resource.md5,
        provider_id=resource.provider_id,
        archive_id=archive.id,
        archive_path=resource.archive_urls[archive.id].removeprefix(archive.url),
    ))
    if authorized:
        resource.id = r.json().get('id')
        assert_json_result(r, r.json(), resource)
        assert_db_state(resource_batch + [resource])
        assert_db_ar_state(resource_batch + [resource])
    else:
        assert_forbidden(r)
        assert_db_state(resource_batch)
        assert_db_ar_state(resource_batch)


def test_create_resource_conflict(api, resource_batch):
    scopes = [ODPScope.RESOURCE_WRITE]
    ar = ArchiveResourceFactory(archive=ArchiveFactory())
    resource = resource_build(archive=ar.archive, archive_path=ar.path)

    r = api(scopes).post('/resource/', json=dict(
        title=resource.title,
        description=resource.description,
        filename=resource.filename,
        mimetype=resource.mimetype,
        size=resource.size,
        md5=resource.md5,
        provider_id=resource.provider_id,
        archive_id=ar.archive_id,
        archive_path=ar.path,
    ))

    # set expected archive_urls here; somehow ar.resource is reloaded by above
    ar.resource.archive_urls = {ar.archive_id: ar.archive.url + ar.path}

    assert_conflict(r, f'path {ar.path} already exists in archive {ar.archive_id}')
    assert_db_state(resource_batch + [ar.resource])
    assert_db_ar_state(resource_batch + [ar.resource])
