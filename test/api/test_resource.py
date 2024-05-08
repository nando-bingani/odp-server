from datetime import datetime
from random import randint

import pytest
from sqlalchemy import select

from odp.const import ODPScope
from odp.db.models import Archive, ArchiveResource, Resource
from test import TestSession
from test.api import assert_conflict, assert_forbidden, assert_new_timestamp, assert_not_found
from test.api.conftest import try_skip_user_provider_constraint
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


def resource_build(archive=None, archive_path=None, provider=None):
    """Build and return an uncommitted Resource instance.
    Referenced archive and provider are however committed."""
    archive = archive or ArchiveFactory()
    resource = ResourceFactory.build(
        provider=(provider := provider or ProviderFactory()),
        provider_id=provider.id,
    )
    path = archive_path or f'{fake.uri_path(deep=randint(1, 5))}/{resource.filename}'
    resource.archive_urls = {
        archive.id: archive.url + path
    }
    return resource


def assert_db_state(resources):
    """Verify that the resource table contains the given resource batch,
    and that the archive_resource table contains the generated archive paths."""
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


def parameterize_api_fixture(
        resources,
        grant_type,
        client_provider_constraint,
        user_provider_constraint,
        force_mismatch=False,
):
    """Return tuple(client_provider, user_providers) for parameterizing
    the api fixture, based on constraint params and generated resource batch.

    Set force_mismatch=True for the list test; this creates a new provider
    for the mismatch cases. For all the other tests we can reuse any existing
    providers other than the #2 resource's provider for the mismatches.
    """
    try_skip_user_provider_constraint(grant_type, user_provider_constraint)

    if client_provider_constraint == 'client_provider_any':
        client_provider = None
    elif client_provider_constraint == 'client_provider_match':
        client_provider = resources[2].provider
    elif client_provider_constraint == 'client_provider_mismatch':
        client_provider = ProviderFactory() if force_mismatch else resources[0].provider

    if user_provider_constraint == 'user_provider_none':
        user_providers = None
    elif user_provider_constraint == 'user_provider_match':
        user_providers = [p.provider for p in resources[1:3]]
    elif user_provider_constraint == 'user_provider_mismatch':
        user_providers = [ProviderFactory()] if force_mismatch else [p.provider for p in resources[0:2]]

    return dict(client_provider=client_provider, user_providers=user_providers)


@pytest.mark.require_scope(ODPScope.RESOURCE_READ)
def test_list_resources(
        api,
        scopes,
        resource_batch,
        client_provider_constraint,
        user_provider_constraint,
):
    api_kwargs = parameterize_api_fixture(
        resource_batch,
        api.grant_type,
        client_provider_constraint,
        user_provider_constraint,
        force_mismatch=True,
    )
    authorized = ODPScope.RESOURCE_READ in scopes

    if client_provider_constraint == 'client_provider_any':
        expected_result_batch = resource_batch
    elif client_provider_constraint == 'client_provider_match':
        expected_result_batch = [resource_batch[2]]
    elif client_provider_constraint == 'client_provider_mismatch':
        expected_result_batch = []

    if api.grant_type == 'authorization_code':
        if user_provider_constraint == 'user_provider_match':
            expected_result_batch = list(set(resource_batch[1:3]).intersection(expected_result_batch))
        else:
            expected_result_batch = []

    # todo: test the various list parameterizations
    r = api(scopes, **api_kwargs).get('/resource/')

    if authorized:
        assert_json_results(r, r.json(), expected_result_batch)
    else:
        assert_forbidden(r)

    assert_db_state(resource_batch)


@pytest.mark.require_scope(ODPScope.RESOURCE_READ)
def test_get_resource(
        api,
        scopes,
        resource_batch,
        client_provider_constraint,
        user_provider_constraint,
):
    api_kwargs = parameterize_api_fixture(
        resource_batch,
        api.grant_type,
        client_provider_constraint,
        user_provider_constraint,
    )
    authorized = (
            ODPScope.RESOURCE_READ in scopes and
            client_provider_constraint in ('client_provider_any', 'client_provider_match') and
            (api.grant_type == 'client_credentials' or user_provider_constraint == 'user_provider_match')
    )

    r = api(scopes, **api_kwargs).get(f'/resource/{resource_batch[2].id}')

    if authorized:
        assert_json_result(r, r.json(), resource_batch[2])
    else:
        assert_forbidden(r)

    assert_db_state(resource_batch)


def test_get_resource_not_found(
        api,
        resource_batch,
        client_provider_constraint,
        user_provider_constraint,
):
    scopes = [ODPScope.RESOURCE_READ]
    api_kwargs = parameterize_api_fixture(
        resource_batch,
        api.grant_type,
        client_provider_constraint,
        user_provider_constraint,
    )
    r = api(scopes, **api_kwargs).get('/resource/foo')
    assert_not_found(r)
    assert_db_state(resource_batch)


@pytest.mark.require_scope(ODPScope.RESOURCE_WRITE)
def test_create_resource(
        api,
        scopes,
        resource_batch,
        client_provider_constraint,
        user_provider_constraint,
):
    api_kwargs = parameterize_api_fixture(
        resource_batch,
        api.grant_type,
        client_provider_constraint,
        user_provider_constraint,
    )
    authorized = (
            ODPScope.RESOURCE_WRITE in scopes and
            client_provider_constraint in ('client_provider_any', 'client_provider_match') and
            (api.grant_type == 'client_credentials' or user_provider_constraint == 'user_provider_match')
    )

    resource = resource_build(provider=resource_batch[2].provider)
    archive = TestSession.get(Archive, list(resource.archive_urls)[0])

    r = api(scopes, **api_kwargs).post('/resource/', json=dict(
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
    else:
        assert_forbidden(r)
        assert_db_state(resource_batch)


def test_create_resource_conflict(
        api,
        resource_batch,
        client_provider_constraint,
        user_provider_constraint,
):
    scopes = [ODPScope.RESOURCE_WRITE]
    api_kwargs = parameterize_api_fixture(
        resource_batch,
        api.grant_type,
        client_provider_constraint,
        user_provider_constraint,
    )
    authorized = (
            client_provider_constraint in ('client_provider_any', 'client_provider_match') and
            (api.grant_type == 'client_credentials' or user_provider_constraint == 'user_provider_match')
    )

    ar = ArchiveResourceFactory(archive=ArchiveFactory())
    resource = resource_build(archive=ar.archive, archive_path=ar.path, provider=resource_batch[2].provider)

    r = api(scopes, **api_kwargs).post('/resource/', json=dict(
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

    if authorized:
        assert_conflict(r, f'path {ar.path} already exists in archive {ar.archive_id}')
    else:
        assert_forbidden(r)

    assert_db_state(resource_batch + [ar.resource])
