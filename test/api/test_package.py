from datetime import datetime
from random import randint

import pytest
from sqlalchemy import select

from odp.const import ODPScope
from odp.db.models import Package, PackageResource
from test import TestSession
from test.api import assert_empty_result, assert_forbidden, assert_new_timestamp, assert_not_found
from test.factories import PackageFactory, ProviderFactory, ResourceFactory


@pytest.fixture
def package_batch():
    """Create and commit a batch of Package instances, with
    associated resources."""
    packages = []
    for _ in range(randint(3, 5)):
        packages += [package := PackageFactory(
            resources=(resources := ResourceFactory.create_batch(randint(0, 4))),
        )]
        package.resource_ids = [resource.id for resource in resources]

    return packages


def package_build(**id):
    """Build and return an uncommitted Package instance.
    Referenced provider and resources are however committed."""
    package = PackageFactory.build(
        **id,
        provider=(provider := ProviderFactory()),
        provider_id=provider.id,
        resources=(resources := ResourceFactory.create_batch(randint(0, 4))),
    )
    package.resource_ids = [resource.id for resource in resources]
    return package


def assert_db_state(packages):
    """Verify that the DB package table contains the given package batch."""
    result = TestSession.execute(select(Package)).scalars().all()
    result.sort(key=lambda p: p.id)
    packages.sort(key=lambda p: p.id)
    assert len(result) == len(packages)
    for n, row in enumerate(result):
        assert row.id == packages[n].id
        assert row.metadata_ == packages[n].metadata_
        assert row.notes == packages[n].notes
        assert_new_timestamp(row.timestamp)
        assert row.provider_id == packages[n].provider_id
        assert row.schema_id == packages[n].schema_id
        assert row.schema_type == packages[n].schema_type == 'metadata'


def assert_db_pr_state(packages):
    """Verify that the package_resource table is consistent with the
    resource ids for the batched packages."""
    result = TestSession.execute(select(PackageResource.package_id, PackageResource.resource_id)).all()
    result.sort(key=lambda pr: (pr.package_id, pr.resource_id))
    package_resources = []
    for package in packages:
        for resource_id in package.resource_ids:
            package_resources += [(package.id, resource_id)]
    package_resources.sort()
    assert result == package_resources


def assert_json_result(response, json, package):
    """Verify that the API result matches the given package object."""
    # todo: check linked record
    assert response.status_code == 200
    assert json['id'] == package.id
    assert json['provider_id'] == package.provider_id
    assert json['provider_key'] == package.provider.key
    assert json['schema_id'] == package.schema_id
    assert json['metadata'] == package.metadata_
    assert json['notes'] == package.notes
    assert_new_timestamp(datetime.fromisoformat(json['timestamp']))
    assert sorted(json['resource_ids']) == sorted(package.resource_ids)


def assert_json_results(response, json, packages):
    """Verify that the API result list matches the given package batch."""
    items = json['items']
    assert json['total'] == len(items) == len(packages)
    items.sort(key=lambda i: i['id'])
    packages.sort(key=lambda p: p.id)
    for n, package in enumerate(packages):
        assert_json_result(response, items[n], package)


@pytest.mark.require_scope(ODPScope.PACKAGE_READ)
def test_list_packages(api, package_batch, scopes):
    authorized = ODPScope.PACKAGE_READ in scopes
    r = api(scopes).get('/package/')
    if authorized:
        assert_json_results(r, r.json(), package_batch)
    else:
        assert_forbidden(r)
    assert_db_state(package_batch)
    assert_db_pr_state(package_batch)


@pytest.mark.require_scope(ODPScope.PACKAGE_READ)
def test_get_package(api, package_batch, scopes):
    authorized = ODPScope.PACKAGE_READ in scopes
    r = api(scopes).get(f'/package/{package_batch[2].id}')
    if authorized:
        assert_json_result(r, r.json(), package_batch[2])
    else:
        assert_forbidden(r)
    assert_db_state(package_batch)
    assert_db_pr_state(package_batch)


def test_get_package_not_found(api, package_batch):
    scopes = [ODPScope.PACKAGE_READ]
    r = api(scopes).get('/package/foo')
    assert_not_found(r)
    assert_db_state(package_batch)
    assert_db_pr_state(package_batch)


@pytest.mark.require_scope(ODPScope.PACKAGE_WRITE)
def test_create_package(api, package_batch, scopes):
    authorized = ODPScope.PACKAGE_WRITE in scopes
    package = package_build()

    r = api(scopes).post('/package/', json=dict(
        provider_id=package.provider_id,
        schema_id=package.schema_id,
        metadata=package.metadata_,
        notes=package.notes,
        resource_ids=package.resource_ids,
    ))

    if authorized:
        package.id = r.json().get('id')
        assert_json_result(r, r.json(), package)
        assert_db_state(package_batch + [package])
        assert_db_pr_state(package_batch + [package])
    else:
        assert_forbidden(r)
        assert_db_state(package_batch)
        assert_db_pr_state(package_batch)


@pytest.mark.require_scope(ODPScope.PACKAGE_WRITE)
def test_update_package(api, package_batch, scopes):
    authorized = ODPScope.PACKAGE_WRITE in scopes
    package = package_build(id=package_batch[2].id)

    r = api(scopes).put(f'/package/{package.id}', json=dict(
        provider_id=package.provider_id,
        schema_id=package.schema_id,
        metadata=package.metadata_,
        notes=package.notes,
        resource_ids=package.resource_ids,
    ))

    if authorized:
        assert_json_result(r, r.json(), package)
        assert_db_state(package_batch[:2] + [package] + package_batch[3:])
        assert_db_pr_state(package_batch[:2] + [package] + package_batch[3:])
    else:
        assert_forbidden(r)
        assert_db_state(package_batch)
        assert_db_pr_state(package_batch)


def test_update_package_not_found(api, package_batch):
    scopes = [ODPScope.PACKAGE_WRITE]
    package = package_build(id='foo')

    r = api(scopes).put(f'/package/{package.id}', json=dict(
        provider_id=package.provider_id,
        schema_id=package.schema_id,
        metadata=package.metadata_,
        notes=package.notes,
        resource_ids=package.resource_ids,
    ))

    assert_not_found(r)
    assert_db_state(package_batch)
    assert_db_pr_state(package_batch)


@pytest.mark.require_scope(ODPScope.PACKAGE_WRITE)
def test_delete_package(api, package_batch, scopes):
    authorized = ODPScope.PACKAGE_WRITE in scopes
    r = api(scopes).delete(f'/package/{package_batch[2].id}')
    if authorized:
        assert_empty_result(r)
        assert_db_state(package_batch[:2] + package_batch[3:])
        assert_db_pr_state(package_batch[:2] + package_batch[3:])
    else:
        assert_forbidden(r)
        assert_db_state(package_batch)
        assert_db_pr_state(package_batch)


def test_delete_package_not_found(api, package_batch):
    scopes = [ODPScope.PACKAGE_WRITE]
    r = api(scopes).delete('/package/foo')
    assert_not_found(r)
    assert_db_state(package_batch)
    assert_db_pr_state(package_batch)
