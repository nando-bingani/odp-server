import os
from datetime import datetime
from random import randint

import pytest
from sqlalchemy import select

import migrate.systemdata
from odp.catalog.mims import MIMSCatalog
from odp.catalog.saeon import SAEONCatalog
from odp.const import ODPScope
from odp.db import Session
from odp.db.models import Catalog, Tag
from odp.lib.cache import Cache
from test import datacite4_example, iso19115_example
from test.api import all_scopes, all_scopes_excluding, assert_forbidden, assert_new_timestamp, assert_not_found, assert_redirect, assert_unprocessable
from test.factories import CatalogFactory, CollectionTagFactory, RecordFactory, RecordTagFactory, fake


@pytest.fixture
def catalog_batch():
    """Create and commit a batch of Catalog instances."""
    return [CatalogFactory() for _ in range(randint(3, 5))]


@pytest.fixture(params=[False, True])
def catalog_exists(request):
    return request.param


@pytest.fixture(params=['uuid', 'doi', 'invalid'])
def record_id_format(request):
    return request.param


@pytest.fixture(params=[None, True])
def tag_collection_published(request):
    return request.param


@pytest.fixture(params=[None, 'MIMS', 'FOO'])
def tag_collection_infrastructure(request):
    return request.param


@pytest.fixture(params=[None, False, True])
def tag_record_qc(request):
    return request.param


@pytest.fixture(params=[None, True])
def tag_record_retracted(request):
    return request.param


@pytest.fixture
def static_publishing_data():
    os.environ['SAEON_CATALOG_URL'] = 'http://odp.catalog/saeon'
    os.environ['MIMS_CATALOG_URL'] = 'http://odp.catalog/mims'
    os.environ['DATACITE_CATALOG_URL'] = 'http://odp.catalog/datacite'
    migrate.systemdata.init_system_scopes()
    migrate.systemdata.init_schemas()
    migrate.systemdata.init_vocabularies()
    migrate.systemdata.init_tags()
    migrate.systemdata.init_catalogs()


@pytest.fixture(params=['SAEON', 'MIMS'])
def catalog_id(request):
    return request.param


@pytest.fixture
def published_record(
        tag_collection_published,
        tag_collection_infrastructure,
        tag_record_qc,
        tag_record_retracted,
):
    """Fixture which creates and returns a single record instance,
    with valid (example) metadata, optionally with collection and/or
    record tags, and evaluated for publishing."""
    record = RecordFactory(use_example_metadata=True)
    if tag_collection_published is not None:
        CollectionTagFactory.create(
            tag=Session.get(Tag, ('Collection.Published', 'collection')),
            collection=record.collection,
        )
    if tag_collection_infrastructure is not None:
        CollectionTagFactory.create(
            tag=Session.get(Tag, ('Collection.Infrastructure', 'collection')),
            collection=record.collection,
            data={'infrastructure': tag_collection_infrastructure}
        )
    if tag_record_qc is not None:
        RecordTagFactory.create(
            tag=Session.get(Tag, ('Record.QC', 'record')),
            record=record,
            data={'pass_': tag_record_qc}
        )
    if tag_record_retracted is not None:
        RecordTagFactory.create(
            tag=Session.get(Tag, ('Record.Retracted', 'record')),
            record=record,
        )

    catalog_classes = {
        'SAEON': SAEONCatalog,
        'MIMS': MIMSCatalog,
    }
    for catalog_id, catalog_cls in catalog_classes.items():
        catalog_cls(catalog_id, Cache(__name__)).publish()

    # save fixture params on record for later reference
    record.tag_collection_published = tag_collection_published
    record.tag_collection_infrastructure = tag_collection_infrastructure
    record.tag_record_qc = tag_record_qc
    record.tag_record_retracted = tag_record_retracted

    return record


def assert_db_state(catalogs):
    """Verify that the DB catalog table contains the given catalog batch."""
    Session.expire_all()
    result = Session.execute(select(Catalog)).scalars().all()
    assert set((row.id, row.url) for row in result) \
           == set((catalog.id, catalog.url) for catalog in catalogs)


def assert_json_result(response, json, catalog):
    """Verify that the API result matches the given catalog object."""
    assert response.status_code == 200
    assert json['id'] == catalog.id
    assert json['url'] == catalog.url


def assert_json_results(response, json, catalogs):
    """Verify that the API result list matches the given catalog batch."""
    items = json['items']
    assert json['total'] == len(items) == len(catalogs)
    items.sort(key=lambda i: i['id'])
    catalogs.sort(key=lambda c: c.id)
    for n, catalog in enumerate(catalogs):
        assert_json_result(response, items[n], catalog)


@pytest.mark.parametrize('scopes', [
    [ODPScope.CATALOG_READ],
    [],
    all_scopes,
    all_scopes_excluding(ODPScope.CATALOG_READ),
])
def test_list_catalogs(api, catalog_batch, scopes):
    authorized = ODPScope.CATALOG_READ in scopes
    r = api(scopes).get('/catalog/')
    if authorized:
        assert_json_results(r, r.json(), catalog_batch)
    else:
        assert_forbidden(r)
    assert_db_state(catalog_batch)


@pytest.mark.parametrize('scopes', [
    [ODPScope.CATALOG_READ],
    [],
    all_scopes,
    all_scopes_excluding(ODPScope.CATALOG_READ),
])
def test_get_catalog(api, catalog_batch, scopes):
    authorized = ODPScope.CATALOG_READ in scopes
    r = api(scopes).get(f'/catalog/{catalog_batch[2].id}')
    if authorized:
        assert_json_result(r, r.json(), catalog_batch[2])
    else:
        assert_forbidden(r)
    assert_db_state(catalog_batch)


def test_get_catalog_not_found(api, catalog_batch):
    scopes = [ODPScope.CATALOG_READ]
    r = api(scopes).get('/catalog/foo')
    assert_not_found(r)
    assert_db_state(catalog_batch)


def test_redirect_to(api, catalog_batch, catalog_exists, record_id_format):
    if catalog_exists:
        catalog = catalog_batch[2]
        catalog_id = catalog.id
    else:
        catalog_id = 'foo'

    if record_id_format == 'uuid':
        record_id = fake.uuid4()
    elif record_id_format == 'doi':
        record_id = '10.12345/' + fake.word()
    else:
        record_id = fake.word()

    r = api([]).get(f'/catalog/{catalog_id}/go/{record_id}', follow_redirects=False)

    if record_id_format in ('uuid', 'doi'):
        if catalog_exists:
            assert_redirect(r, f'{catalog.url}/{record_id}')
        else:
            assert_not_found(r)
    else:
        assert_unprocessable(r)


def test_list_records(api, static_publishing_data, published_record, catalog_id):
    def check_metadata_record(schema_id, deep=True):
        metadata_record = next(filter(
            lambda m: m['schema_id'] == schema_id, result['metadata_records']
        ))
        uri_id = schema_id.split('.')[1].lower()
        assert metadata_record['schema_uri'] == f'https://odp.saeon.ac.za/schema/metadata/saeon/{uri_id}'

        if deep:
            expected_metadata = datacite4_example() if schema_id == 'SAEON.DataCite4' else iso19115_example()
            if published_record.doi:
                expected_metadata |= dict(doi=published_record.doi)
            else:
                expected_metadata.pop('doi')
            assert metadata_record['metadata'] == expected_metadata

    published = (
            published_record.tag_collection_published is True and
            published_record.tag_record_qc is True and
            published_record.tag_record_retracted is None
    )
    if catalog_id == 'MIMS':
        published = published and published_record.tag_collection_infrastructure == 'MIMS'

    r = api(
        [ODPScope.CATALOG_READ], create_scopes=False
    ).get(f'/catalog/{catalog_id}/records')

    assert r.status_code == 200
    json = r.json()
    items = json['items']
    assert json['total'] == len(items) == published

    if not published:
        return

    result = items[0]
    assert result['id'] == published_record.id
    assert result['doi'] == published_record.doi
    assert result['sid'] == published_record.sid
    assert result['collection_key'] == published_record.collection.key
    assert result['collection_name'] == published_record.collection.name
    assert result['provider_key'] == published_record.collection.provider.key
    assert result['provider_name'] == published_record.collection.provider.name
    assert result['published'] is True
    assert result['searchable'] is True
    assert_new_timestamp(datetime.fromisoformat(result['timestamp']))

    if published_record.schema_id == 'SAEON.DataCite4':
        assert len(result['metadata_records']) == 1
        check_metadata_record('SAEON.DataCite4')

    elif published_record.schema_id == 'SAEON.ISO19115':
        assert len(result['metadata_records']) == 2
        check_metadata_record('SAEON.ISO19115')
        # TODO: check why the example translated record does not
        #  exactly match the dynamically translated one here
        check_metadata_record('SAEON.DataCite4', deep=False)

    else:
        assert False
