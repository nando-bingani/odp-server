from random import randint

from sqlalchemy import select

import migrate.systemdata
from odp.const import ODPScope, ODPSystemRole
from odp.const.db import ScopeType
from odp.db.models import (
    Archive,
    Catalog,
    Client,
    ClientScope,
    Collection,
    CollectionTag,
    Keyword,
    Package,
    PackageTag,
    Provider,
    ProviderUser,
    Record,
    RecordTag,
    Resource,
    Role,
    RoleCollection,
    RoleScope,
    Schema,
    Scope,
    Tag,
    User,
    UserRole,
    Vocabulary,
    VocabularyTerm,
)
from test import TestSession
from test.factories import (
    ArchiveFactory,
    CatalogFactory,
    ClientFactory,
    CollectionFactory,
    CollectionTagFactory,
    KeywordFactory,
    PackageFactory,
    PackageTagFactory,
    ProviderFactory,
    RecordFactory,
    RecordTagFactory,
    ResourceFactory,
    RoleFactory,
    SchemaFactory,
    ScopeFactory,
    TagFactory,
    UserFactory,
    VocabularyFactory,
)


def sorted_tuples(rows):
    return sorted(rows, key=lambda row: ''.join(str(val) for val in row))


def test_db_setup():
    migrate.systemdata.init_system_scopes()
    migrate.systemdata.Session.commit()

    result = TestSession.execute(select(Scope)).scalars()
    assert sorted(row.id for row in result) == sorted(s.value for s in ODPScope)

    # create a batch of arbitrary scopes; these should not be assigned to
    # any predefined system roles by init_system_roles()
    scopes = ScopeFactory.create_batch(5)

    migrate.systemdata.init_system_roles()
    migrate.systemdata.Session.commit()

    result = TestSession.execute(select(Role)).scalars()
    assert sorted_tuples((row.id, row.collection_specific) for row in result) \
           == sorted_tuples((r.value, False) for r in ODPSystemRole)

    result = TestSession.execute(
        select(RoleScope).
        where(RoleScope.role_id == ODPSystemRole.ODP_ADMIN)
    ).scalars()
    assert sorted_tuples((row.scope_id, row.scope_type) for row in result) \
           == sorted_tuples((s.value, ScopeType.odp) for s in ODPScope)

    result = TestSession.execute(
        select(RoleScope).
        where(RoleScope.role_id != ODPSystemRole.ODP_ADMIN)
    ).scalars()
    assert all(
        row.scope_id not in (s.id for s in scopes) and row.scope_type == ScopeType.odp
        for row in result
    )


def test_create_archive():
    archive = ArchiveFactory()
    result = TestSession.execute(select(Archive)).scalar_one()
    assert (
               result.id,
               result.url,
               result.adapter,
               result.scope_id,
           ) == (
               archive.id,
               archive.url,
               archive.adapter,
               archive.scope_id,
           )


def test_create_catalog():
    catalog = CatalogFactory()
    result = TestSession.execute(select(Catalog)).scalar_one()
    assert (result.id, result.url, result.data, result.timestamp) \
           == (catalog.id, catalog.url, catalog.data, catalog.timestamp)


def test_create_client():
    client = ClientFactory()
    result = TestSession.execute(select(Client, Provider).outerjoin(Provider)).one()
    assert (
               result.Client.id,
               result.Client.provider_specific,
               result.Client.provider_id,
               result.Provider.key if result.Client.provider_id else None,
           ) == (
               client.id,
               client.provider_specific,
               client.provider_id,
               client.provider.key if client.provider_id else None,
           )


def test_create_client_with_scopes():
    scopes = ScopeFactory.create_batch(5)
    client = ClientFactory(scopes=scopes)
    result = TestSession.execute(select(ClientScope)).scalars()
    assert sorted_tuples((row.client_id, row.scope_id, row.scope_type) for row in result) \
           == sorted_tuples((client.id, scope.id, scope.type) for scope in scopes)


def test_create_collection():
    collection = CollectionFactory()
    result = TestSession.execute(select(Collection, Provider).join(Provider)).one()
    assert (
               result.Collection.id,
               result.Collection.key,
               result.Collection.name,
               result.Collection.doi_key,
               result.Collection.provider_id,
               result.Provider.name,
           ) == (
               collection.id,
               collection.key,
               collection.name,
               collection.doi_key,
               collection.provider.id,
               collection.provider.name,
           )


def test_create_collection_tag():
    collection_tag = CollectionTagFactory()
    result = TestSession.execute(select(CollectionTag).join(Collection).join(Tag)).scalar_one()
    assert (result.collection_id, result.tag_id, result.tag_type, result.user_id, result.data) \
           == (collection_tag.collection.id, collection_tag.tag.id, 'collection', collection_tag.user.id, collection_tag.data)


def test_create_keyword():
    def assert_result(kw):
        result = results.pop(kw.id)
        assert (
                   kw.id,
                   kw.data,
                   kw.status,
                   kw.parent_id,
                   kw.child_schema_id,
                   kw.child_schema_type,
               ) == (
                   result.id,
                   result.data,
                   result.status,
                   result.parent_id,
                   result.child_schema_id,
                   result.child_schema_type,
               )
        for child_kw in kw.children:
            assert_result(child_kw)

    keyword = KeywordFactory()
    results = {row.id: row for row in TestSession.execute(select(Keyword)).scalars()}
    assert_result(keyword)
    assert not results  # should have popped all


def test_create_package():
    package = PackageFactory()
    result = TestSession.execute(select(Package)).scalar_one()
    assert (
               result.id,
               result.title,
               result.status,
               result.provider_id,
               result.timestamp,
           ) == (
               package.id,
               package.title,
               package.status,
               package.provider_id,
               package.timestamp,
           )


def test_create_package_tag():
    package_tag = PackageTagFactory()
    result = TestSession.execute(select(PackageTag).join(Package).join(Tag)).scalar_one()
    assert (
               result.package_id,
               result.tag_id,
               result.tag_type,
               result.user_id,
               result.data,
               result.timestamp,
           ) == (
               package_tag.package.id,
               package_tag.tag.id,
               'package',
               package_tag.user.id,
               package_tag.data,
               package_tag.timestamp,
           )


def test_create_provider():
    provider = ProviderFactory()
    result = TestSession.execute(select(Provider)).scalar_one()
    assert (result.id, result.key, result.name) == (provider.id, provider.key, provider.name)


def test_create_provider_with_users():
    users = UserFactory.create_batch(5)
    provider = ProviderFactory(users=users)
    result = TestSession.execute(select(ProviderUser)).scalars()
    assert sorted_tuples((row.provider_id, row.user_id) for row in result) \
           == sorted_tuples((provider.id, user.id) for user in users)


def test_create_record():
    record = RecordFactory(is_child_record=randint(0, 1))
    result = TestSession.execute(
        select(Record).where(Record.id == record.id)
    ).scalar_one()
    assert (
               result.id,
               result.doi,
               result.sid,
               result.metadata_,
               result.validity,
               result.collection_id,
               result.schema_id,
               result.schema_type,
               result.parent_id,
           ) == (
               record.id,
               record.doi,
               record.sid,
               record.metadata_,
               record.validity,
               record.collection.id,
               record.schema.id,
               record.schema.type,
               record.parent_id,
           )
    if record.parent_id:
        parent = TestSession.execute(
            select(Record).where(Record.id == record.parent_id)
        ).scalar_one()
        assert result.parent == parent
        assert result.parent_id == parent.id
        assert parent.children == [result]


def test_create_record_tag():
    record_tag = RecordTagFactory()
    result = TestSession.execute(select(RecordTag).join(Record).join(Tag)).scalar_one()
    assert (result.record_id, result.tag_id, result.tag_type, result.user_id, result.data) \
           == (record_tag.record.id, record_tag.tag.id, 'record', record_tag.user.id, record_tag.data)


def test_create_resource():
    resource = ResourceFactory()
    result = TestSession.execute(select(Resource)).scalar_one()
    assert (
               result.id,
               result.title,
               result.description,
               result.filename,
               result.mimetype,
               result.size,
               result.md5,
               result.timestamp,
               result.provider_id,
           ) == (
               resource.id,
               resource.title,
               resource.description,
               resource.filename,
               resource.mimetype,
               resource.size,
               resource.md5,
               resource.timestamp,
               resource.provider_id,
           )


def test_create_role():
    role = RoleFactory()
    result = TestSession.execute(select(Role)).scalar_one()
    assert (result.id, result.collection_specific) == (role.id, role.collection_specific)


def test_create_role_with_collections():
    collections = CollectionFactory.create_batch(5)
    role = RoleFactory(collections=collections)
    result = TestSession.execute(select(RoleCollection)).scalars()
    assert sorted_tuples((row.role_id, row.collection_id) for row in result) \
           == sorted_tuples((role.id, collection.id) for collection in collections)


def test_create_role_with_scopes():
    scopes = ScopeFactory.create_batch(5, type='odp') + ScopeFactory.create_batch(5, type='client')
    role = RoleFactory(scopes=scopes)
    result = TestSession.execute(select(RoleScope)).scalars()
    assert sorted_tuples((row.role_id, row.scope_id, row.scope_type) for row in result) \
           == sorted_tuples((role.id, scope.id, scope.type) for scope in scopes)


def test_create_schema():
    schema = SchemaFactory()
    result = TestSession.execute(
        select(Schema).
        where(Schema.id.notlike('vocab-schema-%'))  # ignore schemas created by vocabulary factories
    ).scalar_one()
    assert (result.id, result.type, result.uri) == (schema.id, schema.type, schema.uri)


def test_create_scope():
    scope = ScopeFactory()
    result = TestSession.execute(select(Scope)).scalar_one()
    assert (result.id, result.type) == (scope.id, scope.type)


def test_create_tag():
    tag = TagFactory()
    result = TestSession.execute(select(Tag, Scope).join(Scope)).one()
    assert (
               result.Tag.id,
               result.Tag.type,
               result.Tag.cardinality,
               result.Tag.public,
               result.Tag.schema_id,
               result.Tag.scope_id,
               result.Tag.scope_type,
               result.Tag.vocabulary_id,
           ) == (
               tag.id,
               tag.type,
               tag.cardinality,
               tag.public,
               tag.schema_id,
               tag.scope.id,
               ScopeType.odp,
               tag.vocabulary_id,
           )


def test_create_user():
    user = UserFactory()
    result = TestSession.execute(select(User)).scalar_one()
    assert (result.id, result.name, result.email, result.active, result.verified) \
           == (user.id, user.name, user.email, user.active, user.verified)


def test_create_user_with_roles():
    roles = RoleFactory.create_batch(5)
    user = UserFactory(roles=roles)
    result = TestSession.execute(select(UserRole)).scalars()
    assert sorted_tuples((row.user_id, row.role_id) for row in result) \
           == sorted_tuples((user.id, role.id) for role in roles)


def test_create_vocabulary():
    vocabulary = VocabularyFactory()
    result = TestSession.execute(select(Vocabulary, VocabularyTerm).join(VocabularyTerm))
    assert sorted_tuples((
                             row.Vocabulary.id,
                             row.Vocabulary.scope_id,
                             row.Vocabulary.scope_type,
                             row.Vocabulary.schema_id,
                             row.Vocabulary.schema_type,
                             row.Vocabulary.static,
                             row.VocabularyTerm.vocabulary_id,
                             row.VocabularyTerm.term_id,
                             row.VocabularyTerm.data
                         ) for row in result) \
           == sorted_tuples((
                                vocabulary.id,
                                vocabulary.scope_id,
                                'odp',
                                vocabulary.schema_id,
                                'vocabulary',
                                vocabulary.static,
                                term.vocabulary_id,
                                term.term_id,
                                term.data,
                            ) for term in vocabulary.terms)
