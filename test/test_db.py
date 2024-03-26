from random import randint

from sqlalchemy import select

import migrate.systemdata
from odp.const import ODPScope, ODPSystemRole
from odp.const.db import ScopeType
from odp.db import Session
from odp.db.models import (
    Archive,
    Catalog,
    Client,
    ClientScope,
    Collection,
    CollectionTag,
    Package,
    Provider,
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
    UserProvider,
    UserRole,
    Vocabulary,
    VocabularyTerm,
)
from test.factories import (
    ArchiveFactory,
    CatalogFactory,
    ClientFactory,
    CollectionFactory,
    CollectionTagFactory,
    PackageFactory,
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
    Session.commit()
    result = Session.execute(select(Scope)).scalars()
    assert sorted(row.id for row in result) == sorted(s.value for s in ODPScope)

    # create a batch of arbitrary scopes; these should not be assigned to
    # any predefined system roles by init_system_roles()
    scopes = ScopeFactory.create_batch(5)

    migrate.systemdata.init_system_roles()
    Session.commit()
    result = Session.execute(select(Role)).scalars()
    assert sorted_tuples((row.id, row.collection_specific) for row in result) \
           == sorted_tuples((r.value, False) for r in ODPSystemRole)

    result = Session.execute(
        select(RoleScope).
        where(RoleScope.role_id == ODPSystemRole.ODP_ADMIN)
    ).scalars()
    assert sorted_tuples((row.scope_id, row.scope_type) for row in result) \
           == sorted_tuples((s.value, ScopeType.odp) for s in ODPScope)

    result = Session.execute(
        select(RoleScope).
        where(RoleScope.role_id != ODPSystemRole.ODP_ADMIN)
    ).scalars()
    assert all(
        row.scope_id not in (s.id for s in scopes) and row.scope_type == ScopeType.odp
        for row in result
    )


def test_create_archive():
    archive = ArchiveFactory()
    result = Session.execute(select(Archive)).scalar_one()
    assert (result.id, result.url) == (archive.id, archive.url)


def test_create_catalog():
    catalog = CatalogFactory()
    result = Session.execute(select(Catalog)).scalar_one()
    assert (result.id, result.url, result.data, result.timestamp) \
           == (catalog.id, catalog.url, catalog.data, catalog.timestamp)


def test_create_client():
    client = ClientFactory()
    result = Session.execute(select(Client, Provider).outerjoin(Provider)).one()
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
    result = Session.execute(select(ClientScope)).scalars()
    assert sorted_tuples((row.client_id, row.scope_id, row.scope_type) for row in result) \
           == sorted_tuples((client.id, scope.id, scope.type) for scope in scopes)


def test_create_collection():
    collection = CollectionFactory()
    result = Session.execute(select(Collection, Provider).join(Provider)).one()
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
    result = Session.execute(select(CollectionTag).join(Collection).join(Tag)).scalar_one()
    assert (result.collection_id, result.tag_id, result.tag_type, result.user_id, result.data) \
           == (collection_tag.collection.id, collection_tag.tag.id, 'collection', collection_tag.user.id, collection_tag.data)


def test_create_package():
    package = PackageFactory()
    result = Session.execute(select(Package)).scalar_one()
    assert (
               result.id,
               result.metadata_,
               result.validity,
               result.notes,
               result.provider_id,
               result.schema_id,
               result.schema_type,
               result.timestamp,
           ) == (
               package.id,
               package.metadata_,
               package.validity,
               package.notes,
               package.provider_id,
               package.schema_id,
               package.schema_type,
               package.timestamp,
           )


def test_create_provider():
    provider = ProviderFactory()
    result = Session.execute(select(Provider)).scalar_one()
    assert (result.id, result.key, result.name) == (provider.id, provider.key, provider.name)


def test_create_record():
    record = RecordFactory(is_child_record=randint(0, 1))
    result = Session.execute(
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
        parent = Session.execute(
            select(Record).where(Record.id == record.parent_id)
        ).scalar_one()
        assert result.parent == parent
        assert result.parent_id == parent.id
        assert parent.children == [result]


def test_create_record_tag():
    record_tag = RecordTagFactory()
    result = Session.execute(select(RecordTag).join(Record).join(Tag)).scalar_one()
    assert (result.record_id, result.tag_id, result.tag_type, result.user_id, result.data) \
           == (record_tag.record.id, record_tag.tag.id, 'record', record_tag.user.id, record_tag.data)


def test_create_resource():
    resource = ResourceFactory()
    result = Session.execute(select(Resource)).scalar_one()
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
    result = Session.execute(select(Role)).scalar_one()
    assert (result.id, result.collection_specific) == (role.id, role.collection_specific)


def test_create_role_with_collections():
    collections = CollectionFactory.create_batch(5)
    role = RoleFactory(collections=collections)
    result = Session.execute(select(RoleCollection)).scalars()
    assert sorted_tuples((row.role_id, row.collection_id) for row in result) \
           == sorted_tuples((role.id, collection.id) for collection in collections)


def test_create_role_with_scopes():
    scopes = ScopeFactory.create_batch(5, type='odp') + ScopeFactory.create_batch(5, type='client')
    role = RoleFactory(scopes=scopes)
    result = Session.execute(select(RoleScope)).scalars()
    assert sorted_tuples((row.role_id, row.scope_id, row.scope_type) for row in result) \
           == sorted_tuples((role.id, scope.id, scope.type) for scope in scopes)


def test_create_schema():
    schema = SchemaFactory()
    result = Session.execute(
        select(Schema).
        where(Schema.id.notlike('vocab-schema-%'))  # ignore schemas created by vocabulary factories
    ).scalar_one()
    assert (result.id, result.type, result.uri) == (schema.id, schema.type, schema.uri)


def test_create_scope():
    scope = ScopeFactory()
    result = Session.execute(select(Scope)).scalar_one()
    assert (result.id, result.type) == (scope.id, scope.type)


def test_create_tag():
    tag = TagFactory()
    result = Session.execute(select(Tag, Scope).join(Scope)).one()
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
    result = Session.execute(select(User)).scalar_one()
    assert (result.id, result.name, result.email, result.active, result.verified) \
           == (user.id, user.name, user.email, user.active, user.verified)


def test_create_user_with_providers():
    providers = ProviderFactory.create_batch(5)
    user = UserFactory(providers=providers)
    result = Session.execute(select(UserProvider)).scalars()
    assert sorted_tuples((row.user_id, row.provider_id) for row in result) \
           == sorted_tuples((user.id, provider.id) for provider in providers)


def test_create_user_with_roles():
    roles = RoleFactory.create_batch(5)
    user = UserFactory(roles=roles)
    result = Session.execute(select(UserRole)).scalars()
    assert sorted_tuples((row.user_id, row.role_id) for row in result) \
           == sorted_tuples((user.id, role.id) for role in roles)


def test_create_vocabulary():
    vocabulary = VocabularyFactory()
    result = Session.execute(select(Vocabulary, VocabularyTerm).join(VocabularyTerm))
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
