from random import randint

import pytest
from sqlalchemy import select

from odp.const import ODPScope
from odp.const.db import SchemaType, ScopeType
from odp.db.models import Scope, Vocabulary, VocabularyTerm, VocabularyTermAudit
from test import TestSession
from test.api import (
    assert_conflict, assert_empty_result, assert_forbidden, assert_new_timestamp,
    assert_not_found, assert_unprocessable,
)
from test.factories import FactorySession, SchemaFactory, VocabularyFactory, VocabularyTermFactory, fake


@pytest.fixture
def vocabulary_batch():
    """Create and commit a batch of (non-static) Vocabulary
    instances, with associated terms. The #2 vocab is given scope
    odp.vocabulary:project, for use with term modification tests."""
    vocabs = []
    for n in range(randint(3, 5)):
        kwargs = dict(static=False)
        if n == 2:
            kwargs |= dict(
                scope=FactorySession.get(Scope, (ODPScope.VOCABULARY_PROJECT, ScopeType.odp)),
                schema=SchemaFactory(
                    type=SchemaType.vocabulary,
                    uri='https://odp.saeon.ac.za/schema/vocabulary/project',
                )
            )
        vocabs += [VocabularyFactory(**kwargs)]
    return vocabs


def assert_db_state(vocabularies):
    """Verify that the DB vocabulary table contains the given vocabulary batch."""
    result = TestSession.execute(select(Vocabulary)).scalars().all()
    result.sort(key=lambda v: v.id)
    vocabularies.sort(key=lambda v: v.id)
    assert len(result) == len(vocabularies)
    for n, row in enumerate(result):
        assert row.id == vocabularies[n].id
        assert row.scope_id == vocabularies[n].scope_id
        assert row.scope_type == 'odp'
        assert row.schema_id == vocabularies[n].schema_id
        assert row.schema_type == 'vocabulary'
        assert row.static == vocabularies[n].static


def assert_db_term_state(vocab_id, *terms):
    """Verify that the vocabulary_term table contains the given terms."""
    result = TestSession.execute(select(VocabularyTerm).where(VocabularyTerm.vocabulary_id == vocab_id)).scalars().all()
    result.sort(key=lambda t: t.term_id)
    terms = sorted(terms, key=lambda t: t.term_id)
    assert len(result) == len(terms)
    for n, row in enumerate(result):
        assert row.vocabulary_id == vocab_id
        assert row.term_id == terms[n].term_id
        assert row.data == terms[n].data | dict(id=row.term_id)


def assert_audit_log(command, term, grant_type):
    """Verify that the vocabulary term audit table contains the given entry."""
    result = TestSession.execute(select(VocabularyTermAudit)).scalar_one()
    assert result.client_id == 'odp.test.client'
    assert result.user_id == ('odp.test.user' if grant_type == 'authorization_code' else None)
    assert result.command == command
    assert_new_timestamp(result.timestamp)
    assert result._vocabulary_id == term.vocabulary_id
    assert result._term_id == term.term_id
    assert result._data == term.data | dict(id=term.term_id)


def assert_no_audit_log():
    """Verify that no audit log entries have been created."""
    assert TestSession.execute(select(VocabularyTermAudit)).first() is None


def assert_json_result(response, json, vocabulary):
    """Verify that the API result matches the given vocabulary object."""
    assert response.status_code == 200
    assert json['id'] == vocabulary.id
    assert json['scope_id'] == vocabulary.scope_id
    assert json['schema_id'] == vocabulary.schema_id
    assert json['schema_uri'] == vocabulary.schema.uri
    assert json['schema_']['$id'] == vocabulary.schema.uri
    assert json['static'] == vocabulary.static

    json_terms = sorted(json['terms'], key=lambda t: t['id'])
    vocab_terms = sorted(vocabulary.terms, key=lambda t: t.term_id)
    assert [(json_term['id'], json_term['data']) for json_term in json_terms] == \
           [(vocab_term.term_id, vocab_term.data) for vocab_term in vocab_terms]


def assert_json_results(response, json, vocabularies):
    """Verify that the API result list matches the given vocabulary batch."""
    items = json['items']
    assert json['total'] == len(items) == len(vocabularies)
    items.sort(key=lambda i: i['id'])
    vocabularies.sort(key=lambda v: v.id)
    for n, vocabulary in enumerate(vocabularies):
        assert_json_result(response, items[n], vocabulary)


@pytest.mark.require_scope(ODPScope.VOCABULARY_READ)
def test_list_vocabularies(api, vocabulary_batch, scopes):
    authorized = ODPScope.VOCABULARY_READ in scopes
    r = api(scopes).get('/vocabulary/')
    if authorized:
        assert_json_results(r, r.json(), vocabulary_batch)
    else:
        assert_forbidden(r)
    assert_db_state(vocabulary_batch)


@pytest.mark.require_scope(ODPScope.VOCABULARY_READ)
def test_get_vocabulary(api, vocabulary_batch, scopes):
    authorized = ODPScope.VOCABULARY_READ in scopes
    r = api(scopes).get(f'/vocabulary/{vocabulary_batch[2].id}')
    if authorized:
        assert_json_result(r, r.json(), vocabulary_batch[2])
    else:
        assert_forbidden(r)
    assert_db_state(vocabulary_batch)


def test_get_vocabulary_not_found(api, vocabulary_batch):
    scopes = [ODPScope.VOCABULARY_READ]
    r = api(scopes).get('/vocabulary/foo')
    assert_not_found(r)
    assert_db_state(vocabulary_batch)


@pytest.mark.require_scope(ODPScope.VOCABULARY_PROJECT)
def test_create_term(api, vocabulary_batch, scopes):
    authorized = ODPScope.VOCABULARY_PROJECT in scopes
    client = api(scopes)

    vocab = vocabulary_batch[2]
    vocab.terms  # load existing terms
    term = VocabularyTermFactory.stub(
        vocabulary_id=vocab.id,
        data={'title': 'Some Project'},
    )

    r = client.post(f'/vocabulary/{vocab.id}/term', json=dict(
        id=term.term_id,
        data=term.data,
    ))

    if authorized:
        assert_empty_result(r)
        assert_db_term_state(vocab.id, *vocab.terms, term)
        assert_audit_log('insert', term, api.grant_type)
    else:
        assert_forbidden(r)
        assert_db_term_state(vocab.id, *vocab.terms)
        assert_no_audit_log()

    assert_db_state(vocabulary_batch)


def test_create_term_conflict(api, vocabulary_batch):
    scopes = [ODPScope.VOCABULARY_PROJECT]
    client = api(scopes)

    vocab = vocabulary_batch[2]
    vocab.terms  # load existing terms

    r = client.post(f'/vocabulary/{vocab.id}/term', json=dict(
        id=vocab.terms[1].term_id,
        data={'title': 'Some Project'},
    ))

    assert_conflict(r, 'Term already exists in vocabulary')
    assert_db_state(vocabulary_batch)
    assert_db_term_state(vocab.id, *vocab.terms)
    assert_no_audit_log()


def test_create_term_invalid(api, vocabulary_batch):
    scopes = [ODPScope.VOCABULARY_PROJECT]
    client = api(scopes)

    vocab = vocabulary_batch[2]
    vocab.terms  # load existing terms

    r = client.post(f'/vocabulary/{vocab.id}/term', json=dict(
        id=fake.word(),
        data={'name': 'Project should have a title not a name'},
    ))

    assert_unprocessable(r, valid=False)
    assert_db_state(vocabulary_batch)
    assert_db_term_state(vocab.id, *vocab.terms)
    assert_no_audit_log()


def test_create_term_static_vocab(api, vocabulary_batch):
    scopes = [ODPScope.VOCABULARY_PROJECT]
    client = api(scopes)

    vocab = vocabulary_batch[2]
    vocab.static = True
    FactorySession.add(vocab)
    FactorySession.commit()
    vocab.terms  # load existing terms

    r = client.post(f'/vocabulary/{vocab.id}/term', json=dict(
        id=fake.word(),
        data={'title': 'Some Project'},
    ))

    assert_unprocessable(r, 'Static vocabulary cannot be modified')
    assert_db_state(vocabulary_batch)
    assert_db_term_state(vocab.id, *vocab.terms)
    assert_no_audit_log()


@pytest.mark.require_scope(ODPScope.VOCABULARY_PROJECT)
def test_update_term(api, vocabulary_batch, scopes):
    authorized = ODPScope.VOCABULARY_PROJECT in scopes
    client = api(scopes)

    vocab = vocabulary_batch[2]
    term = VocabularyTermFactory.stub(
        vocabulary_id=vocab.id,
        term_id=vocab.terms[2].term_id,  # load existing terms
        data={'title': 'Some Project'},
    )

    r = client.put(f'/vocabulary/{vocab.id}/term', json=dict(
        id=term.term_id,
        data=term.data,
    ))

    if authorized:
        assert_empty_result(r)
        expected_terms = vocab.terms[:2] + [term] + vocab.terms[3:]
        assert_db_term_state(vocab.id, *expected_terms)
        assert_audit_log('update', term, api.grant_type)
    else:
        assert_forbidden(r)
        assert_db_term_state(vocab.id, *vocab.terms)
        assert_no_audit_log()

    assert_db_state(vocabulary_batch)


def test_update_term_not_found(api, vocabulary_batch):
    scopes = [ODPScope.VOCABULARY_PROJECT]
    client = api(scopes)

    vocab = vocabulary_batch[2]
    vocab.terms  # load existing terms

    r = client.put(f'/vocabulary/{vocab.id}/term', json=dict(
        id=fake.word(),
        data={'title': 'Some Project'},
    ))
    assert_not_found(r)
    assert_db_state(vocabulary_batch)
    assert_db_term_state(vocab.id, *vocab.terms)
    assert_no_audit_log()


def test_update_term_invalid(api, vocabulary_batch):
    scopes = [ODPScope.VOCABULARY_PROJECT]
    client = api(scopes)

    vocab = vocabulary_batch[2]

    r = client.put(f'/vocabulary/{vocab.id}/term', json=dict(
        id=vocab.terms[1].term_id,  # load existing terms
        data={'name': 'Project should have a title not a name'},
    ))

    assert_unprocessable(r, valid=False)
    assert_db_state(vocabulary_batch)
    assert_db_term_state(vocab.id, *vocab.terms)
    assert_no_audit_log()


def test_update_term_static_vocab(api, vocabulary_batch):
    scopes = [ODPScope.VOCABULARY_PROJECT]
    client = api(scopes)

    vocab = vocabulary_batch[2]
    vocab.static = True
    FactorySession.add(vocab)
    FactorySession.commit()

    r = client.put(f'/vocabulary/{vocab.id}/term', json=dict(
        id=vocab.terms[1].term_id,  # load existing terms
        data={'title': 'Some Project'},
    ))

    assert_unprocessable(r, 'Static vocabulary cannot be modified')
    assert_db_state(vocabulary_batch)
    assert_db_term_state(vocab.id, *vocab.terms)
    assert_no_audit_log()


@pytest.mark.require_scope(ODPScope.VOCABULARY_PROJECT)
def test_delete_term(api, vocabulary_batch, scopes):
    authorized = ODPScope.VOCABULARY_PROJECT in scopes
    client = api(scopes)

    vocab = vocabulary_batch[2]
    deleted_term = vocab.terms[2]  # load existing terms

    r = client.delete(f'/vocabulary/{vocab.id}/term/{deleted_term.term_id}')

    if authorized:
        assert_empty_result(r)
        expected_terms = vocab.terms[:2] + vocab.terms[3:]
        assert_db_term_state(vocab.id, *expected_terms)
        assert_audit_log('delete', deleted_term, api.grant_type)
    else:
        assert_forbidden(r)
        assert_db_term_state(vocab.id, *vocab.terms)
        assert_no_audit_log()

    assert_db_state(vocabulary_batch)


def test_delete_term_not_found(api, vocabulary_batch):
    scopes = [ODPScope.VOCABULARY_PROJECT]
    client = api(scopes)

    vocab = vocabulary_batch[2]
    vocab.terms  # load existing terms

    r = client.delete(f'/vocabulary/{vocab.id}/term/{fake.word()}')

    assert_not_found(r)
    assert_db_state(vocabulary_batch)
    assert_db_term_state(vocab.id, *vocab.terms)
    assert_no_audit_log()


def test_delete_term_static_vocab(api, vocabulary_batch):
    scopes = [ODPScope.VOCABULARY_PROJECT]
    client = api(scopes)

    vocab = vocabulary_batch[2]
    vocab.static = True
    FactorySession.add(vocab)
    FactorySession.commit()

    term_id = vocab.terms[1].term_id  # load existing terms
    r = client.delete(f'/vocabulary/{vocab.id}/term/{term_id}')

    assert_unprocessable(r, 'Static vocabulary cannot be modified')
    assert_db_state(vocabulary_batch)
    assert_db_term_state(vocab.id, *vocab.terms)
    assert_no_audit_log()
