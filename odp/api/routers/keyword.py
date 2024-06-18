from datetime import datetime, timezone
from functools import partial

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from jschon import JSON, URI
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from starlette.status import HTTP_404_NOT_FOUND, HTTP_409_CONFLICT, HTTP_422_UNPROCESSABLE_ENTITY

from odp.api.lib.auth import Authorize, Authorized
from odp.api.lib.paging import Paginator
from odp.api.models import KeywordModel, KeywordModelAdmin, KeywordModelIn, Page
from odp.const import KEYWORD_REGEX, ODPScope
from odp.const.db import AuditCommand, KeywordStatus, SchemaType
from odp.db import Session
from odp.db.models import Keyword, KeywordAudit, Schema
from odp.lib.schema import schema_catalog

router = APIRouter()


def get_parent_key(key: str) -> str:
    if not (parent_key := key.rpartition('/')[0]):
        raise HTTPException(
            HTTP_422_UNPROCESSABLE_ENTITY, 'key must be suffixed to a parent key'
        )

    return parent_key


def get_validating_schema(parent: Keyword) -> Schema | None:
    """Get the validating schema for keywords in the given parent vocabulary."""
    if parent is None:
        return None

    if parent.schema_id:
        return parent.schema

    return get_validating_schema(parent.parent)


async def validate_keyword_data(
        keyword_in: KeywordModelIn,
        key: str,
        parent_key: str = Depends(get_parent_key),
) -> None:
    if keyword := Session.get(Keyword, key):
        parent = keyword.parent
    elif not (parent := Session.get(Keyword, parent_key)):
        raise HTTPException(
            HTTP_404_NOT_FOUND, f"Parent keyword '{parent_key}' does not exist"
        )

    keyword_jsonschema = schema_catalog.get_schema(URI(
        get_validating_schema(parent).uri
    ))

    validity = keyword_jsonschema.evaluate(JSON(keyword_in.data)).output('basic')
    if not validity['valid']:
        raise HTTPException(
            HTTP_422_UNPROCESSABLE_ENTITY, validity
        )


def output_keyword_model(keyword: Keyword, recurse: bool = False) -> KeywordModel:
    schema = get_validating_schema(keyword.parent)
    kw = KeywordModel(
        key=keyword.key,
        data=keyword.data,
        status=keyword.status,
        schema_id=schema.id if schema else None,
    )
    if recurse:
        kw.keywords = [
            output_keyword_model(child, True)
            for child in keyword.children
        ]

    return kw


def create_audit_record(
        auth: Authorized,
        keyword: Keyword,
        timestamp: datetime,
        command: AuditCommand,
) -> None:
    KeywordAudit(
        client_id=auth.client_id,
        user_id=auth.user_id,
        command=command,
        timestamp=timestamp,
        _key=keyword.key,
        _data=keyword.data,
        _status=keyword.status,
        _schema_id=keyword.schema_id,
    ).save()


@router.get(
    '/',
    response_model=Page[KeywordModel],
    dependencies=[Depends(Authorize(ODPScope.KEYWORD_READ))],
    description=f'List top-level keywords (root vocabularies). '
                f'Requires `{ODPScope.KEYWORD_READ}` scope.'
)
async def list_vocabularies(
        paginator: Paginator = Depends(partial(Paginator, sort='key')),
):
    stmt = (
        select(Keyword).
        where(Keyword.parent_key == None)
    )

    return paginator.paginate(
        stmt,
        lambda row: output_keyword_model(row.Keyword),
    )


@router.get(
    '/{parent_key:path}/',
    response_model=Page[KeywordModel],
    dependencies=[Depends(Authorize(ODPScope.KEYWORD_READ))],
    description=f'List the keywords in a vocabulary. '
                f'Requires `{ODPScope.KEYWORD_READ}` scope.'
)
async def list_keywords(
        parent_key: str = Path(..., title='Parent keyword (vocabulary) identifier', regex=KEYWORD_REGEX),
        recurse: bool = Query(False, title='Populate sub-keywords for each keyword, recursively'),
        paginator: Paginator = Depends(partial(Paginator, sort='key')),
):
    if not Session.get(Keyword, parent_key):
        raise HTTPException(
            HTTP_404_NOT_FOUND, f"Parent keyword '{parent_key}' does not exist"
        )

    stmt = (
        select(Keyword).
        where(Keyword.parent_key == parent_key)
    )

    return paginator.paginate(
        stmt,
        lambda row: output_keyword_model(row.Keyword, recurse),
    )


@router.get(
    '/{key:path}',
    response_model=KeywordModel,
    dependencies=[Depends(Authorize(ODPScope.KEYWORD_READ))],
    description=f'Get a keyword, optionally with sub-keywords. '
                f'Requires `{ODPScope.KEYWORD_READ}` scope.'
)
async def get_keyword(
        key: str = Path(..., title='Keyword identifier', regex=KEYWORD_REGEX),
        recurse: bool = Query(False, title='Populate sub-keywords, recursively'),
):
    if not (keyword := Session.get(Keyword, key)):
        raise HTTPException(
            HTTP_404_NOT_FOUND, f"Keyword '{key}' does not exist"
        )

    return output_keyword_model(keyword, recurse)


@router.post(
    '/{key:path}',
    response_model=KeywordModel,
    dependencies=[
        Depends(validate_keyword_data),
    ],
    description=f'Create a keyword with status `{KeywordStatus.proposed}`. '
                f'Requires `{ODPScope.KEYWORD_SUGGEST}` scope.'
)
async def suggest_keyword(
        keyword_in: KeywordModelIn,
        key: str = Path(..., title='Keyword identifier', regex=KEYWORD_REGEX),
        parent_key: str = Depends(get_parent_key),
        auth: Authorized = Depends(Authorize(ODPScope.KEYWORD_SUGGEST)),
):
    if Session.get(Keyword, key):
        raise HTTPException(
            HTTP_409_CONFLICT, f"Keyword '{key}' already exists"
        )

    keyword = Keyword(
        key=key,
        data=keyword_in.data,
        status=KeywordStatus.proposed,
        parent_key=parent_key,
    )
    keyword.save()

    create_audit_record(
        auth,
        keyword,
        datetime.now(timezone.utc),
        AuditCommand.insert,
    )

    return output_keyword_model(keyword)


@router.put(
    '/{key:path}',
    response_model=KeywordModel,
    dependencies=[
        Depends(validate_keyword_data),
    ],
    description=f'Create or update a keyword. '
                f'Requires `{ODPScope.KEYWORD_ADMIN}` scope.'
)
async def set_keyword(
        keyword_in: KeywordModelAdmin,
        key: str = Path(..., title='Keyword identifier', regex=KEYWORD_REGEX),
        parent_key: str = Depends(get_parent_key),
        auth: Authorized = Depends(Authorize(ODPScope.KEYWORD_ADMIN)),
):
    if keyword := Session.get(Keyword, key):
        command = AuditCommand.update
    else:
        keyword = Keyword(key=key, parent_key=parent_key)
        command = AuditCommand.insert

    if (
            keyword.data != keyword_in.data or
            keyword.status != keyword_in.status or
            keyword.schema_id != keyword_in.schema_id
    ):
        keyword.data = keyword_in.data
        keyword.status = keyword_in.status
        keyword.schema_id = keyword_in.schema_id
        keyword.schema_type = SchemaType.keyword if keyword_in.schema_id else None

        keyword.save()

        create_audit_record(
            auth,
            keyword,
            datetime.now(timezone.utc),
            command,
        )

    return output_keyword_model(keyword)


@router.delete(
    '/{key:path}',
    description=f'Delete a keyword. '
                f'Requires `{ODPScope.KEYWORD_ADMIN}` scope.'
)
async def delete_keyword(
        key: str = Path(..., title='Keyword identifier', regex=KEYWORD_REGEX),
        auth: Authorized = Depends(Authorize(ODPScope.KEYWORD_ADMIN)),
):
    if not (keyword := Session.get(Keyword, key)):
        raise HTTPException(
            HTTP_404_NOT_FOUND, f"Keyword '{key}' does not exist"
        )

    create_audit_record(
        auth,
        keyword,
        datetime.now(timezone.utc),
        AuditCommand.delete,
    )

    try:
        keyword.delete()

    except IntegrityError as e:
        raise HTTPException(
            HTTP_422_UNPROCESSABLE_ENTITY, f"Keyword '{key}' cannot be deleted"
        ) from e
