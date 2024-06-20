from datetime import datetime, timezone
from functools import partial

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from jschon import JSON, URI
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from starlette.status import HTTP_404_NOT_FOUND, HTTP_409_CONFLICT, HTTP_422_UNPROCESSABLE_ENTITY

from odp.api.lib.auth import Authorize, Authorized
from odp.api.lib.paging import Paginator
from odp.api.models import KeywordHierarchyModel, KeywordModel, KeywordModelAdmin, KeywordModelIn, Page
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


def get_child_schema(parent: Keyword) -> Schema | None:
    """Get the validating schema for keywords in the given parent vocabulary."""
    if parent is None:
        return None

    if parent.child_schema_id:
        return parent.child_schema

    return get_child_schema(parent.parent)


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
        get_child_schema(parent).uri
    ))

    validity = keyword_jsonschema.evaluate(JSON(keyword_in.data)).output('basic')
    if not validity['valid']:
        raise HTTPException(
            HTTP_422_UNPROCESSABLE_ENTITY, validity
        )


def output_keyword_model(
        keyword: Keyword,
        recurse: bool = False,
) -> KeywordModel | KeywordHierarchyModel:
    cls = KeywordHierarchyModel if recurse else KeywordModel

    schema = get_child_schema(keyword.parent)
    kwargs = dict(
        key=keyword.key,
        data=keyword.data,
        status=keyword.status,
        schema_id=schema.id if schema else None,
    )

    if recurse:
        kwargs |= dict(
            child_keywords=[
                output_keyword_model(child, True)
                for child in keyword.children
            ]
        )

    return cls(**kwargs)


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
        _child_schema_id=keyword.child_schema_id,
    ).save()


@router.get(
    '/',
    dependencies=[Depends(Authorize(ODPScope.KEYWORD_READ))],
)
async def list_vocabularies(
        paginator: Paginator = Depends(partial(Paginator, sort='key')),
) -> Page[KeywordModel]:
    """
    List top-level keywords (root vocabularies). Requires scope `odp.keyword:read`.
    """
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
    dependencies=[Depends(Authorize(ODPScope.KEYWORD_READ))],
)
async def list_keywords(
        parent_key: str = Path(..., title='Parent keyword (vocabulary) identifier', regex=KEYWORD_REGEX),
        recurse: bool = Query(False, title='Populate child keywords, recursively'),
        paginator: Paginator = Depends(partial(Paginator, sort='key')),
) -> Page[KeywordHierarchyModel] | Page[KeywordModel]:
    """
    List the keywords in a vocabulary. Requires scope `odp.keyword:read`.
    """
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
    dependencies=[Depends(Authorize(ODPScope.KEYWORD_READ))],
)
async def get_keyword(
        key: str = Path(..., title='Keyword identifier', regex=KEYWORD_REGEX),
        recurse: bool = Query(False, title='Populate child keywords, recursively'),
) -> KeywordHierarchyModel | KeywordModel:
    """
    Get a keyword. Requires scope `odp.keyword:read`.
    """
    if not (keyword := Session.get(Keyword, key)):
        raise HTTPException(
            HTTP_404_NOT_FOUND, f"Keyword '{key}' does not exist"
        )

    return output_keyword_model(keyword, recurse)


@router.post(
    '/{key:path}',
    dependencies=[Depends(validate_keyword_data)],
)
async def suggest_keyword(
        keyword_in: KeywordModelIn,
        key: str = Path(..., title='Keyword identifier', regex=KEYWORD_REGEX),
        parent_key: str = Depends(get_parent_key),
        auth: Authorized = Depends(Authorize(ODPScope.KEYWORD_SUGGEST)),
) -> KeywordModel:
    """
    Create a keyword with status `proposed`. Requires scope `odp.keyword:suggest`.
    """
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
    dependencies=[Depends(validate_keyword_data)],
)
async def set_keyword(
        keyword_in: KeywordModelAdmin,
        key: str = Path(..., title='Keyword identifier', regex=KEYWORD_REGEX),
        parent_key: str = Depends(get_parent_key),
        auth: Authorized = Depends(Authorize(ODPScope.KEYWORD_ADMIN)),
) -> KeywordModel:
    """
    Create or update a keyword. Requires scope `odp.keyword:admin`.
    """
    if keyword := Session.get(Keyword, key):
        command = AuditCommand.update
    else:
        keyword = Keyword(key=key, parent_key=parent_key)
        command = AuditCommand.insert

    if (
            keyword.data != keyword_in.data or
            keyword.status != keyword_in.status or
            keyword.child_schema_id != keyword_in.child_schema_id
    ):
        keyword.data = keyword_in.data
        keyword.status = keyword_in.status
        keyword.child_schema_id = keyword_in.child_schema_id
        keyword.child_schema_type = SchemaType.keyword if keyword_in.child_schema_id else None

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
)
async def delete_keyword(
        key: str = Path(..., title='Keyword identifier', regex=KEYWORD_REGEX),
        auth: Authorized = Depends(Authorize(ODPScope.KEYWORD_ADMIN)),
) -> None:
    """
    Delete a keyword. Requires scope `odp.keyword:admin`.
    """
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
