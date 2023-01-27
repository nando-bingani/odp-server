import re
from datetime import date
from functools import partial
from typing import Any, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from fastapi.responses import RedirectResponse
from pydantic import UUID4, constr
from sqlalchemy import and_, func, or_, select, text
from starlette.status import HTTP_404_NOT_FOUND, HTTP_422_UNPROCESSABLE_ENTITY

from odp.api.lib.auth import Authorize
from odp.api.lib.datacite import get_datacite_client
from odp.api.lib.paging import Page, Paginator
from odp.api.lib.utils import output_published_record_model
from odp.api.models import CatalogModel, PublishedDataCiteRecordModel, PublishedSAEONRecordModel, RetractedRecordModel
from odp.const import DOI_REGEX, ODPCatalog, ODPScope
from odp.db import Session
from odp.db.models import Catalog, CatalogRecord, PublishedRecord
from odp.lib.datacite import DataciteClient, DataciteError

router = APIRouter()


@router.get(
    '/',
    response_model=Page[CatalogModel],
    dependencies=[Depends(Authorize(ODPScope.CATALOG_READ))],
)
async def list_catalogs(
        paginator: Paginator = Depends(),
):
    stmt = (
        select(Catalog, func.count(CatalogRecord.catalog_id)).
        outerjoin(CatalogRecord, and_(Catalog.id == CatalogRecord.catalog_id, CatalogRecord.published)).
        group_by(Catalog)
    )

    return paginator.paginate(
        stmt,
        lambda row: CatalogModel(
            id=row.Catalog.id,
            url=row.Catalog.url,
            record_count=row.count,
        )
    )


@router.get(
    '/{catalog_id}',
    response_model=CatalogModel,
    dependencies=[Depends(Authorize(ODPScope.CATALOG_READ))],
)
async def get_catalog(
        catalog_id: str,
):
    stmt = (
        select(Catalog, func.count(CatalogRecord.catalog_id)).
        outerjoin(CatalogRecord, and_(Catalog.id == CatalogRecord.catalog_id, CatalogRecord.published)).
        group_by(Catalog).
        where(Catalog.id == catalog_id)
    )

    if not (result := Session.execute(stmt).one_or_none()):
        raise HTTPException(HTTP_404_NOT_FOUND)

    return CatalogModel(
        id=result.Catalog.id,
        url=result.Catalog.url,
        record_count=result.count,
    )


@router.get(
    '/{catalog_id}/records',
    response_model=Page[PublishedSAEONRecordModel | PublishedDataCiteRecordModel | RetractedRecordModel],
    dependencies=[Depends(Authorize(ODPScope.CATALOG_READ))],
)
async def list_records(
        catalog_id: str,
        include_nonsearchable: bool = False,
        include_retracted: bool = False,
        paginator: Paginator = Depends(partial(Paginator, sort='record_id')),
):
    if not Session.get(Catalog, catalog_id):
        raise HTTPException(HTTP_404_NOT_FOUND)

    stmt = (
        select(CatalogRecord)
        .where(CatalogRecord.catalog_id == catalog_id)
    )

    if not include_nonsearchable:
        stmt = stmt.where(or_(CatalogRecord.searchable == None, CatalogRecord.searchable))

    if include_retracted:
        stmt = stmt.join(PublishedRecord, CatalogRecord.record_id == PublishedRecord.id)
    else:
        stmt = stmt.where(CatalogRecord.published)

    return paginator.paginate(
        stmt,
        lambda row: output_published_record_model(row.CatalogRecord) if row.CatalogRecord.published
        else RetractedRecordModel(id=row.CatalogRecord.record_id),
    )


@router.get(
    '/{catalog_id}/search',
    response_model=Page[PublishedSAEONRecordModel],
    dependencies=[Depends(Authorize(ODPScope.CATALOG_READ))],
)
async def search_records(
        catalog_id: str,
        paginator: Paginator = Depends(partial(Paginator, sort='record_id')),
        text_query: str = Query(None, title='Search terms'),
        north_bound: float = Query(None, title='North bound latitude', ge=-90, le=90),
        south_bound: float = Query(None, title='South bound latitude', ge=-90, le=90),
        east_bound: float = Query(None, title='East bound longitude', ge=-180, le=180),
        west_bound: float = Query(None, title='West bound longitude', ge=-180, le=180),
        start_date: date = Query(None, title='Date range start'),
        end_date: date = Query(None, title='Date range end'),
        exclusive_region: bool = Query(False, title='Exclude partial spatial matches'),
        exclusive_interval: bool = Query(False, title='Exclude partial temporal matches'),
        include_nonsearchable: bool = False,
):
    if not Session.get(Catalog, catalog_id):
        raise HTTPException(HTTP_404_NOT_FOUND)

    stmt = (
        select(CatalogRecord)
        .where(CatalogRecord.catalog_id == catalog_id)
        .where(CatalogRecord.published)
    )

    if text_query and (text_query := text_query.strip()):
        stmt = stmt.where(text(
            "full_text @@ plainto_tsquery('english', :text_query)"
        ).bindparams(text_query=text_query))

    if exclusive_region:
        if north_bound is not None:
            stmt = stmt.where(CatalogRecord.spatial_north <= north_bound)

        if south_bound is not None:
            stmt = stmt.where(CatalogRecord.spatial_south >= south_bound)

        if east_bound is not None:
            stmt = stmt.where(CatalogRecord.spatial_east <= east_bound)

        if west_bound is not None:
            stmt = stmt.where(CatalogRecord.spatial_west >= west_bound)

    else:
        if north_bound is not None:
            stmt = stmt.where(CatalogRecord.spatial_south <= north_bound)

        if south_bound is not None:
            stmt = stmt.where(CatalogRecord.spatial_north >= south_bound)

        if east_bound is not None:
            stmt = stmt.where(CatalogRecord.spatial_west <= east_bound)

        if west_bound is not None:
            stmt = stmt.where(CatalogRecord.spatial_east >= west_bound)

    if exclusive_interval:
        if start_date:
            stmt = stmt.where(CatalogRecord.temporal_start >= start_date)

        if end_date:
            # if the record has only a start date, it is taken to be its end date too
            stmt = stmt.where(func.coalesce(CatalogRecord.temporal_end,
                                            CatalogRecord.temporal_start) <= end_date)

    else:
        if start_date:
            # if the record has only a start date, it is taken to be its end date too
            stmt = stmt.where(func.coalesce(CatalogRecord.temporal_end,
                                            CatalogRecord.temporal_start) >= start_date)

        if end_date:
            stmt = stmt.where(CatalogRecord.temporal_start <= end_date)

    if not include_nonsearchable:
        stmt = stmt.where(CatalogRecord.searchable)

    return paginator.paginate(
        stmt,
        lambda row: output_published_record_model(row.CatalogRecord),
    )


@router.get(
    '/{catalog_id}/records/{record_id:path}',
    response_model=PublishedSAEONRecordModel | PublishedDataCiteRecordModel,
    dependencies=[Depends(Authorize(ODPScope.CATALOG_READ))],
)
async def get_published_record(
        catalog_id: str,
        record_id: str = Path(..., title='UUID or DOI'),
):
    stmt = (
        select(CatalogRecord).
        where(CatalogRecord.catalog_id == catalog_id).
        where(CatalogRecord.published)
    )

    try:
        UUID(record_id, version=4)
        stmt = stmt.where(CatalogRecord.record_id == record_id)

    except ValueError:
        if re.match(DOI_REGEX, record_id):
            stmt = stmt.where(CatalogRecord.published_record.comparator.contains({
                'doi': record_id
            }))
        else:
            raise HTTPException(HTTP_422_UNPROCESSABLE_ENTITY, 'Invalid record identifier: expecting a UUID or DOI')

    if not (catalog_record := Session.execute(stmt).scalar_one_or_none()):
        raise HTTPException(HTTP_404_NOT_FOUND)

    return output_published_record_model(catalog_record)


@router.get(
    '/{catalog_id}/external/{record_id}',
    response_model=Optional[dict[str, Any]],
    dependencies=[Depends(Authorize(ODPScope.CATALOG_READ))],
)
async def get_external_published_record(
        catalog_id: str,
        record_id: str,
        datacite: DataciteClient = Depends(get_datacite_client),
):
    if not Session.get(Catalog, catalog_id):
        raise HTTPException(HTTP_404_NOT_FOUND)

    if catalog_id == ODPCatalog.DATACITE:
        stmt = (
            select(CatalogRecord).
            where(CatalogRecord.catalog_id == catalog_id).
            where(CatalogRecord.record_id == record_id).
            where(CatalogRecord.published)
        )

        if not (catalog_record := Session.execute(stmt).scalar_one_or_none()):
            raise HTTPException(HTTP_404_NOT_FOUND)

        try:
            return datacite.get_doi(catalog_record.record.doi)
        except DataciteError as e:
            raise HTTPException(e.status_code, e.error_detail) from e

    raise HTTPException(HTTP_422_UNPROCESSABLE_ENTITY, 'Not an external catalog')


@router.get(
    '/{catalog_id}/go/{record_id_or_doi:path}',
    description='Redirect to the web page for a catalog record.',
)
async def redirect_to(
        catalog_id: str,
        record_id_or_doi: UUID4 | constr(regex=DOI_REGEX),
):
    if not (catalog := Session.get(Catalog, catalog_id)):
        raise HTTPException(HTTP_404_NOT_FOUND)

    return RedirectResponse(f'{catalog.url}/{record_id_or_doi}')
