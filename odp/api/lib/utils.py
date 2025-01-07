from typing import Optional

from odp.api.models import PublishedDataCiteRecordModel, PublishedRecordModel, PublishedSAEONRecordModel
from odp.const import ODPCatalog
from odp.db.models import CatalogRecord


def output_published_record_model(catalog_record: CatalogRecord) -> Optional[PublishedRecordModel]:
    if not catalog_record.published:
        return None

    if catalog_record.catalog_id in (ODPCatalog.SAEON, ODPCatalog.MIMS):
        return PublishedSAEONRecordModel(**catalog_record.published_record | dict(
            keywords=catalog_record.keywords,
            spatial_north=catalog_record.spatial_north,
            spatial_east=catalog_record.spatial_east,
            spatial_south=catalog_record.spatial_south,
            spatial_west=catalog_record.spatial_west,
            temporal_start=catalog_record.temporal_start.isoformat() if catalog_record.temporal_start else None,
            temporal_end=catalog_record.temporal_end.isoformat() if catalog_record.temporal_end else None,
            searchable=catalog_record.searchable,
        ))

    if catalog_record.catalog_id == ODPCatalog.DATACITE:
        return PublishedDataCiteRecordModel(**catalog_record.published_record)
