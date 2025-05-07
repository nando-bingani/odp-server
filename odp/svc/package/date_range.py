import copy
import logging
from datetime import date, datetime, timezone

from sqlalchemy import and_, select
from sqlalchemy.orm import aliased

from odp.const import ODPDateRangeIncType, ODPPackageTag
from odp.db import Session
from odp.db.models import Package, PackageTag
from odp.svc import ServiceModule

logger = logging.getLogger(__name__)


class DateRangeIncModule(ServiceModule):

    def exec(self):
        """
        Fetch and increment date of package date range tag according to it's related date range increment tag.
        """
        date_range_package_tag: PackageTag = aliased(PackageTag)
        date_range_inc_package_tag: PackageTag = aliased(PackageTag)

        stmt = (
            select(Package, date_range_package_tag, date_range_inc_package_tag)
            .join(
                date_range_package_tag,
                and_(
                    Package.id == date_range_package_tag.package_id,
                    date_range_package_tag.tag_id == ODPPackageTag.DATERANGE
                )
            )
            .join(
                date_range_inc_package_tag,
                and_(
                    Package.id == date_range_inc_package_tag.package_id,
                    date_range_inc_package_tag.tag_id == ODPPackageTag.DATERANGEINC
                )
            )
        )

        for (package, date_range_package_tag, date_range_inc_package_tag) in Session.execute(stmt).all():
            try:
                date_range_data = self._get_updated_date_range_data(date_range_package_tag.data,
                                                                    date_range_inc_package_tag)
                self._update_date_range_package_tag(date_range_package_tag, date_range_data, package)
            except Exception as e:
                logger.error(f'Date range increment failed for package {package.id}: {str(e)}')

    @staticmethod
    def _get_updated_date_range_data(date_range_package_tag_data, date_range_inc_package_tag) -> dict:
        date_range_data = copy.deepcopy(date_range_package_tag_data)
        # Iterate through the date range increment types and update the corresponding date range dates.
        for (date_type, increment_type) in date_range_inc_package_tag.data.items():

            match increment_type:
                case ODPDateRangeIncType.CURRENT_DATE:
                    date_range_data[date_type] = date.today().isoformat()
                case _:
                    raise ValueError("Invalid date range increment type")

        return date_range_data

    @staticmethod
    def _update_date_range_package_tag(date_range_package_tag, updated_data, package):
        # Save the changes and update the timestamps.
        date_range_package_tag.data = updated_data
        date_range_package_tag.timestamp = (timestamp := datetime.now(timezone.utc))
        date_range_package_tag.save()

        package.timestamp = timestamp
        package.save()
