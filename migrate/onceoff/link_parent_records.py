from sqlalchemy import select

from odp.api.routers.record import get_parent_id
from odp.db import Session
from odp.db.models import Record


def set_parent_ids():
    for record in Session.execute(
            select(Record)
    ).scalars().all():
        record.parent_id = get_parent_id(record.metadata_, record.schema_id)

    Session.commit()
