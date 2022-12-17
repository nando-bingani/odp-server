"""Temporal search with timezone

Revision ID: 2f612ad23b6b
Revises: fd11fd3ad5e1
Create Date: 2022-12-17 19:09:16.297205

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '2f612ad23b6b'
down_revision = 'fd11fd3ad5e1'
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column('catalog_record', 'temporal_start', type_=sa.TIMESTAMP(timezone=True))
    op.alter_column('catalog_record', 'temporal_end', type_=sa.TIMESTAMP(timezone=True))


def downgrade():
    op.alter_column('catalog_record', 'temporal_start', type_=sa.DateTime)
    op.alter_column('catalog_record', 'temporal_end', type_=sa.DateTime)
