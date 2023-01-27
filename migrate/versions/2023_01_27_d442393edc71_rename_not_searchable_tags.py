"""Rename not-searchable tags

Revision ID: d442393edc71
Revises: 8a115aab0278
Create Date: 2023-01-27 09:17:06.641859

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'd442393edc71'
down_revision = '8a115aab0278'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("delete from tag where id in ('Collection.NotIndexed', 'Record.NotIndexed')")


def downgrade():
    op.execute("delete from tag where id in ('Collection.NotSearchable', 'Record.NotSearchable')")
