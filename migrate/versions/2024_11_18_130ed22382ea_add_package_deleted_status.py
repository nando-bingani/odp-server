"""Add package deleted status

Revision ID: 130ed22382ea
Revises: 734f70d67255
Create Date: 2024-11-18 14:50:48.751981

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '130ed22382ea'
down_revision = '734f70d67255'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("alter type packagestatus add value if not exists 'deleted'")


def downgrade():
    pass
