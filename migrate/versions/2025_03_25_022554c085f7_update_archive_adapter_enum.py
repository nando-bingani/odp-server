"""Update archive adapter enum

Revision ID: 022554c085f7
Revises: a11365fc0f5a
Create Date: 2025-03-25 17:38:03.821753

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '022554c085f7'
down_revision = 'a11365fc0f5a'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("alter type archiveadapter add value if not exists 'filestore'")


def downgrade():
    pass
