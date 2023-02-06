"""Distinguish form and auto logins

Revision ID: bfad1d7213e6
Revises: 2151728bbdde
Create Date: 2023-02-06 11:29:30.588561

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'bfad1d7213e6'
down_revision = '2151728bbdde'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("alter type identitycommand add value if not exists 'auto_login' after 'login'")


def downgrade():
    pass
