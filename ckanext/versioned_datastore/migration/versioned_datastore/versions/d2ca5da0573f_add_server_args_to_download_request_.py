"""
Add server_args to download request model.

Revision ID: d2ca5da0573f
Revises: 526b12c69d55
Create Date: 2023-06-28 13:28:33.607360
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB


# revision identifiers, used by Alembic.
revision = 'd2ca5da0573f'
down_revision = '526b12c69d55'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        'vds_download_request', sa.Column('server_args', JSONB, nullable=True)
    )


def downgrade():
    op.drop_column('vds_download_request', 'server_args')
