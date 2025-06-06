"""
Remove version not null constraint.

Revision ID: 5932a36b7cf3
Revises: d2ca5da0573f
Create Date: 2024-09-30 18:55:43.328542
"""

from alembic import op

# revision identifiers, used by Alembic.
revision = '5932a36b7cf3'
down_revision = 'd2ca5da0573f'
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column('versioned_datastore_import_stats', 'version', nullable=True)


def downgrade():
    op.alter_column('versioned_datastore_import_stats', 'version', nullable=False)
