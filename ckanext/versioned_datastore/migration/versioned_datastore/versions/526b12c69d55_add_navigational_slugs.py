"""
Add navigational slugs.

Revision ID: 526b12c69d55
Revises: 19a61e5b669f
Create Date: 2023-06-07 16:25:59.090795
"""
from datetime import datetime as dt
from uuid import uuid4

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base

# revision identifiers, used by Alembic.
revision = '526b12c69d55'
down_revision = '19a61e5b669f'
branch_labels = None
depends_on = None

Base = declarative_base()


def make_uuid():
    return str(uuid4())


class NavigationalSlug(Base):
    __tablename__ = 'versioned_datastore_navigational_slugs'

    id = sa.Column(sa.UnicodeText, primary_key=True, default=make_uuid)
    query_hash = sa.Column(sa.UnicodeText, nullable=False, index=True, unique=True)
    query = sa.Column(JSONB, nullable=False)
    resource_ids_and_versions = sa.Column(JSONB, nullable=False, default=dict)
    created = sa.Column(sa.DateTime, nullable=False, default=dt.utcnow)


def upgrade():
    bind = op.get_bind()
    NavigationalSlug.__table__.create(bind)


def downgrade():
    bind = op.get_bind()
    NavigationalSlug.__table__.drop(bind)
