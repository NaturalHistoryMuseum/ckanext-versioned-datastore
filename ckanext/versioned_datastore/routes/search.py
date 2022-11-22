import base64
import json
import zlib

from ckan.model import Session
from ckan.plugins import toolkit
from flask import Blueprint
from sqlalchemy import func

from ..lib.query import slugs
from ..lib.query.slug_words import list_one, list_two, list_three
from ..model.slugs import DatastoreSlug

blueprint = Blueprint(name='search', import_name=__name__)


@blueprint.route('/search')
@blueprint.route('/search/<path:slug>')
def view(slug=''):
    return toolkit.render('search/search.html', extra_vars={'slug': slug})


@blueprint.route('/slugerator')
def slug_generator():
    """
    A queryless slug generator.

    Because why not.
    :return:
    """
    extra_vars = {}
    slug_q = Session.query(DatastoreSlug)

    total_permutations = len(list_one) * len(list_two) * len(list_three)
    extra_vars['total'] = total_permutations

    pretty_slugs = slug_q.filter(DatastoreSlug.pretty_slug.isnot(None)).count()
    extra_vars['unreserved'] = pretty_slugs
    extra_vars['remaining'] = total_permutations - pretty_slugs
    extra_vars['percent_used'] = round((pretty_slugs / total_permutations) * 100, 2)

    reserved_slugs = slug_q.filter(
        DatastoreSlug.reserved_pretty_slug.isnot(None)
    ).count()
    extra_vars['reserved'] = reserved_slugs

    date_func = func.date_trunc('day', DatastoreSlug.created)
    q = Session.query(date_func.label('date'), func.count().label('count'))
    q = q.order_by(date_func).group_by(date_func)
    slug_stats = []
    total_slugs = 0
    for stat in q.all():
        total_slugs += stat.count
        formatted_date = stat.date.strftime('%Y-%m-%d')
        slug_stats.append([formatted_date, total_slugs])
    extra_vars['slug_stats'] = slug_stats
    # also compress the data and add to toolkit.c
    toolkit.c.date_interval = 'day'
    toolkit.c.graph_data = base64.b64encode(
        zlib.compress(json.dumps(slug_stats).encode(), level=9)
    )

    attempts = 0
    while attempts < 100:
        new_slug = slugs.generate_pretty_slug()
        if slugs.resolve_slug(new_slug) is None:
            extra_vars['new_slug'] = new_slug
        attempts += 1
        new_slug = None  # reset if it didn't work
    # default to words not on any list if not already set
    extra_vars['new_slug'] = extra_vars.get('new_slug', 'something-something-something')

    return toolkit.render('search/slugerator.html', extra_vars=extra_vars)
