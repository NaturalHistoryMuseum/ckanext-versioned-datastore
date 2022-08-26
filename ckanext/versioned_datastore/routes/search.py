from ckan.plugins import toolkit
from ckan.model import Session
from flask import Blueprint
from ..model.slugs import DatastoreSlug
from ..lib.query import slugs
from ..lib.query.slug_words import list_one, list_two, list_three


blueprint = Blueprint(name='search', import_name=__name__)


@blueprint.route('/search')
@blueprint.route('/search/<path:slug>')
def view(slug=''):
    return toolkit.render('search/search.html', extra_vars={'slug': slug})


@blueprint.route('/slugerator')
def slug_generator():
    '''
    A queryless slug generator. Because why not.
    :return:
    '''
    extra_vars = {}
    slug_q = Session.query(DatastoreSlug)

    total_permutations = len(list_one) * len(list_two) * len(list_three)
    extra_vars['total'] = total_permutations

    pretty_slugs = slug_q.filter(DatastoreSlug.pretty_slug.isnot(None)).count()
    extra_vars['unreserved'] = pretty_slugs
    extra_vars['remaining'] = total_permutations - pretty_slugs
    extra_vars['percent_used'] = round((pretty_slugs / total_permutations) * 100, 2)

    reserved_slugs = slug_q.filter(DatastoreSlug.reserved_pretty_slug.isnot(None)).count()
    extra_vars['reserved'] = reserved_slugs

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
