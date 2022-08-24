from ckan.plugins import toolkit
from ckan.model import Session
from flask import Blueprint
from ..model.slugs import DatastoreSlug
from ..lib.query import slug_words, slugs


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
    pretty_slugs = Session.query(DatastoreSlug).filter(DatastoreSlug.pretty_slug.isnot(None)).count()
    reserved_slugs = Session.query(DatastoreSlug).filter(DatastoreSlug.reserved_pretty_slug.isnot(None)).count()
    slug_combinations = len(slug_words.list_one) * len(slug_words.list_two) * len(slug_words.list_three)
    attempts = 0
    while attempts < 100:
        new_slug = slugs.generate_pretty_slug()
        if slugs.resolve_slug(new_slug) is None:
            break
        attempts += 1
        new_slug = None  # reset if it didn't work
    extra_vars = {
        'new_slug': new_slug or 'something-something-something',  # default to words not on any list
        'unreserved': pretty_slugs,
        'reserved': reserved_slugs,
        'total_combinations': slug_combinations,
        'remaining': slug_combinations - pretty_slugs,
        'percent_used': round((pretty_slugs / slug_combinations) * 100, 2)
    }
    return toolkit.render('search/slugerator.html', extra_vars=extra_vars)
