from ckan.plugins import toolkit
from flask import Blueprint


blueprint = Blueprint(name=u'search', import_name=__name__)


@blueprint.route(u'/search')
def view():
    slug = toolkit.config.get(u'search_slug', None)
    if slug is not None:
        del toolkit.config[u'search_slug']
    return toolkit.render(u'search/search.html', extra_vars={'slug': slug or ''})


@blueprint.route(u'/search/<slug>')
def view_slug(slug):
    toolkit.config[u'search_slug'] = slug
    return toolkit.redirect_to('search.view')