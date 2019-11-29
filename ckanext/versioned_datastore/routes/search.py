from ckan.plugins import toolkit
from flask import Blueprint


blueprint = Blueprint(name=u'search', import_name=__name__)


@blueprint.route(u'/search')
@blueprint.route(u'/search/<slug>')
def view(slug=u''):
    return toolkit.render(u'search/search.html', extra_vars={u'slug': slug})
