from ckan.plugins import toolkit
from flask import Blueprint


blueprint = Blueprint(name='search', import_name=__name__)


@blueprint.route('/search')
@blueprint.route('/search/<path:slug>')
def view(slug=''):
    return toolkit.render('search/search.html', extra_vars={'slug': slug})
