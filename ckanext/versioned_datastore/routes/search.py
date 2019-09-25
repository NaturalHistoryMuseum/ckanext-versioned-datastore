from ckan.plugins import toolkit
from flask import Blueprint


blueprint = Blueprint(name=u'search', import_name=__name__)


@blueprint.route(u'/search')
def view():
    return toolkit.render(u'search/search.html')
