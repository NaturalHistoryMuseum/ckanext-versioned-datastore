from ckan import logic, plugins


def check(access, context, data_dict):
    '''
    Check that the current user has the given access in the given context. The resource id is
    extracted from the data dict and therefore must be present.

    :param access: the access required
    :param context: the context dict
    :param data_dict: the data dict
    :return: a dict containing a "success" key with a boolean value indicating whether the current
             user has the required access. If the user does not then an additional "msg" key is
             returned in this dict which contains a user-friendly message.
    '''
    data_dict_copy = data_dict.copy() if data_dict else {}
    data_dict_copy[u'id'] = data_dict[u'resource_id']

    user = context.get(u'user')
    authorized = logic.check_access(access, context, data_dict_copy)

    if authorized:
        return {u'success': True}
    else:
        return {
            u'success': False,
            u'msg': plugins.toolkit._(u'User {} not authorized to alter resource {}'.format(
                str(user), data_dict_copy[u'id']))
        }


@plugins.toolkit.auth_allow_anonymous_access
def datastore_search(context, data_dict):
    return check(u'resource_show', context, data_dict)


def datastore_create(context, data_dict):
    return check(u'resource_update', context, data_dict)


def datastore_upsert(context, data_dict):
    return check(u'resource_update', context, data_dict)


def datastore_delete(context, data_dict):
    return check(u'resource_update', context, data_dict)


@plugins.toolkit.auth_allow_anonymous_access
def datastore_get_record_versions(context, data_dict):
    return check(u'resource_show', context, data_dict)


@plugins.toolkit.auth_allow_anonymous_access
def datastore_get_resource_versions(context, data_dict):
    return check(u'resource_show', context, data_dict)


@plugins.toolkit.auth_allow_anonymous_access
def datastore_autocomplete(context, data_dict):
    return check(u'resource_show', context, data_dict)


def datastore_reindex(context, data_dict):
    return check(u'resource_update', context, data_dict)


@plugins.toolkit.auth_allow_anonymous_access
def datastore_query_extent(context, data_dict):
    return check(u'resource_show', context, data_dict)


@plugins.toolkit.auth_allow_anonymous_access
def datastore_get_rounded_version(context, data_dict):
    return check(u'resource_show', context, data_dict)
