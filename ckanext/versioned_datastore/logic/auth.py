from ckan.plugins import toolkit


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
    data_dict_copy['id'] = data_dict['resource_id']

    user = context.get('user')
    authorized = toolkit.check_access(access, context, data_dict_copy)

    if authorized:
        return {'success': True}
    else:
        return {
            'success': False,
            'msg': toolkit._(f'User {user} not authorized to alter resource {data_dict_copy["id"]}')
        }


@toolkit.auth_allow_anonymous_access
def datastore_search(context, data_dict):
    return check('resource_show', context, data_dict)


def datastore_create(context, data_dict):
    return check('resource_update', context, data_dict)


def datastore_upsert(context, data_dict):
    return check('resource_update', context, data_dict)


def datastore_delete(context, data_dict):
    return check('resource_update', context, data_dict)


@toolkit.auth_allow_anonymous_access
def datastore_get_record_versions(context, data_dict):
    return check('resource_show', context, data_dict)


@toolkit.auth_allow_anonymous_access
def datastore_get_resource_versions(context, data_dict):
    return check('resource_show', context, data_dict)


@toolkit.auth_allow_anonymous_access
def datastore_autocomplete(context, data_dict):
    return check('resource_show', context, data_dict)


def datastore_reindex(context, data_dict):
    return check('resource_update', context, data_dict)


@toolkit.auth_allow_anonymous_access
def datastore_query_extent(context, data_dict):
    return check('resource_show', context, data_dict)


@toolkit.auth_allow_anonymous_access
def datastore_get_rounded_version(context, data_dict):
    return check('resource_show', context, data_dict)


@toolkit.auth_allow_anonymous_access
def datastore_search_raw(context, data_dict):
    return check('resource_show', context, data_dict)


@toolkit.auth_allow_anonymous_access
def datastore_ensure_privacy(context, data_dict):
    # allow access to everyone
    return {'success': True}


@toolkit.auth_allow_anonymous_access
def datastore_multisearch(context, data_dict):
    # allow access to everyone
    return {'success': True}


@toolkit.auth_allow_anonymous_access
def datastore_field_autocomplete(context, data_dict):
    # allow access to everyone
    return {'success': True}


@toolkit.auth_allow_anonymous_access
def datastore_value_autocomplete(context, data_dict):
    # allow access to everyone
    return {'success': True}


@toolkit.auth_allow_anonymous_access
def datastore_create_slug(context, data_dict):
    # allow access to everyone
    return {'success': True}


@toolkit.auth_allow_anonymous_access
def datastore_resolve_slug(context, data_dict):
    # allow access to everyone
    return {'success': True}


@toolkit.auth_allow_anonymous_access
def datastore_queue_download(context, data_dict):
    # allow access to everyone
    return {'success': True}


@toolkit.auth_allow_anonymous_access
def datastore_guess_fields(context, data_dict):
    # allow access to everyone
    return {'success': True}


@toolkit.auth_allow_anonymous_access
def datastore_hash_query(context, data_dict):
    # allow access to everyone
    return {'success': True}


@toolkit.auth_allow_anonymous_access
def datastore_is_datastore_resource(context, data_dict):
    # allow access to everyone
    return {'success': True}


@toolkit.auth_allow_anonymous_access
def datastore_get_latest_query_schema_version(context, data_dict):
    # allow access to everyone
    return {'success': True}


@toolkit.auth_allow_anonymous_access
def datastore_count(context, data_dict):
    # allow access to everyone
    return {'success': True}


@toolkit.auth_disallow_anonymous_access
def datastore_edit_slug(context, data_dict):
    # only allow logged-in users
    return {'success': True}
