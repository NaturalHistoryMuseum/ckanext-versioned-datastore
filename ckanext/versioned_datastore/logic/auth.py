from ckan import logic, plugins


@plugins.toolkit.auth_allow_anonymous_access
def datastore_search(context, data_dict):
    return logic.check_access('resource_show', context, data_dict)


def datastore_create(context, data_dict):
    return logic.check_access('resource_update', context, data_dict)


def datastore_upsert(context, data_dict):
    return logic.check_access('resource_update', context, data_dict)


def datastore_delete(context, data_dict):
    return logic.check_access('resource_update', context, data_dict)


@plugins.toolkit.auth_allow_anonymous_access
def datastore_get_record_versions(context, data_dict):
    return logic.check_access('resource_show', context, data_dict)


@plugins.toolkit.auth_allow_anonymous_access
def datastore_autocomplete(context, data_dict):
    return logic.check_access('resource_show', context, data_dict)


def datastore_reindex(context, data_dict):
    return logic.check_access('resource_update', context, data_dict)
