from ckan.plugins import PluginImplementations, toolkit
from ckanext.versioned_datastore.interfaces import IVersionedDatastoreDownloads

from . import derivatives, servers, notifiers, transforms
from functools import partial


def get_derivative_generator(derivative_name, *args, **kwargs):
    gens = {g.name: g for g in derivatives.derivatives}
    for plugin in PluginImplementations(IVersionedDatastoreDownloads):
        gens = plugin.download_derivative_generators(gens)
    derivative_class = gens.get(derivative_name)
    if derivative_class is None:
        raise toolkit.ObjectNotFound(
            f'{derivative_name} is not a registered derivative generator type.'
        )
    derivative = derivative_class(*args, **kwargs)
    return derivative


def get_file_server(server_name, *args, **kwargs):
    srvrs = {s.name: s for s in servers.servers}
    for plugin in PluginImplementations(IVersionedDatastoreDownloads):
        srvrs = plugin.download_file_servers(srvrs)
    server_class = srvrs.get(server_name)
    if server_class is None:
        raise toolkit.ObjectNotFound(
            f'{server_name} is not a registered file server type.'
        )
    server = server_class(*args, **kwargs)
    return server


def get_notifier(notifier_type, *args, **kwargs):
    ntfrs = {n.name: n for n in notifiers.notifiers}
    for plugin in PluginImplementations(IVersionedDatastoreDownloads):
        ntfrs = plugin.download_notifiers(ntfrs)
    notifier_class = ntfrs.get(notifier_type)
    if notifier_class is None:
        raise toolkit.ObjectNotFound(
            f'{notifier_type} is not a registered download notifier type.'
        )
    notifier = notifier_class(*args, **kwargs)
    return notifier


def get_transformation(transform_name, **kwargs):
    trns = {t.name: t for t in transforms.transforms}
    for plugin in PluginImplementations(IVersionedDatastoreDownloads):
        trns = plugin.download_data_transformations(trns)
    transform_class = trns.get(transform_name)
    if transform_class is None:
        raise toolkit.ObjectNotFound(
            f'{transform_name} is not a registered download data transformation function.'
        )
    return transform_class(**kwargs)
