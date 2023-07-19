from .email import EmailNotifier
from .webhook import WebhookNotifier
from .null import NullNotifier
from ckan.plugins import toolkit

notifiers = [EmailNotifier, WebhookNotifier, NullNotifier]


def validate_notifier_args(notifier_type, notifier_type_args):
    try:
        notifier = next(n for n in notifiers if n.name == notifier_type)
    except StopIteration:
        raise toolkit.Invalid('Invalid notifier type.')

    return notifier.validate_args(notifier_type_args)
