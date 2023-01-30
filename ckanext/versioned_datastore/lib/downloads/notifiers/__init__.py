from .email import EmailNotifier
from .webhook import WebhookNotifier
from .null import NullNotifier


notifiers = [EmailNotifier, WebhookNotifier, NullNotifier]
