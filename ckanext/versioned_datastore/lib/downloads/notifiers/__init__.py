from .email import EmailNotifier
from .webhook import WebhookNotifier


notifiers = [EmailNotifier, WebhookNotifier]