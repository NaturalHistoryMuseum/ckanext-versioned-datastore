from .base import BaseNotifier


class EmailNotifier(BaseNotifier):
    name = 'email'

    def notify(self, request):
        raise NotImplemented
