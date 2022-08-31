from .base import BaseNotifier


class EmailNotifier(BaseNotifier):
    name = 'email'

    def notify(self, file_url):
        raise NotImplemented
