from .base import BaseNotifier
import requests


class WebhookNotifier(BaseNotifier):
    name = 'webhook'

    def __init__(self, url, param_name, post=False, **kwargs):
        self.url = url
        self.param = param_name
        self.post = post
        super(WebhookNotifier, self).__init__(**kwargs)

    def notify(self, file_url):
        request_method = 'POST' if self.post else 'GET'
        requests.request(request_method, self.url, params={self.param or 'data': file_url})

