import requests

from ckan.plugins import toolkit
from .base import BaseNotifier


class WebhookNotifier(BaseNotifier):
    name = 'webhook'

    def __init__(
        self, request, url, url_param='url', text_param='text', post=False, **kwargs
    ):
        self.url = url
        self.url_param = url_param
        self.text_param = text_param
        self.post = post
        super(WebhookNotifier, self).__init__(request, **kwargs)

    def notify_start(self):
        request_method = 'POST' if self.post else 'GET'
        text, _ = self.start_text()
        context = self.template_context()
        params = {self.url_param: context['status_page'], self.text_param: text}
        requests.request(request_method, self.url, params=params)

    def notify_end(self, file_url):
        request_method = 'POST' if self.post else 'GET'
        text, _ = self.end_text(file_url)
        params = {self.url_param: file_url, self.text_param: text}
        requests.request(request_method, self.url, params=params)

    def notify_error(self):
        request_method = 'POST' if self.post else 'GET'
        text, _ = self.error_text()
        context = self.template_context()
        params = {self.url_param: context['status_page'], self.text_param: text}
        requests.request(request_method, self.url, params=params)

    @classmethod
    def validate_args(cls, type_args):
        url = type_args.get('url')
        if not url:
            raise toolkit.Invalid('URL must be provided')
        return True
