from abc import ABCMeta, abstractmethod
from jinja2 import Template

from ckan.plugins import toolkit, PluginImplementations
from ....interfaces import IVersionedDatastoreDownloads
from ....model.downloads import DownloadRequest


class BaseNotifier(metaclass=ABCMeta):
    name = 'base'

    default_start_text = '''
    Your download on {{ site_name }} has started processing.
    The status of your download can be viewed here: {{ status_page }}
    '''.strip()

    default_start_html = '''
    <p>Your download on <a href="{{ site_url }}">{{ site_name }}</a> has started processing.</p>
    <p>The status of your download can be viewed <a href="{{ status_page }}">here</a>.</p>
    '''.strip()

    default_end_text = '''
    The link to the resource data you requested on {{ site_url }} is available at {{ download_url }}.
    '''.strip()

    default_end_html = '''
    <p>The link to the resource data you requested on <a href="{{ site_url }}">{{ site_name }}</a>
    is available <a href="{{ download_url }}">here</a>.</p>
    '''.strip()

    default_error_text = '''
    Your download on {{ site_name }} has encountered an error and has stopped processing.
    More details can be viewed at: {{ status_page }}.
    Please try again later and contact us at {{ contact_email }} if the problem persists.
    '''.strip()

    default_error_html = '''
    <p>Your download on <a href="{{ site_url }}">{{ site_name }}</a> has encountered an error and
    has stopped processing.</p>
    <p>More details can be viewed <a href="{{ status_page }}">here</a>.</p>
    <p>Please try again later and contact us at <a href="mailto:{{ contact_email }}">{{ contact_email }}</a> if the problem persists.</p>
    '''.strip()

    def __init__(self, request, **type_args):
        self._request = request
        self.type_args = type_args

    @property
    def request(self):
        # this refreshes the joins if necessary
        if self._request is None:
            return
        try:
            self._request.core_record
        except:
            self._request = DownloadRequest.get(self._request.id)
        return self._request

    def template_context(self, file_url=None):
        context = {
            'site_url': toolkit.config.get('ckan.site_url'),
            'site_name': toolkit.config.get(
                'ckan.site_name', toolkit.config.get('ckan.site_url')
            ),
            'status_page': toolkit.url_for(
                'datastore_status.download_status',
                download_id=self.request.id,
                qualified=True,
            ),
            'download_url': file_url,
            'contact_email': toolkit.config.get('smtp.mail_from'),
        }
        for plugin in PluginImplementations(IVersionedDatastoreDownloads):
            context = plugin.download_modify_notifier_template_context(
                self.request, context
            )
        return context

    def _get_text(self, templates, interface_method_name, file_url=None):
        for plugin in PluginImplementations(IVersionedDatastoreDownloads):
            modify_templates = getattr(plugin, interface_method_name)
            templates = modify_templates(*templates)
        context = self.template_context(file_url)
        body, body_html = (
            Template(template).render(**context) for template in templates
        )
        return body, body_html

    def start_text(self):
        templates = (self.default_start_text, self.default_start_html)
        return self._get_text(templates, 'download_modify_notifier_start_templates')

    def end_text(self, file_url):
        templates = (self.default_end_text, self.default_end_html)
        return self._get_text(
            templates, 'download_modify_notifier_end_templates', file_url
        )

    def error_text(self):
        templates = (self.default_error_text, self.default_error_html)
        return self._get_text(templates, 'download_modify_notifier_error_templates')

    @abstractmethod
    def notify_start(self):
        raise NotImplemented

    @abstractmethod
    def notify_end(self, file_url):
        raise NotImplemented

    @abstractmethod
    def notify_error(self):
        raise NotImplemented

    @classmethod
    def validate_args(cls, type_args):
        return True
