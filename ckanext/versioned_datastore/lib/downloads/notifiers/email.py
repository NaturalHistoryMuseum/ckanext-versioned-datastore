from ckan.lib import mailer
from ckan.plugins import PluginImplementations, toolkit
from jinja2 import Template

from .base import BaseNotifier
from ....interfaces import IVersionedDatastoreDownloads

default_body = '''
Hello,
The link to the resource data you requested on {{ site_name }} is available at {{ download_url }}.
Best Wishes,
The Download Bot
'''.strip()

default_html_body = '''
<html lang="en">
<body>
<p>Hello,</p>
<p>The link to the resource data you requested on <a href="{{ site_url }}">{{ site_name }}</a> is
available at <a href="{{ download_url }}">here</a>.</p>
<br />
<p>Best Wishes,</p>
<p>The Download Bot</p>
</body>
</html>
'''.strip()


class EmailNotifier(BaseNotifier):
    name = 'email'

    def __init__(self, emails, **kwargs):
        self.email_addresses = emails if isinstance(emails, list) else [
            emails]
        super(EmailNotifier, self).__init__(**kwargs )

    def notify(self, file_url):
        templates = (default_body, default_html_body)
        for plugin in PluginImplementations(IVersionedDatastoreDownloads):
            templates = plugin.download_modify_email_templates(*templates)

        context = {
            'site_url': toolkit.config.get('ckan.site_url'),
            'site_name': toolkit.config.get('ckan.site_name', toolkit.config.get('ckan.site_url')),
            'download_url': file_url
        }

        body, body_html = (Template(template).render(**context) for template in templates)

        for address in self.email_addresses:
            mailer.mail_recipient(recipient_email=address, recipient_name='Downloader',
                                  subject='Data download', body=body, body_html=body_html)
