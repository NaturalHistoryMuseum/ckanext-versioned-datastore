import re

from ckan.lib import mailer
from ckan.plugins import toolkit
from .base import BaseNotifier

default_html_body = '''
<html lang="en">
<body>
{0}
</body>
</html>
'''.strip()


class EmailNotifier(BaseNotifier):
    name = 'email'

    def __init__(self, request, emails, **kwargs):
        self.email_addresses = emails if isinstance(emails, list) else [emails]
        super(EmailNotifier, self).__init__(request, **kwargs)

    def notify_start(self):
        content, content_html = self.start_text()
        body = content.strip()
        body_html = default_html_body.format(content_html)

        for address in self.email_addresses:
            mailer.mail_recipient(
                recipient_email=address,
                recipient_name='Downloader',
                subject='Data download started',
                body=body,
                body_html=body_html,
            )

    def notify_end(self, file_url):
        content, content_html = self.end_text(file_url)
        body = content.strip()
        body_html = default_html_body.format(content_html)

        for address in self.email_addresses:
            mailer.mail_recipient(
                recipient_email=address,
                recipient_name='Downloader',
                subject='Data download complete',
                body=body,
                body_html=body_html,
            )

    def notify_error(self):
        content, content_html = self.error_text()
        body = content.strip()
        body_html = default_html_body.format(content_html)

        for address in self.email_addresses:
            mailer.mail_recipient(
                recipient_email=address,
                recipient_name='Downloader',
                subject='Data download failed',
                body=body,
                body_html=body_html,
            )

    @classmethod
    def validate_args(cls, type_args):
        emails = type_args.get('emails')
        if not emails:
            raise toolkit.Invalid('Email address must be provided.')
        emails = emails if isinstance(emails, list) else [emails]
        email_rgx = re.compile(r'(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)')
        for e in emails:
            if not email_rgx.fullmatch(e):
                raise toolkit.Invalid('Email address appears to be invalid.')
        return True
