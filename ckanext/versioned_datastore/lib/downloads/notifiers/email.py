from ckan.lib import mailer

from .base import BaseNotifier

default_body = '''
Hello,
{0}
Best Wishes,
The Download Bot
'''.strip()

default_html_body = '''
<html lang="en">
<body>
<p>Hello,</p>
{0}
<br />
<p>Best Wishes,</p>
<p>The Download Bot</p>
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
        body = default_body.format(content)
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
        body = default_body.format(content)
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
        body = default_body.format(content)
        body_html = default_html_body.format(content_html)

        for address in self.email_addresses:
            mailer.mail_recipient(
                recipient_email=address,
                recipient_name='Downloader',
                subject='Data download failed',
                body=body,
                body_html=body_html,
            )
