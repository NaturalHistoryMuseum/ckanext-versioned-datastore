from unittest.mock import MagicMock, patch, call, Mock

import pytest
from ckanext.versioned_datastore.lib.downloads.download import send_email

site_url = 'https://data.nhm.ac.uk'


class MockPlugin:
    def __init__(self):
        self.plain_template = 'plain {{ some_var }} template'
        self.html_template = 'html {{ some_var }} template'
        self.some_var = 'beans!'
        self.rendered_plain_template = 'plain beans! template'
        self.rendered_html_template = 'html beans! template'

    def download_modify_email_templates(self, plain_template, html_template):
        return self.plain_template, self.html_template

    def download_modify_email_template_context(self, request, context):
        context['some_var'] = self.some_var
        return context


@pytest.mark.ckan_config('ckan.site_url', site_url)
@patch('ckanext.versioned_datastore.lib.downloads.download.mailer')
class TestSendEmail:
    def test_defaults(self, mock_mailer):
        request = MagicMock(email_address='user@test.com')
        zip_name = 'test.zip'

        send_email(request, zip_name)

        assert mock_mailer.mail_recipient.called
        args, kwargs = mock_mailer.mail_recipient.call_args
        assert kwargs['recipient_email'] == request.email_address
        assert kwargs['recipient_name'] == 'Downloader'
        assert kwargs['subject'] == 'Data download'
        assert zip_name in kwargs['body']
        assert zip_name in kwargs['body_html']
        assert site_url in kwargs['body']
        assert site_url in kwargs['body_html']

    def test_overrides(self, mock_mailer):
        request = MagicMock(email_address='user@test.com')
        zip_name = 'test.zip'

        mock_plugin = MockPlugin()
        mock_plugin_implementations = MagicMock(return_value=[mock_plugin])
        with patch(
            'ckanext.versioned_datastore.lib.downloads.download.PluginImplementations',
            mock_plugin_implementations,
        ):
            send_email(request, zip_name)

        assert mock_mailer.mail_recipient.called
        assert mock_mailer.mail_recipient.call_args == call(
            recipient_email=request.email_address,
            recipient_name='Downloader',
            subject='Data download',
            body=mock_plugin.rendered_plain_template,
            body_html=mock_plugin.rendered_html_template,
        )

    def test_default_context(self, mock_mailer):
        request = MagicMock(email_address='user@test.com')
        zip_name = 'test.zip'

        mock_plugin = Mock(wraps=MockPlugin())
        mock_plugin_implementations = MagicMock(return_value=[mock_plugin])
        with patch(
            'ckanext.versioned_datastore.lib.downloads.download.PluginImplementations',
            mock_plugin_implementations,
        ):
            send_email(request, zip_name)

        assert mock_mailer.mail_recipient.called
        assert mock_plugin.download_modify_email_template_context.called
        args, kwargs = mock_plugin.download_modify_email_template_context.call_args
        req, ctx = args
        assert req == request
        # defaults
        assert 'site_url' in ctx
        assert 'download_url' in ctx
        # our extra one
        assert 'some_var' in ctx
