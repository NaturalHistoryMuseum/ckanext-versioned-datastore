import pytest
from mock import MagicMock, patch
from tests.helpers import patches

from ckanext.versioned_datastore.lib.downloads.notifiers import (
    EmailNotifier,
    NullNotifier,
    WebhookNotifier,
)

notifiers = [
    (EmailNotifier, {'emails': ['data@nhm.ac.uk']}),
    (NullNotifier, {}),
    (WebhookNotifier, {'url': 'http://localhost'}),
]


@pytest.mark.parametrize('notifier_type,notifier_args', notifiers)
class TestNotifierGetText:
    def test_notifier_start_text(self, notifier_type, notifier_args):
        notifier = notifier_type(MagicMock(), **notifier_args)

        with patches.url_for():
            start_text = notifier.start_text()

        assert isinstance(start_text, tuple)
        assert len(start_text) == 2
        for t in start_text:
            assert '/banana' in t

    def test_notifier_end_text(self, notifier_type, notifier_args):
        notifier = notifier_type(MagicMock(), **notifier_args)

        with patches.url_for():
            end_text = notifier.end_text('/download-url-here')

        assert isinstance(end_text, tuple)
        assert len(end_text) == 2
        for t in end_text:
            assert '/download-url-here' in t

    def test_notifier_error_text(self, notifier_type, notifier_args):
        notifier = notifier_type(MagicMock(), **notifier_args)

        with patches.url_for():
            error_text = notifier.error_text()

        assert isinstance(error_text, tuple)
        assert len(error_text) == 2
        for t in error_text:
            assert '/banana' in t


@patch('ckanext.versioned_datastore.lib.downloads.notifiers.email.mailer')
class TestEmailNotifier:
    def test_email_notifier_start(self, mock_mailer):
        test_email_address = 'test@email.address'
        notifier = EmailNotifier(MagicMock(), emails=[test_email_address])

        with patches.url_for():
            start_text = notifier.start_text()
            notifier.notify_start()

        assert mock_mailer.mail_recipient.called
        args, kwargs = mock_mailer.mail_recipient.call_args
        assert kwargs['recipient_email'] == test_email_address
        assert kwargs['recipient_name'] == 'Downloader'
        assert kwargs['subject'] == 'Data download started'
        assert start_text[0] in kwargs['body']
        assert start_text[1] in kwargs['body_html']

    def test_email_notifier_end(self, mock_mailer):
        test_email_address = 'test@email.address'
        test_download_url = '/download-url-here'
        notifier = EmailNotifier(MagicMock(), emails=[test_email_address])

        with patches.url_for():
            end_text = notifier.end_text(test_download_url)
            notifier.notify_end(test_download_url)

        assert mock_mailer.mail_recipient.called
        args, kwargs = mock_mailer.mail_recipient.call_args
        assert kwargs['recipient_email'] == test_email_address
        assert kwargs['recipient_name'] == 'Downloader'
        assert kwargs['subject'] == 'Data download complete'
        assert end_text[0] in kwargs['body']
        assert end_text[1] in kwargs['body_html']

    def test_email_notifier_error(self, mock_mailer):
        test_email_address = 'test@email.address'
        notifier = EmailNotifier(MagicMock(), emails=[test_email_address])

        with patches.url_for():
            error_text = notifier.error_text()
            notifier.notify_error()

        assert mock_mailer.mail_recipient.called
        args, kwargs = mock_mailer.mail_recipient.call_args
        assert kwargs['recipient_email'] == test_email_address
        assert kwargs['recipient_name'] == 'Downloader'
        assert kwargs['subject'] == 'Data download failed'
        assert error_text[0] in kwargs['body']
        assert error_text[1] in kwargs['body_html']

    def test_email_notifier_multiple_emails(self, mock_mailer):
        test_email_addresses = ['one@email.address', 'two@email.address']
        notifier = EmailNotifier(MagicMock(), emails=test_email_addresses)

        with patches.url_for():
            start_text = notifier.start_text()
            notifier.notify_start()

        assert mock_mailer.mail_recipient.call_count == len(test_email_addresses)
        _, recipient_one = mock_mailer.mail_recipient.call_args_list[0]
        _, recipient_two = mock_mailer.mail_recipient.call_args_list[1]
        for k, v in recipient_one.items():
            if k == 'recipient_email':
                continue
            assert v == recipient_two[k]
        assert recipient_one['recipient_email'] == test_email_addresses[0]
        assert recipient_two['recipient_email'] == test_email_addresses[1]


@patch('ckanext.versioned_datastore.lib.downloads.notifiers.webhook.requests')
class TestWebhookNotifier:
    def test_webhook_notifier_start(self, mock_requests):
        test_webhook_url = 'http://webhook-url'
        notifier = WebhookNotifier(MagicMock(), url=test_webhook_url)

        with patches.url_for():
            start_text = notifier.start_text()
            notifier.notify_start()

        assert mock_requests.request.called
        args, kwargs = mock_requests.request.call_args
        assert args[0] == 'GET'
        assert args[1] == test_webhook_url
        assert kwargs['params']['url'] == '/banana'
        assert kwargs['params']['text'] == start_text[0]

    def test_webhook_notifier_end(self, mock_requests):
        test_webhook_url = 'http://webhook-url'
        test_download_url = '/download-url-here'
        notifier = WebhookNotifier(MagicMock(), url=test_webhook_url)

        with patches.url_for():
            end_text = notifier.end_text(test_download_url)
            notifier.notify_end(test_download_url)

        assert mock_requests.request.called
        args, kwargs = mock_requests.request.call_args
        assert args[0] == 'GET'
        assert args[1] == test_webhook_url
        assert kwargs['params']['url'] == test_download_url
        assert kwargs['params']['text'] == end_text[0]

    def test_webhook_notifier_error(self, mock_requests):
        test_webhook_url = 'http://webhook-url'
        notifier = WebhookNotifier(MagicMock(), url=test_webhook_url)

        with patches.url_for():
            error_text = notifier.error_text()
            notifier.notify_error()

        assert mock_requests.request.called
        args, kwargs = mock_requests.request.call_args
        assert args[0] == 'GET'
        assert args[1] == test_webhook_url
        assert kwargs['params']['url'] == '/banana'
        assert kwargs['params']['text'] == error_text[0]

    def test_webhook_notifier_non_default_parameters(self, mock_requests):
        test_webhook_url = 'http://webhook-url'
        notifier = WebhookNotifier(
            MagicMock(),
            url=test_webhook_url,
            url_param='something-not-url',
            text_param='anything-but-text',
            post=True,
        )

        with patches.url_for():
            start_text = notifier.start_text()
            notifier.notify_start()

        assert mock_requests.request.called
        args, kwargs = mock_requests.request.call_args
        assert args[0] == 'POST'
        assert args[1] == test_webhook_url
        assert kwargs['params']['something-not-url'] == '/banana'
        assert kwargs['params']['anything-but-text'] == start_text[0]
        assert 'url' not in kwargs['params']
        assert 'text' not in kwargs['params']


@patch('ckanext.versioned_datastore.lib.downloads.notifiers.null.logger')
class TestNullNotifier:
    def test_null_notifier_start(self, mock_logger):
        notifier = NullNotifier(MagicMock())

        with patches.url_for():
            notifier.notify_start()

        assert mock_logger.debug.called
        args, kwargs = mock_logger.debug.call_args
        assert args[0] == 'Processing started.'

    def test_null_notifier_end(self, mock_logger):
        test_download_url = '/download-url-here'
        notifier = NullNotifier(MagicMock())

        with patches.url_for():
            notifier.notify_end(test_download_url)

        assert mock_logger.debug.called
        args, kwargs = mock_logger.debug.call_args
        assert args[0] == 'Processing ended.'

    def test_null_notifier_error(self, mock_logger):
        notifier = NullNotifier(MagicMock())

        with patches.url_for():
            notifier.notify_error()

        assert mock_logger.debug.called
        args, kwargs = mock_logger.debug.call_args
        assert args[0] == 'Processing failed.'


class ContextOverridePlugin:
    def download_modify_notifier_template_context(self, request, context):
        context['status_page'] = '/boops'
        return context

    def download_modify_notifier_start_templates(self, *templates):
        return templates

    def download_modify_notifier_end_templates(self, *templates):
        return templates

    def download_modify_notifier_error_templates(self, *templates):
        return templates


class TemplateOverridePlugin:
    def download_modify_notifier_template_context(self, request, context):
        return context

    def download_modify_notifier_start_templates(self, *templates):
        return 'plain start template', 'html start template'

    def download_modify_notifier_end_templates(self, *templates):
        return 'plain end template', 'html end template'

    def download_modify_notifier_error_templates(self, *templates):
        return 'plain error template', 'html error template'


class TestNotifierTemplateOverrides:
    def test_notifier_context_overrides(self):
        mock_plugin = ContextOverridePlugin()
        notifier = NullNotifier(MagicMock())

        with patch(
            'ckanext.versioned_datastore.lib.downloads.notifiers.base.PluginImplementations',
            return_value=[mock_plugin],
        ), patch('ckan.plugins.toolkit.url_for', return_value='/banana'):
            start_text = notifier.start_text()

        assert isinstance(start_text, tuple)
        assert len(start_text) == 2
        for t in start_text:
            assert '/banana' not in t
            assert '/boops' in t

    def test_notifier_start_text_overrides(self):
        mock_plugin = TemplateOverridePlugin()
        notifier = NullNotifier(MagicMock())

        with patch(
            'ckanext.versioned_datastore.lib.downloads.notifiers.base.PluginImplementations',
            return_value=[mock_plugin],
        ), patch('ckan.plugins.toolkit.url_for', return_value='/banana'):
            start_text = notifier.start_text()

        assert isinstance(start_text, tuple)
        assert len(start_text) == 2
        assert start_text[0] == 'plain start template'
        assert start_text[1] == 'html start template'

    def test_notifier_end_text_overrides(self):
        mock_plugin = TemplateOverridePlugin()
        notifier = NullNotifier(MagicMock())

        with patch(
            'ckanext.versioned_datastore.lib.downloads.notifiers.base.PluginImplementations',
            return_value=[mock_plugin],
        ), patch('ckan.plugins.toolkit.url_for', return_value='/banana'):
            end_text = notifier.end_text('/download-url-here')

        assert isinstance(end_text, tuple)
        assert len(end_text) == 2
        assert end_text[0] == 'plain end template'
        assert end_text[1] == 'html end template'

    def test_notifier_error_text_overrides(self):
        mock_plugin = TemplateOverridePlugin()
        notifier = NullNotifier(MagicMock())

        with patch(
            'ckanext.versioned_datastore.lib.downloads.notifiers.base.PluginImplementations',
            return_value=[mock_plugin],
        ), patch('ckan.plugins.toolkit.url_for', return_value='/banana'):
            error_text = notifier.error_text()

        assert isinstance(error_text, tuple)
        assert len(error_text) == 2
        assert error_text[0] == 'plain error template'
        assert error_text[1] == 'html error template'
