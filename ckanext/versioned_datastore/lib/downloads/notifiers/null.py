from logging import getLogger

from .base import BaseNotifier

logger = getLogger(__name__)


class NullNotifier(BaseNotifier):
    name = 'none'

    def notify_start(self):
        logger.debug('Processing started.')

    def notify_end(self, file_url):
        logger.debug('Processing ended.')

    def notify_error(self):
        logger.debug('Processing failed.')
