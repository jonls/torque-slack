
"""Slack integration functions"""

import time
import urllib2
import json
from threading import Thread
from Queue import Queue
import logging

logger = logging.getLogger(__name__)


class AttachmentColor(object):
    """Predefined colors of attachments"""
    Good = 'good'
    Warning = 'warning'
    Danger = 'danger'


class Markup(unicode):
    """Markup-safe string that does not need escaping"""
    @classmethod
    def escape(cls, s):
        """Escape string into Markup object

        If already escape, the same object is returned.
        """
        if not isinstance(s, Markup):
            return Markup(s.replace('&', '&amp;').
                          replace('<', '&lt;').
                          replace('>', '&gt;'))
        return s

    def __repr__(self):
        return 'Markup(' + super(Markup, self).__repr__() + ')'


class Message(object):
    """Slack WebHook message"""
    def __init__(self, text=None, username=None,
                 channel=None, attachments=None,
                 mrkdwn=None):
        self.text = text
        self.username = username
        self.channel = channel
        self.attachments = attachments
        self.mrkdwn = mrkdwn

    def document(self):
        doc = {}
        if self.text is not None:
            doc['text'] = Markup.escape(self.text)
        if self.username is not None:
            doc['username'] = self.username
        if self.channel is not None:
            doc['channel'] = self.channel
        if self.attachments is not None:
            doc['attachments'] = [a.document() for a in
                                  self.attachments]
        if self.mrkdwn is not None:
            doc['mrkdwn'] = bool(self.mrkdwn)
        return doc


class Attachment(object):
    """Slack message attachment"""
    def __init__(self, fallback, color=None, pretext=None,
                 author=None, title=None, title_link=None,
                 text=None, image_url=None, mrkdwn_in=None):
        self.fallback = fallback
        self.color = color
        self.pretext = pretext
        self.author = author
        self.title = title
        self.title_link = title_link
        self.text = text
        self.image_url = image_url
        self.mrkdwn_in = mrkdwn_in

    def document(self):
        doc = {'fallback': self.fallback}
        if self.color is not None:
            doc['color'] = self.color
        if self.pretext is not None:
            doc['pretext'] = Markup.escape(self.pretext)
        if self.author is not None:
            doc['author_name'] = self.author.name
            if self.author.link is not None:
                doc['author_link'] = self.author.link
            if self.author.icon is not None:
                doc['author_icon'] = self.author.icon
        if self.title is not None:
            doc['title'] = Markup.escape(self.title)
        if self.title_link is not None:
            doc['title_link'] = self.title_link
        if self.text is not None:
            doc['text'] = Markup.escape(self.text)
        if self.image_url is not None:
            doc['image_url'] = self.image_url
        if self.mrkdwn_in is not None:
            doc['mrkdwn_in'] = self.mrkdwn_in
        return doc


class Author(object):
    """Slack message attachment author"""
    def __init__(self, name, link=None, icon=None):
        self.name = name
        self.link = link
        self.icon = icon


class SlackWebHook(Thread):
    """Threaded interface to WebHooks API"""

    def __init__(self, endpoint=None, min_post_delay=6.0):
        super(SlackWebHook, self).__init__()
        self._endpoint = endpoint
        self._min_post_delay = min_post_delay
        self._message_queue = Queue()
        self._running = True

    def enqueue(self, message):
        self._message_queue.put(message)

    def stop(self):
        self._running = False
        self._message_queue.put(None)

    def run(self):
        while self._running:
            message = self._message_queue.get()
            if message is None:
                continue

            logger.info('Posting message {}'.format(message.document()))

            # Post to endpoint
            data = json.dumps(message.document())
            req = urllib2.Request(self._endpoint, data,
                                  {'Content-Type': 'application/json'})
            try:
                f = urllib2.urlopen(req)
                response = f.read()
                f.close()
                retry_after = 0
            except urllib2.HTTPError as e:
                if e.code == 429:
                    retry_after = int(e.headers.get('Retry-After', '0'))
                else:
                    logger.warning('Error posting message!', exc_info=True)
                    retry_after = 120

            # Wait to avoid flooding
            wait_time = max(retry_after, self._min_post_delay)
            logger.info('Waiting for {} seconds'.format(wait_time))
            time.sleep(wait_time)
