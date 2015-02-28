
"""Slack integration functions"""

import time
import urllib2
import json
from threading import Thread
from Queue import Queue
import logging

logger = logging.getLogger(__name__)

def escape(s):
    return s.replace('<', '&lt;').replace('>', '&gt;').replace('&', '&amp;')

def create_text_message(text):
    return {'text': escape(text)}

class SlackWebHook(Thread):
    """Threaded interface to WebHooks API"""

    def __init__(self, endpoint=None, min_post_delay=6.0):
        super(SlackWebHook, self).__init__()
        self._endpoint = endpoint
        self._min_post_delay = min_post_delay
        self._message_queue = Queue()
        self._running = True

    def format_message(event):
        job_id = event['job_id']
        job_name = event['job_name']
        user = event['user']
        state = event['state']

        if state == 'started':
            text = '{}: Job *{}* ({}) is now running!'.format(
                user, job_id, job_name)
        elif state == 'complete':
            text = '{}: Job *{}* ({}) has finished after {}!'.format(
                user, job_id, job_name, event['walltime'])
        else:
            text = '{}: Job *{}* ({}) is in state *{}*'.format(
                user, job_id, job_name, state)

        return slack_escape(text)

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

            logger.info('Posting message {}'.format(message))

            # Post to endpoint
            data = json.dumps(message)
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
                    raise

            # Wait to avoid flooding
            wait_time = max(retry_after, self._min_post_delay)
            logger.info('Waiting for {} seconds'.format(wait_time))
            time.sleep(wait_time)
