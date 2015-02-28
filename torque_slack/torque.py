
import os
import re
import operator
from datetime import datetime
import logging

import pyinotify

logger = logging.getLogger(__name__)

DEFAULT_TORQUE_HOME = '/var/spool/torque'

class LogCollectorError(Exception):
    """Raised on errors on collecting log entries"""

class FilesWatcher(pyinotify.ProcessEvent):
    """Callback on created and modified files

    When a file is created, it is set to be the watched file.
    When a file is modified, the each new lines in the file is
    passed to the callback. If a file is modified that is not
    the currently watched file, an error is raised."""

    def __init__(self, callback):
        self._file = None
        self._filepath = None
        self._callback = callback
        self._buffer = ''

    def process_IN_CREATE(self, event):
        if self._file is not None:
            self._file.close()
            self._filepath = event.pathname
            self._file = open(event.pathname, 'r')
            self._buffer = ''

    def process_IN_MODIFY(self, event):
        if self._file is None:
            self._filepath = event.pathname
            self._file = open(event.pathname, 'r')
        elif self._filepath != event.pathname:
            raise LogCollectorError('Unexpected modifications to {}'.format(
                event.pathname))

        buffer = self._buffer + self._file.read()
        buffer, _, self._buffer = buffer.rpartition('\n')
        if buffer != '':
            for line in buffer.split('\n'):
                self._callback(line)

class TorqueLogCollector(object):
    def __init__(self, queue, torque_home=None):
        if torque_home is None:
            torque_home = os.environ.get('TORQUE_HOME', DEFAULT_TORQUE_HOME)

        logger.info('Collecting log messages from {}'.format(torque_home))
        self._torque_home = torque_home

        self._queue = queue

        # Start listening for server logs
        server_logs = os.path.join(self._torque_home, 'server_logs')
        self._server_notifier = self._directory_listen(server_logs,
                                                       self._server_cb)
        self._server_notifier.start()

        # Start listening for accounting logs
        acct_logs = os.path.join(self._torque_home, 'server_priv/accounting')
        self._acct_notifier = self._directory_listen(acct_logs,
                                                     self._acct_cb)
        self._acct_notifier.start()

    def stop(self):
        """Stop listening for new log entries"""
        logger.info('Stopping accounting logs notifier')
        self._acct_notifier.stop()

        logger.info('Stopping server logs notifier')
        self._server_notifier.stop()

    def _directory_listen(self, directory, callback):
        """Listen for log changes in a directory

        Returns a notifier thread that is started using the start()
        method. Each new log file line will be passed to the callback.
        The log entries of the last modified file will be replayed.
        """
        wm = pyinotify.WatchManager()
        notifier = pyinotify.ThreadedNotifier(wm, FilesWatcher(callback))
        mask = pyinotify.IN_CREATE | pyinotify.IN_MODIFY
        wdd = wm.add_watch(directory, mask, rec=True)

        def files_mtime(directory):
            for name in os.listdir(directory):
                path = os.path.join(directory, name)
                yield path, os.path.getmtime(path)

        recent = max(files_mtime(directory), key=operator.itemgetter(1))
        if len(recent) > 0:
            with open(recent[0], 'r') as f:
                for line in f:
                    callback(line.rstrip())

        return notifier

    def _parse_log_date(self, line):
        """Parse date of log entry

        Return date as a datetime object and the remaining log entry.
        """
        m = re.match(r'^(\d{2})/(\d{2})/(\d{4}) ' +
                     '(\d{2}):(\d{2}):(\d{2});(.*)$', line)
        if not m:
            raise LogCollectorError('Unable to match date on log message: {}'.format(line))

        # Parse time stamp
        month = int(m.group(1))
        day = int(m.group(2))
        year = int(m.group(3))

        hour = int(m.group(4))
        minute = int(m.group(5))
        second = int(m.group(6))

        dt = datetime(year=year, month=month, day=day,
                      hour=hour, minute=minute, second=second)

        return dt, m.group(7)

    def _server_cb(self, line):
        """Callback when a server log entry appears"""
        # Example:
        # 02/27/2015 00:59:44;0100;PBS_Server.23657;Job;22495[].clusterhn.cluster.com;enqueuing into default, state 1 hop 1

        dt, line = self._parse_log_date(line)
        log_type, server, section, about, message = line.split(';')

        event = {'log': 'server',
                 'timestamp': dt,
                 'type': log_type,
                 'server': server,
                 'section': section,
                 'about': about,
                 'message': message}

        self._queue.put(event)

    def _acct_cb(self, line):
        """Callback when an accounting log entry appears"""

        # Example:
        # 02/26/2015 00:04:48;Q;22320.clusterhn.cluster.com;queue=default

        dt, line = self._parse_log_date(line)
        state, job_id, properties = line.split(';')
        properties = dict(self._parse_properties(properties.rstrip()))

        event = {'log': 'accounting',
                 'timestamp': dt,
                 'job_id': job_id,
                 'state': state,
                 'properties': properties}

        self._queue.put(event)

    def _parse_properties(self, s):
        """"Parse list of properties separated by space"""
        for prop in s.split(' '):
            key, value = prop.split('=', 1)
            yield key, value
