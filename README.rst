Torque-Slack
============

A daemon to post Torque job updates to Slack. It uses the Torque logs
to detect when jobs are started and completed, and posts messages accordingly
to Slack.

The accounting logs are used to detect which and these will normally require
root permission to access. The daemon is split into the log parser and the
Slack poster so that only the log parser needs to run with root permissions.

Configuration
-------------

The configuration is read from a simple YAML file. See `config-example.yaml`_
for an example. To load the configuration run::

   $ torque-log-parser --config config.yaml | \
       torque-slack-poster --config config.yaml

You may want to run the first process in the pipe with root privileges (e.g. by
starting it with sudo) and the second process as a less privileged user.

.. _config-example.yaml: config-example.yaml
