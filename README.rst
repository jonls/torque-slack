Torque-Slack
============

A daemon to post Torque job updates to Slack. It uses the Torque logs
to detect when jobs are started and completed, and posts messages to Slack.
Currently, the accounting logs are used to detect which and these require root
permission to access.

Configuration
-------------

The configuration is read from a simple YAML file. See `config-example.yaml`_
for an example. To load the configuration run::

   $ torque-slack --config config.yaml

.. _config-example.yaml: config-example.yaml
