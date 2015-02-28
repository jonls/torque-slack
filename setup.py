#!/usr/bin/env python

from setuptools import setup

setup(
    name='torque-slack',
    version='0.1',
    license='GPLv3+',
    url='http://jonls.dk',
    author='Jon Lund Steffensen',
    author_email='jonlst@gmail.com',

    description='Post Torque job information to Slack',

    packages=['torque_slack'],
    scripts=['scripts/torque-slack'],
    install_requires=[
        'PyYAML',
        'pyinotify'
    ]
)
