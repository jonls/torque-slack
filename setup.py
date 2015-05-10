#!/usr/bin/env python

from setuptools import setup

# Read long description
with open('README.rst') as f:
    long_description = f.read()

setup(
    name='torque-slack',
    version='0.2',
    license='MIT',
    url='http://jonls.dk',
    author='Jon Lund Steffensen',
    author_email='jonlst@gmail.com',

    description='Post Torque job information to Slack',
    long_description=long_description,

    packages=['torque_slack'],
    scripts=['scripts/torque-slack'],
    install_requires=[
        'PyYAML',
        'pyinotify'
    ]
)
