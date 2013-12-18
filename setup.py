#!/usr/bin/env python
from setuptools import setup

setup(
    name='smr',
    version='0.0.1',
    description='SMR (Simple Map Reduce) is a simple tool for writing map-reduce jobs in python',
    long_description='SMR (Simple Map Reduce) is a simple tool for writing map-reduce jobs in python',
    author='Ivan Dyedov',
    author_email='ivan@dyedov.com',
    url='',
    packages=['smr'],
    install_requires=['boto>=2.20.1'],
    entry_points={
        'console_scripts': [
            'smr = smr.smr:main',
            'smr-map = smr.map:main',
            'smr-reduce = smr.reduce:main',
        ]
    },
)
