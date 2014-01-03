#!/usr/bin/env python
from setuptools import setup
from smr import __version__

setup(
    name='smr',
    version=__version__,
    description='SMR (Simple Map Reduce) is a simple tool for writing map-reduce jobs in python',
    long_description='SMR (Simple Map Reduce) is a simple tool for writing map-reduce jobs in python',
    author='Ivan Dyedov',
    author_email='ivan@dyedov.com',
    url='',
    packages=['smr'],
    install_requires=['boto>=2.20.1', 'paramiko>=1.12.0'],
    entry_points={
        'console_scripts': [
            'smr = smr.main:main',
            'smr-ec2 = smr.ec2:main',
            'smr-map = smr.map:main',
            'smr-reduce = smr.reduce:main',
        ]
    },
)
