#!/usr/bin/env python
from setuptools import setup

setup(
    name='smr',
    version='0.0.2',
    description='SMR (Simple Map Reduce) is a simple tool for writing map-reduce jobs in python',
    long_description='SMR (Simple Map Reduce) is a simple tool for writing map-reduce jobs in python',
    author='Ivan Dyedov',
    author_email='ivan@dyedov.com',
    url='https://github.com/idyedov/smr',
    packages=['smr'],
    install_requires=['boto>=2.31.1', 'paramiko>=1.14.0', 'psutil>=2.1.1'],
    entry_points={
        'console_scripts': [
            'smr = smr.main:main',
            'smr-ec2 = smr.ec2:main',
            'smr-map = smr.map:main',
            'smr-reduce = smr.reduce:main',
        ]
    },
)
