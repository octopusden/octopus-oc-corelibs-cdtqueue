#!/usr/bin/env python
from setuptools import setup
from setuptools import find_packages
from sys import version_info

# NOTE: here we need tests also because many dependent packages uses them for their testing
included_packages = find_packages()

if version_info.major >= 3:
    python_requires = ">=3.6"
else:
    python_requires = ">=2.7,<3"

__version = "4.0.2"

spec = {
        "name": "oc-cdt-queue2",
        "version": __version,
        "description": "Code for Deliveries-related message queues management",
        "long_description": "",
        "long_description_content_type": "text/plain",
        "license": "Apache2.0",
        "install_requires": ["pika==1.1.0"],
        "packages": included_packages,
        "python_requires": python_requires
        }


setup(**spec)
