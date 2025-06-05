#!/usr/bin/env python
from setuptools import setup
from setuptools import find_packages

# NOTE: here we need tests also because many dependent packages uses them for their testing
included_packages = find_packages()

__version = "4.1.3"

spec = {
        "name": "oc-cdt-queue2",
        "version": __version,
        "description": "Code for Deliveries-related message queues management",
        "long_description": "",
        "long_description_content_type": "text/plain",
        "license": "Apache2.0",
        "install_requires": ["pika"],
        "packages": included_packages,
        "python_requires": ">=3.6"
        }


setup(**spec)
