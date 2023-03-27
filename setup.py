#!/usr/bin/env python
from setuptools import setup
from setuptools import find_packages

import unittest
from datetime import datetime


def my_test_suite():
    test_loader = unittest.TestLoader()
    test_suite = test_loader.discover('cdt_queue2')
    return test_suite

def dynamic_version(version):
    """
    Returns full version of a package
    :param str_version: string, package version to append
    :return: str_version with appended build_id
    """
    _bid = datetime.strftime(datetime.now(), "%Y%m%d%H%M00")  # Temporal hack to combat one second difference

    return '.'.join([version, _bid])


included_packages = find_packages()

MAJOR = 4
MINOR = 0
RELEASE = 1


setup(name="cdt_queue2",
      version=dynamic_version(
          ".".join(list(map(lambda x: str(x), [MAJOR, MINOR, RELEASE])))),
      description="Code for CDT message queue management",
      license="Apache2.0",
      install_requires=[
          "pika==1.1.0",
      ],
      packages=included_packages,
      package_data={},
      test_suite="setup.my_test_suite",
)
