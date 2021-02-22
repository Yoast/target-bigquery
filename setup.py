"""Setup."""
# -*- coding: utf-8 -*-
from setuptools import find_packages, setup

setup(
    name='target-bigquery',
    version='1.6.1',
    description='Singer.io target for writing data to Google BigQuery',
    author='Yoast',
    url='https://github.com/Yoast/target-bigquery',
    classifiers=['Programming Language :: Python :: 3 :: Only'],
    py_modules=['target_bigquery'],
    install_requires=[
        'google-api-python-client~=1.12.8',
        'google-cloud~=0.34.0',
        'google-cloud-bigquery~=2.8.0',
        'jsonschema~=2.6.0',
        'oauth2client~=4.1.3',
        'singer-python~=5.10.0',
    ],
    entry_points="""
        [console_scripts]
        target-bigquery=target_bigquery:main
      """,
    packages=find_packages(),
)
