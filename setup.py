"""Setup.py for the Tecton Airflow provider package."""

from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()

"""Perform the package airflow-provider-sample setup."""
setup(
    name='apache-airflow-providers-tecton',
    version="0.0.1",
    description='Official Airflow provider for integration with Tecton.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    entry_points={
        "apache_airflow_provider": [
            "provider_info=apache_airflow_providers_tecton.__init__:get_provider_info"
        ]
    },
    license='Apache License 2.0',
    packages=['apache_airflow_providers_tecton', 'apache_airflow_providers_tecton.hooks',
              'apache_airflow_providers_tecton.sensors', 'apache_airflow_providers_tecton.operators'],
    install_requires=['apache-airflow>=2.0', 'requests'],
    setup_requires=['setuptools', 'wheel'],
    test_require=['requests_mock'],
    url='http://tecton.ai/',
    classifiers=[
        "Framework :: Apache Airflow",
        "Framework :: Apache Airflow :: Provider",
    ],
    python_requires='~=3.7',
)
