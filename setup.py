# Copyright 2022 Tecton, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="airflow-tecton",
    version="0.0.3",
    description="Provider for using Tecton with Airflow.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    entry_points={
        "apache_airflow_provider": [
            "provider_info=airflow_tecton.__init__:get_provider_info"
        ]
    },
    license="Apache License 2.0",
    packages=[
        "airflow_tecton",
        "airflow_tecton.hooks",
        "airflow_tecton.sensors",
        "airflow_tecton.operators",
    ],
    install_requires=["apache-airflow>=2.0",
                      "requests",
                      "pandas==1.3.5",
                      "pydantic==1.10.9",
                      "pydantic-core==0.39.0",
                      "pyarrow",
                      "fastparquet"],
    setup_requires=["setuptools", "wheel"],
    test_require=["requests_mock"],
    url="http://tecton.ai/",
    classifiers=[
        "Framework :: Apache Airflow",
        "Framework :: Apache Airflow :: Provider",
    ],
    python_requires=">=3.7",
)
