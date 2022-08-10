<!--
Copyright 2022 Tecton, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->
# [preview] airflow-tecton

Contains an Apache Airflow provider that allows you to author Tecton workflows inside Airflow.

Two basic capabilities are supported:
1) Submitting materialization jobs
2) Waiting for Feature View/Feature Service data to materialize.

Note this package is in preview and it will not work with your Tecton installation unless enabled.

# Installation and Configuration

## Installation

You can install this package via `pip install airflow-tecton`. Note that this requires `apache-airflow>=2.0`.

For a deployed environment, add `airflow-tecton` to your `requirements.txt` or wherever you configure installed packages.

You can confirm a successful installation by running `airflow providers list`, which should show `airflow-tecton` in the list.

## Configuration

This provider uses operators that interface with Tecton's API, thus it requires you set up Airflow to connect to Tecton.

You can add a new connection by going to `Connections` under the `Admin` tab in the Airflow UI. From there, hit the `+` button and select `Tecton` in the connection type dropdown. From there, you can configure the connection name, Tecton URL, and Tecton API key. Note that the default connection name is `tecton_default`, so we recommend starting with this as a connection name to minimize configuration.

# Usage

This package contains Airflow operators to submit and monitor materialization.

## Materialization Job Submission

There are two methods available to submit materialization jobs:
1) [TectonTriggerOperator](./airflow_tecton/operators/tecton_trigger_operator.py): This triggers a materialization job for a Feature View. Tecton will retry any failing jobs automatically. Note that completion of this operator only means submission succeeded. To wait for completion, you must combine this with `TectonSensor`.
2) [TectonJobOperator](./airflow_tecton/operators/tecton_job_operator.py): This triggers a materialization job for a Feature View with no retries. Additionally, when this operator is terminated, it will make a best effort to clean up the execution of the materialization job. Using this operator allows you to use standard Airflow keyword arguments to configure retry behavior. Additionally, this operator is synchronous, meaning that when the operator has succeeded, the underlying job has succeeded.

Both of these require the following arguments:
1) workspace - the workspace name of the Feature View you intend to materialize
2) feature_view - the name of the Feature View you intend to materialize
3) online - whether the job should materialize to the online store. This requires that your FeatureView also has online materialization enabled.
4) offline - whether the job should materialize to the offline store. This requires that your FeatureView also has offline materialization enabled.

The time interval of the materialization job is configured automatically using Airflow templates. By default, it is from the `data_interval_start` to the `data_interval_end` of your DAG run. These can overridden if necessary.

#### Example Usage

```python
from airflow_tecton import TectonJobOperator, TectonTriggerOperator

TectonJobOperator(
    task_id="tecton_job",
    workspace="my_workspace",
    feature_view="my_fv",
    online=False,
    offline=True,
    retries=3
)


TectonTriggerOperator(
    task_id="trigger_tecton",
    workspace="my_workspace",
    feature_view="my_fv",
    online=True,
    offline=True,
)
```

### Compatibility with Feature Views
Note that these functions require you have your Feature View configured with `batch_trigger=BatchTriggerType.TRIGGERED`. Currently, conversion of Feature Views with scheduled materialization is not supported.

## Waiting For Materialization

### [TectonSensor](./airflow_tecton/sensors/tecton_sensor.py)

This enables you to wait for Materialization to complete for both Feature Views and Feature Services. Common uses are for monitoring as well as kicking off a training job after daily materialization completes.


#### Example Usage
```python
from airflow_tecton import TectonSensor

TectonSensor(
    task_id="wait_for_fs_online",
    workspace="my_workspace",
    feature_service="my_fs",
    online=True,
    offline=False,
)

TectonSensor(
    task_id="wait_for_fv",
    workspace="my_workspace",
    feature_view="my_fv",
    online=True,
    offline=True,
)
```

# Examples

See [example dags here](./airflow_tecton/example_dags).

# Development
## Pre-commit

This repo uses pre-commit. Run `pre-commit install` in the repo root to configure pre-commit hooks. Pre-commit hooks take care of running unit tests as well as linting files.

## Run unit tests manually

Run `python -m pytest tests/` in the repo root.

# License
This is licensed with the Apache 2.0 License.
