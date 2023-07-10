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
import logging
import pprint
import time


def is_job_running(hook, workspace, feature_view, online, offline, allow_overwrite=False, start_time=None, end_time=None, job_type="batch"):
    job = hook.find_materialization_job(
        workspace=workspace,
        feature_view=feature_view,
        online=online,
        offline=offline,
        start_time=start_time,
        end_time=end_time,
        job_type=job_type,
    )
    if job:
        logging.info(f"Existing job found: {pprint.pformat(job)}")
        if job["state"].lower().endswith("running"):
            logging.info(f"Job already in running state; not triggering new job")
            return [job["id"]]
        elif job["state"].lower().endswith("success"):
            if allow_overwrite:
                logging.info(
                    "Overwriting existing job in success state; (allow_overwrite=True)"
                )
            else:
                logging.info(
                    f"Job already in success state; not triggering new job"
                )
                return [job["id"]]
        else:
            logging.info(f"Job in {job['state']} state; triggering new job")

    return None


def is_job_finished(hook, workspace, feature_view, online, offline, allow_overwrite=False, start_time=None, end_time=None, job_type="batch"):
    job = hook.find_materialization_job(
        workspace=workspace,
        feature_view=feature_view,
        online=online,
        offline=offline,
        start_time=start_time,
        end_time=end_time,
        job_type=job_type,
    )
    if job:
        logging.info(f"Existing job found: {pprint.pformat(job)}")
        if job["state"].lower().endswith("running"):
            logging.info("Job in running state; cancelling")
            hook.cancel_materialization_job(
                workspace=workspace,
                feature_view=feature_view,
                job_id=job["id"],
            )
            while (
                    not hook.get_materialization_job(
                        workspace=workspace,
                        feature_view=feature_view,
                        job_id=job["id"],
                    )["job"]["state"]
                    .lower()
                    .endswith("manually_cancelled")
            ):
                logging.info(f"waiting for job to enter state manually_cancelled")
                time.sleep(60)
        elif job["state"].lower().endswith("success"):
            if allow_overwrite:
                logging.info(
                    "Overwriting existing job in success state; (allow_overwrite=True)"
                )
            else:
                logging.info("Existing job in success state; exiting")
                return True
    return False


def check_job_status(hook, workspace, feature_view, job_id):
    job_result = hook.get_materialization_job(
        workspace, feature_view, job_id
    )["job"]
    while job_result["state"].upper().endswith("RUNNING"):
        if "attempts" in job_result:
            attempts = job_result["attempts"]
            latest_attempt = attempts[-1]
            logging.info(
                f"Latest attempt #{len(attempts)} in state {latest_attempt['state']} with URL {latest_attempt['run_url']}"
            )
        else:
            logging.info(f"No attempt launched yet")
        time.sleep(60)
        job_result = hook.get_materialization_job(
            workspace, feature_view, job_id
        )["job"]
    if job_result["state"].upper().endswith("SUCCESS"):
        return
    else:
        raise Exception(
            f"Final job state was {job_result['state']}. Final response:\n {job_result}"
        )


def kill_job(hook, workspace, feature_view, job_id):
    if job_id:
        logging.info(f"Attempting to kill job {job_id}")
        hook.cancel_materialization_job(
            workspace, feature_view, job_id
        )
        logging.info(f"Successfully killed job {job_id}")
    else:
        logging.debug(f"No job started; none to kill")
