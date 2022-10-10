import asyncio
from datetime import datetime, timezone, timedelta
import logging
import os
import google.cloud.logging
from google.cloud import tasks_v2
from google.protobuf import timestamp_pb2
import google.auth.transport.requests
import google.oauth2.id_token
import json
from functools import partial

# Instantiates logging client
client = google.cloud.logging.Client()
cloud_logger = logging.getLogger("cloudLogger")
cloud_logger.setLevel(logging.INFO)  # defaults to WARN
handler = client.get_default_handler()
cloud_logger.addHandler(handler)


# Intialize global variables
PROJECT = os.environ["PROJECT_ID"]
# Queue
QUEUE_LOCATION = os.environ["QUEUE_LOCATION"]
QUEUE_NAME = os.environ["QUEUE_NAME"]
task_url = f"https://us-central1-{PROJECT}.cloudfunctions.net/bq-load-file"
CONFIG_JOB_NAMES = os.environ["CONFIG_JOB_NAMES"]
PULL_DICT = [pull.strip() for pull in CONFIG_JOB_NAMES]

def bq_start_extraction(request):
    return asyncio.run(schedule_objects())


async def schedule_objects():
    """
    Schedule some async functions to kick off cloud functions to grab multiple endpoints and stores.
    """

    loop = asyncio.get_event_loop()

    order_task_values = [
        loop.run_in_executor(None, partial(add_object_task_to_queue, job_name))
        for job_name in PULL_DICT
    ]

    await asyncio.gather(*order_task_values)

    return "All object pulls queued"


def add_object_task_to_queue(job_name):
    """
    Function to create google cloud task
    :param endpoint_object(string): object name
    """
    if not job_name:
        cloud_logger.error(
            "add_object_task_to_queue function was passed an job_name of None type."
        )
        return None

    try:
        # Initialize cloud task client
        client = tasks_v2.CloudTasksClient()

        # Construct the fully qualified queue name.
        parent = client.queue_path(PROJECT, QUEUE_LOCATION, QUEUE_NAME)

        # Construct the request body.
        task = {
            "http_request": {  # Specify the type of request.
                "http_method": tasks_v2.HttpMethod.POST,
                "url": task_url,  # The full url path that the task will be sent to.
            }
        }

        # Construct task payload
        payload = {"job_name": job_name}
        payload = json.dumps(payload)

        # Retrieve service account credentials to generate token for authenticating task
        auth_req = google.auth.transport.requests.Request()
        id_token = google.oauth2.id_token.fetch_id_token(auth_req, task_url)
        auth_string = f"bearer {id_token}"
        task["http_request"]["headers"] = {
            "Content-type": "application/json",
            "Authorization": auth_string,
        }

        # The API expects a payload of type bytes
        converted_payload = payload.encode()
        # Add the payload to the request
        task["http_request"]["body"] = converted_payload

        # Add the timestamp to the tasks
        timestamp = timestamp_pb2.Timestamp()
        timestamp.FromDatetime(datetime.utcnow())
        task["schedule_time"] = timestamp

        # Use google cloud task client to build and send task
        response = client.create_task(request={"parent": parent, "task": task})
        cloud_logger.info(f"Successfully created task. Task name: {response.name}")
        return f"Success: Task created for BQ Job - {job_name}"
    except Exception as e:
        cloud_logger.error(f"Error adding task to queue. Error: {e}")
        return f"Failure: Task not created for BQ Job - {job_name}"
