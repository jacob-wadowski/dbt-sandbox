import logging
import os
import json
import io
from datetime import datetime, timedelta
from google.cloud.bigquery.enums import CreateDisposition, WriteDisposition
import google.cloud.logging
from google.cloud import bigquery
from google.cloud import storage
from google.cloud import datastore

# Instantiates logging client
client = google.cloud.logging.Client()
cloud_logger = logging.getLogger("cloudLogger")
cloud_logger.setLevel(logging.INFO)  # defaults to WARN
handler = client.get_default_handler()
cloud_logger.addHandler(handler)

# Intialize global variables
GLOBAL_STORAGE_BUCKET = os.environ["GLOBAL_STORAGE_BUCKET"]

STORAGE_CLIENT = storage.Client()
BIG_QUERY_CLIENT = bigquery.Client()
DATASTORE_CLIENT = datastore.Client()


def start_file_load(request):
    """
    The request POST or GET should be in the format of
    {
        "job_name" <string>: "shopify_orders",
        "last_modified_pull_date" <datetime>: "{date_time_here}" (OPTIONAL),
        "backfill" <bool> : true/false (OPTIONAL)
    }

    It will then read from a config file that is stored in a storage account located in
    gs://{GLOBAL_STORAGE_BUCKET}/config/config.json
    """
    # Get the JSON request from the HTTP intitiation.
    request_json = request.get_json(silent=True)
    # Get the parameters from the POST request.
    job_name = request_json.get("job_name")
    if job_name is None:
        cloud_logger.error("Job name was not included in request json.")
        return "Please include job_name in request json."

    current_time = datetime.utcnow()

    # Get the config file.
    cloud_logger.debug("Grabbing config file for %s", job_name)
    config = get_config(job_name)

    # Grab the following parameters from the config file.
    # TODO Create a formatted dictionary from a function instead?
    blob_prefix = config["blob_prefix"]
    schema_file_location = config["schema_file_location"]
    storage_bucket = config["storage_bucket"]
    destination_table_id = config["destination_table_id"]
    file_type = config["file_type"]
    hive_partitioning = config.get("hive_partitioning")
    create_behavior = config.get("create_behavior")
    write_behavior = config.get("write_behavior")
    extra_kwargs = config.get("extra_kwargs", {})

    # Validate create and write behaviors
    if create_behavior is not None:
        try:
            getattr(CreateDisposition, create_behavior)
        except AttributeError as e:
            logging.error(
                "The value '%s' provided for CreateDisposition does not match any valid value.",
                create_behavior,
            )
            return e
    if write_behavior is not None:
        try:
            getattr(WriteDisposition, write_behavior)
        except AttributeError as e:
            cloud_logger.error(
                "The value '%s' provided for WriteDisposition does not match any valid value.",
                write_behavior,
            )
            return e

    # Get the latest entity for the job.
    entity = get_entity(job_name)

    # See if the pull has been run in the last five minutes
    last_attempted_grab: datetime = (
        entity.get("last_attempted_grab") if entity else None
    )

    # Get the latest date run and insert the current pull time.
    last_modified_time = get_last_modified_time(entity)

    # TODO If the latest date doesn't exist, figure out if the table exists
    # If the table exists, then for now we'll assume that the date may
    # have been erased somehow.
    if last_modified_time and last_attempted_grab:
        backfill_flag = request_json.get("backfill", False)
        if (
            last_attempted_grab.timestamp()
            > (current_time - timedelta(minutes=5)).timestamp()
        ):
            cloud_logger.warning(
                "The last date run is too close. Last date was %s", last_attempted_grab
            )
            return None
    else:
        backfill_flag = True

    set_entity_item(job_name, "last_attempted_grab", datetime.utcnow())

    # If backfill is True, then use a asterisk instead of grabbing each individual filename.
    if backfill_flag is True:
        blobs_to_load = f"gs://{storage_bucket}/{blob_prefix}*"
    else:
        blobs_to_load = list_blobs(storage_bucket, blob_prefix, last_modified_time)

    if len(blobs_to_load) == 0:
        cloud_logger.warning("There are no new blobs to load")
        return f"There are no new blobs to load for {job_name}"

    schema = get_schema(schema_file_location) if schema_file_location else None

    try:
        load_job_result = load_files_to_biquery(
            list_file_names=blobs_to_load,
            schema=schema,
            file_type=file_type,
            destination_table_id=destination_table_id,
            hive_partitioning=hive_partitioning,
            create_behavior=create_behavior,
            write_behavior=write_behavior,
            **extra_kwargs,
        )
        if backfill_flag is False:
            try:
                cloud_logger.info(
                    "Loaded in %s rows from %s files",
                    load_job_result.output_rows,
                    load_job_result.input_files,
                )
            except Exception as e:
                cloud_logger.warning("Could not output load job results")
                cloud_logger.warning(e)
        else:
            cloud_logger.debug("Backfill was initiated, no results being returned")

    except Exception as e:
        cloud_logger.error(e)
        return None

    set_entity_item(job_name, "last_modified_time", current_time)
    return f"Success for {job_name}"


def get_last_modified_time(entity) -> datetime:
    latest_date = None
    if entity is not None:
        latest_date = entity.get("last_modified_time")
    return latest_date


def get_entity(job_name):
    key = DATASTORE_CLIENT.key("data-pipelines", job_name)
    entity = DATASTORE_CLIENT.get(key=key)
    return entity


def set_entity_item(job_name, key, value):
    entity = get_entity(job_name)
    if entity is None:
        entity_key = DATASTORE_CLIENT.key("data-pipelines", job_name)
        entity = DATASTORE_CLIENT.entity(key=entity_key)
    entity.update({key: value})
    DATASTORE_CLIENT.put(entity)
    return True


def list_blobs(bucket_name: str, prefix: str, tsAfter: datetime) -> list:
    """
    Returns a list of blobs that were modified after the specified datetime.
    """
    list_of_blobs = []

    blobs = STORAGE_CLIENT.list_blobs(bucket_name, prefix=prefix, delimiter=None)

    for blob in blobs:
        # Check to see if the modified timestamp for the file is newer than the passed timestamp
        if int(blob.updated.timestamp()) > int(tsAfter.timestamp()):
            list_of_blobs.append(f"gs://{bucket_name}/{blob.name}")

    cloud_logger.debug("Grabbed %s files", len(list_of_blobs))

    return list_of_blobs


def get_config(job_name: str) -> dict:
    bucket = STORAGE_CLIENT.bucket(GLOBAL_STORAGE_BUCKET)
    blob = bucket.get_blob("config/config.json")
    # Check to see if the blob actually exists
    if blob is None:
        raise FileNotFoundError("Config file does not exist.")

    # Download the blob
    try:
        blob_string = blob.download_as_string()
    except:
        logging.error("Blob exists, but cannot be downloaded")
        raise

    # Grab the current job item from the config file.
    try:
        return_json = json.loads(blob_string).get(job_name)
    except TypeError:
        cloud_logger.error("Could not convert the config file to dict")
        raise
    except Exception as e:
        cloud_logger.error(e)
        raise

    if return_json is None:
        raise ValueError(f"{job_name} was not found in the config file.")
    return return_json


def get_schema(schema_file_location: str):
    bucket = STORAGE_CLIENT.bucket(GLOBAL_STORAGE_BUCKET)
    blob = bucket.get_blob(f"bq-schemas/{schema_file_location}")

    # Check to see if the blob actually exists
    if blob is None:
        raise FileNotFoundError("Schema file does not exist.")

    # Download the blob
    try:
        blob_bytes = blob.download_as_bytes()
    except:
        cloud_logger.error("Blob exists, but cannot be downloaded")
        raise

    try:
        return_schema = BIG_QUERY_CLIENT.schema_from_json(io.BytesIO(blob_bytes))
    except:
        cloud_logger.error(
            "Could not transform the schema into a usuable BigQuery schema"
        )
        raise

    return return_schema


def load_files_to_biquery(
    list_file_names: list,
    schema,
    file_type: str,
    destination_table_id: str,
    hive_partitioning: dict,
    create_behavior: str,
    write_behavior: str,
    **kwargs,
):
    client = bigquery.Client()

    if file_type == "json":
        source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    elif file_type == "csv":
        source_format = bigquery.SourceFormat.CSV
    else:
        raise ValueError("Please enter a valid file_type")

    # Create a hive partitioning object if it exists
    if hive_partitioning is not None:
        from google.cloud.bigquery.external_config import HivePartitioningOptions

        hive_partitioning = HivePartitioningOptions.from_api_repr(hive_partitioning)

    job_config = bigquery.LoadJobConfig(
        source_format=source_format,
        schema=schema,
        create_disposition=create_behavior,
        write_disposition=write_behavior,
        hive_partitioning=hive_partitioning,
        **kwargs,
    )

    load_job = client.load_table_from_uri(
        source_uris=list_file_names,
        destination=destination_table_id,
        # Must match the destination dataset location. We assume it's US for now.
        location="US",
        job_config=job_config,
    )  # Make an API request.

    # Waits for the job to complete.
    return load_job.result()
