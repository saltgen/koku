#
# Copyright 2021 Red Hat Inc.
# SPDX-License-Identifier: Apache-2.0
#
"""Kafka message handler."""
import itertools
import json
import logging
import os
import re
import shutil
import tempfile
import threading
import time
import traceback
from pathlib import Path
from tarfile import ReadError
from tarfile import TarFile
from typing import Literal

import pandas as pd
import requests
from confluent_kafka import TopicPartition
from django.conf import settings
from django.db import connections
from django.db import DEFAULT_DB_ALIAS
from django.db import InterfaceError
from django.db import OperationalError
from kombu.exceptions import OperationalError as KombuOperationalError

from api.common import log_json
from api.provider.models import Sources
from kafka_utils.utils import extract_from_header
from kafka_utils.utils import get_consumer
from kafka_utils.utils import get_producer
from kafka_utils.utils import is_kafka_connected
from masu.config import Config
from masu.database.report_manifest_db_accessor import ReportManifestDBAccessor
from masu.external import UNCOMPRESSED
from masu.external.downloader.ocp.ocp_report_downloader import create_daily_archives
from masu.external.downloader.ocp.ocp_report_downloader import OCPReportDownloader
from masu.external.downloader.ocp.ocp_report_downloader import read_ocp_csv
from masu.external.ros_report_shipper import ROSReportShipper
from masu.processor import is_customer_large
from masu.processor._tasks.process import _process_report_file
from masu.processor.report_processor import ReportProcessorDBError
from masu.processor.report_processor import ReportProcessorError
from masu.processor.tasks import OCP_QUEUE
from masu.processor.tasks import OCP_QUEUE_XL
from masu.processor.tasks import record_all_manifest_files
from masu.processor.tasks import record_report_status
from masu.processor.tasks import summarize_reports
from masu.prometheus_stats import KAFKA_CONNECTION_ERRORS_COUNTER
from masu.util.ocp import common as utils


LOG = logging.getLogger(__name__)
SUCCESS_CONFIRM_STATUS = "success"
FAILURE_CONFIRM_STATUS = "failure"
MANIFEST_ACCESSOR = ReportManifestDBAccessor()


class KafkaMsgHandlerError(Exception):
    """Kafka msg handler error."""


def close_and_set_db_connection():  # pragma: no cover
    """Close the db connection and set to None."""
    if connections[DEFAULT_DB_ALIAS].connection:
        connections[DEFAULT_DB_ALIAS].connection.close()
    connections[DEFAULT_DB_ALIAS].connection = None


def delivery_callback(err, msg):
    """Acknowledge message success or failure."""
    if err is not None:
        LOG.error(f"Failed to deliver message: {msg}: {err}")
    else:
        LOG.info("Validation message delivered.")


def create_manifest_entries(report_meta: utils.OCPManifest):
    """
    Creates manifest database entries for report processing tracking.

    Args:
        report_meta (dict): Report context dictionary from extract_payload.
        request_id (String): Identifier associated with the payload
        context (Dict): Context for logging (account, etc)

    Returns:
        manifest_id (Integer): Manifest identifier of the created db entry.

    """

    downloader = OCPReportDownloader(
        report_meta.schema_name,
        report_meta.cluster_id,
        None,
        provider_uuid=report_meta.provider_uuid,
        request_id=report_meta.request_id,
        account=report_meta.account_id,
    )
    return downloader._prepare_db_manifest_record(report_meta)


def get_account_from_cluster_id(cluster_id, request_id, context):
    """
    Returns the provider details for a given OCP cluster id.

    Args:
        cluster_id (String): Cluster UUID.
        request_id (String): Identifier associated with the payload manifest
        context (Dict): Context for logging (account, etc)

    Returns:
        (dict) - keys: value
                 authentication: String,
                 customer_name: String,
                 billing_source: String,
                 provider_type: String,
                 schema_name: String,
                 provider_uuid: String

    """
    account = None
    if provider := utils.get_provider_from_cluster_id(cluster_id):
        context |= {"provider_uuid": provider.uuid, "cluster_id": cluster_id}
        LOG.info(log_json(request_id, msg="found provider for cluster-id", context=context))
        account = provider.account
    return account


def download_payload(request_id, url, context):
    """
    Download the payload from ingress to temporary location.

        Args:
        request_id (String): Identifier associated with the payload
        url (String): URL path to payload in the Insights upload service..
        context (Dict): Context for logging (account, etc)

        Returns:
        Tuple: temp_file (os.PathLike)
    """
    # Create temporary directory for initial file staging and verification in the
    # OpenShift PVC directory so that any failures can be triaged in the event
    # the pod goes down.
    os.makedirs(Config.DATA_DIR, exist_ok=True)
    temp_dir = tempfile.mkdtemp(dir=Config.DATA_DIR)

    # Download file from quarantine bucket as tar.gz
    try:
        download_response = requests.get(url)
        download_response.raise_for_status()
    except requests.exceptions.HTTPError as err:
        shutil.rmtree(temp_dir)
        msg = f"Unable to download file. Error: {str(err)}"
        LOG.warning(log_json(request_id, msg=msg), exc_info=err)
        raise KafkaMsgHandlerError(msg) from err

    sanitized_request_id = re.sub("[^A-Za-z0-9]+", "", request_id)
    temp_file = Path(temp_dir, sanitized_request_id).with_suffix(".tar.gz")
    try:
        temp_file.write_bytes(download_response.content)
    except OSError as error:
        shutil.rmtree(temp_dir)
        msg = f"Unable to write file. Error: {str(error)}"
        LOG.warning(log_json(request_id, msg=msg, context=context), exc_info=error)
        raise KafkaMsgHandlerError(msg) from error

    return temp_file


def extract_payload_contents(request_id, tarball_path, context):
    """
    Extract the payload contents into a temporary location.

        Args:
        request_id (String): Identifier associated with the payload
        tarball_path (os.PathLike): the path to the payload file to extract
        context (Dict): Context for logging (account, etc)

        Returns:
            (String): path to manifest file
    """
    # Extract tarball into temp directory

    if not os.path.isfile(tarball_path):
        msg = f"Unable to find tar file {tarball_path}."
        LOG.warning(log_json(request_id, msg=msg, context=context))
        raise KafkaMsgHandlerError("Extraction failure, file not found.")

    try:
        mytar = TarFile.open(tarball_path, mode="r:gz")
        mytar.extractall(path=tarball_path.parent)
        files = mytar.getnames()
        manifest_path = [manifest for manifest in files if "manifest.json" in manifest]
    except (ReadError, EOFError, OSError) as error:
        msg = f"Unable to untar file {tarball_path}. Reason: {str(error)}"
        LOG.warning(log_json(request_id, msg=msg, context=context))
        shutil.rmtree(tarball_path.parent)
        raise KafkaMsgHandlerError("Extraction failure.")

    if not manifest_path:
        msg = "No manifest found in payload."
        LOG.warning(log_json(request_id, msg=msg, context=context))
        raise KafkaMsgHandlerError("No manifest found in payload.")

    return manifest_path[0], files


def _get_source_id(provider_uuid):
    """Obtain the source id for a given provider uuid."""
    source = Sources.objects.filter(koku_uuid=provider_uuid).first()
    if source:
        return source.source_id
    return None


def handle_ros_payload(
    b64_identity: str, report: utils.OCPManifest, payload_dir: os.PathLike, payload_files: list[str], context: dict
) -> None:
    ros_reports = [
        (ros_file, payload_dir.joinpath(ros_file))
        for ros_file in report.resource_optimization_files
        if ros_file in payload_files
    ]
    ros_processor = ROSReportShipper(report, b64_identity, context)
    try:
        ros_processor.process_manifest_reports(ros_reports)
    except Exception as e:
        # If a ROS report fails to process, this should not prevent Koku processing from continuing.
        msg = f"ROS reports not processed for payload. Reason: {e}"
        LOG.warning(log_json(report.tracing_id, msg=msg, context=context), exc_info=e)


def extract_payload(request_id, url, b64_identity, context) -> list[utils.OCPManifest]:  # noqa: C901
    """
    Extract OCP usage report payload into local directory structure.

    Payload is expected to be a .tar.gz file that contains:
    1. manifest.json - dictionary containing usage report details needed
        for report processing.
        Dictionary Contains:
            files - names of .csv usage reports for the manifest
            date - DateTime that the payload was created
            uuid - uuid for payload
            cluster_id  - OCP cluster ID.
    2. *.csv - Actual usage report for the cluster.  Format is:
        Format is: <uuid>_report_name.csv

    On successful completion the report and manifest will be in a directory
    structure that the OCPReportDownloader is expecting.

    Ex: /var/tmp/insights_local/my-ocp-cluster-1/20181001-20181101

    Once the files are extracted:
    1. Provider account is retrieved for the cluster id.  If no account is found we return.
    2. Manifest database record is created which will establish the assembly_id and number of files
    3. Report stats database record is created and is used as a filter to determine if the file
       has already been processed.
    4. All report files that have not been processed will have the local path to that report file
       added to the report_meta context dictionary for that file.
    5. Report file context dictionaries that require processing is added to a list which will be
       passed to the report processor.  All context from report_meta is used by the processor.

    Args:
        url (String): URL path to payload in the Insights upload service..
        request_id (String): Identifier associated with the payload
        context (Dict): Context for logging (account, etc)

    Returns:
        [dict]: keys: value
                files: [String],
                date: DateTime,
                cluster_id: String
                manifest_path: String,
                provider_uuid: String,
                provider_type: String
                schema_name: String
                manifest_id: Integer
                current_file: String

    """
    payload_filepath = download_payload(request_id, url, context)
    manifest_filename, payload_files = extract_payload_contents(request_id, payload_filepath, context)

    payload_dir = payload_filepath.parent
    # Open manifest.json file and build the payload dictionary.
    full_manifest_path = payload_dir.joinpath(manifest_filename)
    ocp_manifest = utils.get_ocp_manifest(full_manifest_path, request_id)

    context |= {
        "request_id": request_id,
        "cluster_id": ocp_manifest.cluster_id,
        "manifest_uuid": ocp_manifest.uuid,
    }
    LOG.info(
        log_json(
            request_id,
            msg=f"Payload with the request id {request_id} from cluster {ocp_manifest.cluster_id}"
            + f" is part of the report with manifest id {ocp_manifest.uuid}",
            context=context,
        )
    )
    account = get_account_from_cluster_id(ocp_manifest.cluster_id, request_id, context)
    if not account:
        msg = f"Recieved unexpected OCP report from {ocp_manifest.cluster_id}"
        LOG.warning(log_json(request_id, msg=msg, context=context))
        shutil.rmtree(payload_filepath.parent)
        return []

    ocp_manifest = ocp_manifest.model_copy(update=account, deep=True)

    context["provider_type"] = ocp_manifest.provider_type
    context["schema_name"] = ocp_manifest.schema_name
    ocp_manifest.source_id = _get_source_id(ocp_manifest.provider_uuid)

    ocp_manifest.destination_dir = Path(Config.INSIGHTS_LOCAL_REPORT_DIR, ocp_manifest.cluster_id)
    os.makedirs(ocp_manifest.destination_dir, exist_ok=True)

    # Save Manifest
    ocp_manifest.manifest_id = create_manifest_entries(ocp_manifest)

    handle_ros_payload(b64_identity, ocp_manifest, payload_dir, payload_files, context)

    reports = []
    starts = set()
    ends = set()
    for report_file in ocp_manifest.files:
        if report_file not in payload_files:
            msg = f"file {str(report_file)} has not downloaded yet"
            LOG.debug(log_json(request_id, msg=msg, context=context))
            continue

        current_meta = ocp_manifest.model_copy()
        current_meta.current_file = payload_dir.joinpath(report_file)

        df = read_ocp_csv(
            current_meta.current_file,
            usecols=["report_period_end", "report_period_start", "interval_start", "interval_end"],
        )
        starts.add(current_meta.start if df.interval_start.empty else df.interval_start.min())
        ends.add(current_meta.end if df.interval_end.empty else df.interval_end.max())

        record_all_manifest_files(current_meta.manifest_id, current_meta.files, current_meta.uuid)
        if record_report_status(current_meta.manifest_id, report_file, current_meta.uuid, context):
            # Report already processed
            continue
        msg = "successfully extracted OCP report file"
        LOG.info(log_json(request_id, msg=msg, context=context, report_file=report_file))
        split_files = create_daily_archives(current_meta, context)
        current_meta.split_files = list(split_files)
        current_meta.ocp_files_to_process = {file.stem: values for file, values in split_files.items()}
        reports.append(current_meta)
    # Remove temporary directory and files
    shutil.rmtree(payload_filepath.parent)

    # fix the start and end of the reports based on the files in the payload
    start = min(starts)
    end = max(ends)
    for report in reports:
        report.start = start
        report.end = end
    return reports


def get_report_dates(report_dates, start, end):
    print(start, end)
    starts, ends = {start}, {end}
    for dates in report_dates:
        if not pd.isnull(dates["report_start"]):
            starts.add(dates["report_start"])
        if not pd.isnull(dates["report_end"]):
            ends.add(dates["report_end"])
    return min(starts), max(ends)


@KAFKA_CONNECTION_ERRORS_COUNTER.count_exceptions()
def send_confirmation(request_id, status):  # pragma: no cover
    """
    Send kafka validation message to Insights Upload service.

    When a new file lands for topic 'hccm' we must validate it
    so that it will be made permanently available to other
    apps listening on the 'platform.upload.available' topic.

    Args:
        request_id (String): Request ID for file being confirmed.
        status (String): Either 'success' or 'failure'

    Returns:
        None

    """
    producer = get_producer()
    validation = {"request_id": request_id, "validation": status}
    msg = bytes(json.dumps(validation), "utf-8")
    producer.produce(Config.VALIDATION_TOPIC, value=msg, callback=delivery_callback)
    producer.poll(0)


def handle_message(request_id: str, kmsg: dict, context: dict) -> tuple[Literal, list[utils.OCPManifest], os.PathLike]:
    """
    Handle messages from message pending queue.

    Handle's messages with topics: 'platform.upload.hccm',
    and 'platform.upload.available'.

    The OCP cost usage payload will land on topic hccm.
    These messages will be extracted into the local report
    directory structure.  Once the file has been verified
    (successfully extracted) we will report the status to
    the Insights Upload Service so the file can be made available
    to other apps on the service.

    Messages on the available topic are messages that have
    been verified by an app on the Insights upload service.
    For now we are just logging the URL for demonstration purposes.
    In the future if we want to maintain a URL to our report files
    in the upload service we could look for hashes for files that
    we have previously validated on the hccm topic.


    Args:
        value - Upload Service message containing usage payload information.

    Returns:
        (String, [dict]) - String: Upload Service confirmation status
                         [dict]: keys: value
                                 files: [String],
                                 date: DateTime,
                                 cluster_id: String
                                 manifest_path: String,
                                 provider_uuid: String,
                                 provider_type: String
                                 schema_name: String
                                 manifest_id: Integer
                                 current_file: String

    """
    try:
        LOG.info(log_json(request_id, msg="extracting payload for msg", context=context, **kmsg))
        reports = extract_payload(request_id, kmsg["url"], kmsg["b64_identity"], context)
        return SUCCESS_CONFIRM_STATUS, reports
    except (OperationalError, InterfaceError) as error:
        close_and_set_db_connection()
        msg = f"Unable to extract payload, db closed. {type(error).__name__}: {error}"
        LOG.warning(log_json(request_id, msg=msg, context=context))
        raise KafkaMsgHandlerError(msg) from error
    except Exception as error:  # noqa
        traceback.print_exc()
        msg = f"Unable to extract payload. Error: {type(error).__name__}: {error}"
        LOG.warning(log_json(request_id, msg=msg, context=context))
        return FAILURE_CONFIRM_STATUS, []


def summarize_manifest(report_meta: utils.OCPManifest):
    """
    Kick off manifest summary when all report files have completed line item processing.

    Args:
        manifest_uuid (string) - The id associated with the payload manifest
        report (Dict) - keys: value
                        schema_name: String,
                        manifest_id: Integer,
                        provider_uuid: String,
                        provider_type: String,

    Returns:
        Celery Async UUID.

    """

    start_date = report_meta.start
    end_date = report_meta.end

    context = {
        "provider_uuid": report_meta.provider_uuid,
        "schema": report_meta.schema_name,
        "cluster_id": report_meta.cluster_id,
        "start_date": start_date,
        "end_date": end_date,
    }

    ocp_processing_queue = OCP_QUEUE
    if is_customer_large(report_meta.schema_name):
        ocp_processing_queue = OCP_QUEUE_XL

    if not MANIFEST_ACCESSOR.manifest_ready_for_summary(report_meta.manifest_id):
        return

    new_report_meta = {
        "schema_name": report_meta.schema_name,
        "provider_type": report_meta.provider_type,
        "provider_uuid": report_meta.provider_uuid,
        "manifest_id": report_meta.manifest_id,
        "manifest_uuid": report_meta.uuid,
        "start": start_date,
        "end": end_date,
    }
    if not (start_date or end_date):
        # we cannot process without start and end dates
        LOG.info(
            log_json(
                report_meta.request_id,
                msg="missing start or end dates - cannot summarize ocp reports",
                context=context,
            )
        )
        return

    if "0001-01-01 00:00:00+00:00" not in [str(start_date), str(end_date)]:
        # we have valid dates, so we can summarize the payload
        LOG.info(log_json(report_meta.request_id, msg="summarizing ocp reports", context=context))
        return summarize_reports.s([new_report_meta], ocp_processing_queue).apply_async(queue=ocp_processing_queue)

    cr_status = report_meta.cr_status
    if data_collection_message := cr_status.get("reports", {}).get("data_collection_message", ""):
        # remove potentially sensitive info from the error message
        msg = f'data collection error [operator]: {re.sub("{[^}]+}", "{***}", data_collection_message)}'
        cr_status["reports"]["data_collection_message"] = msg
        # The full CR status is logged below, but we should limit our alert to just the query.
        # We can check the full manifest to get the full error.
        LOG.error(msg)
        LOG.info(log_json(report_meta.request_id, msg=msg, context=context))
    LOG.info(
        log_json(
            report_meta.request_id,
            msg="cr status for invalid manifest",
            context=context,
            **cr_status,
        )
    )


def process_report(report: utils.OCPManifest):
    """
    Process line item report.

    Returns True when line item processing is complete.  This is important because
    the listen_for_messages -> process_messages path must have a positive acknowledgement
    that line item processing is complete before committing.

    If the service goes down in the middle of processing (SIGTERM) we do not want a
    stray kafka commit to prematurely commit the message before processing has been
    complete.

    Args:
        request_id (Str): The request id
        report (Dict) - keys: value
                        request_id: String,
                        account: String,
                        schema_name: String,
                        manifest_id: Integer,
                        provider_uuid: String,
                        provider_type: String,
                        current_file: String,
                        date: DateTime

    Returns:
        True if line item report processing is complete.

    """
    # The create_table flag is used by the ParquetReportProcessor
    # to create a Hive/Trino table.
    report_dict = {
        "file": report.current_file,
        "split_files": report.split_files,
        "ocp_files_to_process": report.ocp_files_to_process,
        "compression": UNCOMPRESSED,
        "manifest_id": report.manifest_id,
        "provider_uuid": report.provider_uuid,
        "tracing_id": report.request_id,
        "provider_type": report.provider_type,
        "start_date": report.date,
        "create_table": True,
    }
    try:
        return _process_report_file(report.schema_name, report.provider_type, report_dict)
    except NotImplementedError as err:
        LOG.info(f"NotImplementedError: {str(err)}")
        return True


def report_metas_complete(report_metas: list[utils.OCPManifest]) -> bool:
    """
    Verify if all reports from the ingress payload have been processed.

    in process_messages, a dictionary value "process_complete" is added to the
    report metadata dictionary for a report file.  This must be True for it to be
    considered processed.

    Args:
        report_metas (list) - List of report metadata dictionaries needed for line item
        processing.

    Returns:
        True if all report files for the payload have completed line item processing.

    """
    return all(report_meta.process_complete for report_meta in report_metas)


def process_messages(msg):
    """
    Process messages and send validation status.

    Processing involves:
    1. Downloading, verifying, extracting, and preparing report files for processing.
    2. Line item processing each report file in the payload (downloaded from step 1).
    3. Check if all reports have been processed for the manifest and if so, kick off
       the celery worker task to summarize.
    4. Send payload validation status to ingress service.

    Args:
        msg (ConsumerRecord) - Message from kafka hccm topic.

    Returns:
        None

    """
    process_complete = False
    kmsg = json.loads(msg.value().decode("utf-8"))
    request_id = kmsg.get("request_id")
    context = {"account": kmsg.get("account"), "org_id": kmsg.get("org_id")}
    status, reports = handle_message(request_id, kmsg, context)

    for report in reports:
        if report.daily_reports and len(report.files) != MANIFEST_ACCESSOR.number_of_files(report.manifest_id):
            # we have not received all of the daily files yet, so don't process them
            break
        report.process_complete = process_report(report)
        LOG.info(
            log_json(
                request_id,
                msg=f"Processing: {report.current_file} complete.",
                ocp_files_to_process=report.ocp_files_to_process,
            )
        )
    if reports:
        process_complete = report_metas_complete(reports)
        if summary_task_id := summarize_manifest(report):
            LOG.info(log_json(request_id, msg=f"Summarization celery uuid: {summary_task_id}"))

    if status and not settings.DEBUG:
        if reports:
            file_list = [meta.current_file for meta in reports]
            files_string = ",".join(map(str, file_list))
            LOG.info(log_json(request_id, msg=f"Sending Ingress Service confirmation for: {files_string}"))
        else:
            LOG.info(log_json(request_id, msg=f"Sending Ingress Service confirmation for: {kmsg}"))
        send_confirmation(request_id, status)

    return process_complete


def listen_for_messages_loop():
    """Wrap listen_for_messages in while true."""
    kafka_conf = {
        "group.id": "hccm-group",
        "queued.max.messages.kbytes": 1024,
        "enable.auto.commit": False,
        "max.poll.interval.ms": 1080000,  # 18 minutes
    }
    consumer = get_consumer(kafka_conf)
    consumer.subscribe([Config.UPLOAD_TOPIC])
    LOG.info("Consumer is listening for messages...")
    for _ in itertools.count():  # equivalent to while True, but mockable
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue

        if msg.error():
            KAFKA_CONNECTION_ERRORS_COUNTER.inc()
            LOG.error(f"[listen_for_messages_loop] consumer.poll message: {msg}. Error: {msg.error()}")
            continue

        listen_for_messages(msg, consumer)


def rewind_consumer_to_retry(consumer, topic_partition):
    """Helper method to log and rewind kafka consumer for retry."""
    LOG.info(f"Seeking back to offset: {topic_partition.offset}, partition: {topic_partition.partition}")
    consumer.seek(topic_partition)
    time.sleep(Config.RETRY_SECONDS)


def listen_for_messages(msg, consumer):
    """
    Listen for messages on the hccm topic.

    Once a message from one of these topics arrives, we add
    them extract the payload and line item process the report files.

    Once all files from the manifest are complete a celery job is
    dispatched to the worker to complete summary processing for the manifest.

    Several exceptions can occur while listening for messages:
    Database Errors - Re-processing attempts will be made until successful.
    Internal Errors - Re-processing attempts will be made until successful.
    Report Processing Errors - Kafka message will be committed with an error.
                               Errors of this type would require a report processor
                               fix and we do not want to block the message queue.

    Upon successful processing the kafka message is manually committed.  Manual
    commits are used so we can use the message queue to store unprocessed messages
    to make the service more tolerant of SIGTERM events.

    Args:
        consumer - (Consumer): kafka consumer for HCCM ingress topic.

    Returns:
        None

    """
    offset = msg.offset()
    partition = msg.partition()
    topic_partition = TopicPartition(topic=Config.UPLOAD_TOPIC, partition=partition, offset=offset)
    try:
        LOG.info(f"Processing message offset: {offset} partition: {partition}")
        service = extract_from_header(msg.headers(), "service")
        LOG.debug(f"service: {service} | {msg.headers()}")
        if service == "hccm":
            process_messages(msg)
        LOG.debug(f"COMMITTING: message offset: {offset} partition: {partition}")
        consumer.commit()
    except (InterfaceError, OperationalError, ReportProcessorDBError) as error:
        close_and_set_db_connection()
        LOG.error(f"[listen_for_messages] Database error. Error: {type(error).__name__}: {error}. Retrying...")
        rewind_consumer_to_retry(consumer, topic_partition)
    except (KafkaMsgHandlerError, KombuOperationalError) as error:
        LOG.error(f"[listen_for_messages] Internal error. {type(error).__name__}: {error}. Retrying...")
        rewind_consumer_to_retry(consumer, topic_partition)
    except ReportProcessorError as error:
        LOG.error(f"[listen_for_messages] Report processing error: {str(error)}")
        LOG.debug(f"COMMITTING: message offset: {offset} partition: {partition}")
        consumer.commit()
    except Exception as error:
        LOG.error(f"[listen_for_messages] UNKNOWN error encountered: {type(error).__name__}: {error}", exc_info=True)


def koku_listener_thread():  # pragma: no cover
    """
    Configure Listener listener thread.

    Returns:
        None

    """
    if is_kafka_connected(Config.INSIGHTS_KAFKA_HOST, Config.INSIGHTS_KAFKA_PORT):  # Check that Kafka is running
        LOG.info("Kafka is running.")

    try:
        listen_for_messages_loop()
    except KeyboardInterrupt:
        exit(0)


def initialize_kafka_handler():  # pragma: no cover
    """
    Start Listener thread.

    Args:
        None

    Returns:
        None

    """
    if Config.KAFKA_CONNECT:
        event_loop_thread = threading.Thread(target=koku_listener_thread)
        event_loop_thread.daemon = True
        event_loop_thread.start()
        event_loop_thread.join()
