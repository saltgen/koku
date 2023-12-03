import logging
import os
from pathlib import Path

from botocore.exceptions import ClientError
from django.conf import settings

import masu.prometheus_stats as worker_stats
from api.common import log_json
from api.utils import DateHelper
from masu.config import Config
from masu.database.report_manifest_db_accessor import ReportManifestDBAccessor
from masu.external import UNCOMPRESSED
from masu.external.downloader.report_downloader_base import ReportDownloaderBase
from masu.processor._tasks.process import _process_report_file
from masu.processor.report_processor import ReportProcessorDBError
from masu.processor.report_processor import ReportProcessorError
from masu.processor.tasks import record_all_manifest_files
from masu.processor.worker_cache import WorkerCache
from masu.util.aws import common as utils
from masu.util.aws.common import _get_s3_objects
from masu.util.common import get_path_prefix

DATA_DIR = Config.TMP_DIR
LOG = logging.getLogger(__name__)


class ReportDownloaderError(Exception):
    """Report Downloader error."""


class ReportDownloaderNoFileError(Exception):
    """Report Downloader error for missing file."""


def fetch_csv_files_for_reprocessing(customer_name, provider_type, provider_uuid, start_date, tracing_id):  # noqa C901
    """Function to download csv files from S3 to attempt parquet reprocessing"""
    dh = DateHelper()
    context = {
        "schema": customer_name,
        "provider_uuid": provider_uuid,
        "provider_type": provider_type,
        "date": start_date,
    }
    csv_path_prefix = get_path_prefix(customer_name, provider_type, provider_uuid, start_date, Config.CSV_DATA_TYPE)
    # list files in s3
    reports_to_download = []
    get_objects = _get_s3_objects(csv_path_prefix)
    if not get_objects:
        return None
    for obj_summary in get_objects:
        reports_to_download.append(obj_summary.Object().key)

    # Download files from S3 (We may need to break this up into batches if there is 100000s of files)
    file_names = []
    bucket = settings.S3_BUCKET_NAME
    for key in reports_to_download:
        s3_filename = key.split("/")[-1]
        directory_path = Path(Config.TMP_DIR, customer_name, str(provider_type), str(provider_uuid))
        directory_path.mkdir(parents=True, exist_ok=True)
        local_s3_filename = utils.get_local_file_name(key)
        msg = f"Local S3 filename: {local_s3_filename}"
        LOG.info(log_json(tracing_id, msg=msg, context=context))
        full_file_path = directory_path.joinpath(s3_filename)
        # Make sure the data directory exists
        os.makedirs(directory_path, exist_ok=True)
        try:
            s3_resource = utils.get_s3_resource(settings.S3_ACCESS_KEY, settings.S3_SECRET, settings.S3_REGION)
            s3_bucket = s3_resource.Bucket(settings.S3_BUCKET_NAME)
            s3_bucket.download_file(key, full_file_path)
        except ClientError as ex:
            if ex.response["Error"]["Code"] == "NoSuchKey":
                msg = f"Unable to find {s3_filename} in S3 Bucket: {bucket}"
                LOG.info(log_json(tracing_id, msg=msg, context=context))
                raise ReportDownloaderNoFileError(msg)
            if ex.response["Error"]["Code"] == "AccessDenied":
                msg = f"Unable to access S3 Bucket {bucket}: (AccessDenied)"
                LOG.info(log_json(tracing_id, msg=msg, context=context))
                raise ReportDownloaderNoFileError(msg)
        file_names.append(full_file_path)

    # create new manifest entry
    manifest_id = ReportDownloaderBase(tracing_id=tracing_id, provider_uuid=provider_uuid)._process_manifest_db_record(
        tracing_id, start_date, len(file_names), start_date
    )
    fake_filename = "reprocess_cvs"
    fake_file = f"{csv_path_prefix}/{fake_filename}"
    if manifest_id:
        LOG.debug("Saving all manifest file names.")
        record_all_manifest_files(
            manifest_id,
            [fake_filename],
            tracing_id,
        )
    # set clear Parquet files
    ReportManifestDBAccessor().mark_s3_parquet_to_be_cleared(manifest_id)

    report_dict = {
        "file": fake_file,
        "split_files": file_names,
        "compression": UNCOMPRESSED,
        "start_date": start_date,
        "assembly_id": tracing_id,
        "manifest_id": manifest_id,
        "tracing_id": tracing_id,
        "provider_type": provider_type,
        "provider_uuid": provider_uuid,
        "create_table": False,
        "start": start_date,
        "end": dh.month_end(start_date),
        "invoice_month": dh.invoice_month_from_bill_date(start_date),
    }

    # Maybe this should move to its own task? (Download/create csvs step 1, processing/create parquet step 2)
    cache_key = f"{provider_uuid}:{fake_file}"
    WorkerCache().add_task_to_cache(cache_key)
    try:
        LOG.info(log_json(tracing_id, msg="processing starting", context=context))
        result = _process_report_file(customer_name, provider_type, report_dict)

    except (ReportProcessorError, ReportProcessorDBError) as processing_error:
        worker_stats.PROCESS_REPORT_ERROR_COUNTER.labels(provider_type=provider_type).inc()
        LOG.error(log_json(tracing_id, msg=f"Report processing error: {processing_error}", context=context))
        WorkerCache().remove_task_from_cache(cache_key)
        raise processing_error
    except NotImplementedError as err:
        LOG.info(log_json(tracing_id, msg=f"Not implemented error: {err}", context=context))
        WorkerCache().remove_task_from_cache(cache_key)

    WorkerCache().remove_task_from_cache(cache_key)
    if not result:
        LOG.info(log_json(tracing_id, msg="no report files processed, skipping summary", context=context))
        return None

    return [manifest_id], [report_dict]
