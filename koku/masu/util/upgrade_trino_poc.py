import logging
import os
import uuid
from pathlib import Path

import ciso8601
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError
from django.conf import settings

from masu.config import Config
from masu.database.provider_db_accessor import ProviderDBAccessor
from masu.processor.oci.oci_report_parquet_processor import OCIReportParquetProcessor
from masu.processor.parquet.parquet_report_processor import OPENSHIFT_REPORT_TYPE
from masu.util.aws.common import get_s3_resource
from masu.util.common import get_path_prefix

# import pandas as pd

LOG = logging.getLogger(__name__)

# step one: make load-test-customer-data data_source=OCI && make create-test-customer
# step two: docker exec -it trino trino --catalog hive --schema org1234567 --user admin --debug
# step 2a: select * from oci_cost_line_items; (See error)
# step 3: http://127.0.0.1:5042/api/cost-management/v1/fix_parquet/
# Repeat step 2 and see results


class StateTracker:
    FOUND_S3_FILE = "found_s3_file"
    DOWNLOADED_LOCALLY = "downloaded_locally"
    NO_CHANGES_NEEDED = "no_changes_needed"
    COERCE_REQUIRED = "coerce_required"
    SENT_TO_S3_COMPLETE = "sent_to_s3_complete"
    SENT_TO_S3_FAILED = "sent_to_s3_failed"
    FAILED_DTYPE_CONVERSION = "failed_data_type_conversion"

    def __init__(self, provider_uuid):
        self.files = []
        self.tracker = {}
        self.local_files = {}
        self.provider_uuuid = provider_uuid
        self.context_key = "dtype_conversion"

    def set_state(self, s3_obj_key, state):
        self.tracker[s3_obj_key] = state

    def add_local_file(self, s3_obj_key, local_path):
        self.local_files[s3_obj_key] = local_path
        self.tracker[s3_obj_key] = self.DOWNLOADED_LOCALLY

    def get_files_that_need_updated(self):
        """Returns a mapping of files in the s3 needs
        updating state.

         {s3_object_key: local_file_path} for
        """
        mapping = {}
        for s3_obj_key, state in self.tracker.items():
            if state == self.COERCE_REQUIRED:
                mapping[s3_obj_key] = self.local_files.get(s3_obj_key)
        return mapping

    def generate_simulate_messages(self):
        """
        Generates the simulate messages.
        """
        files_count = 0
        files_failed = []
        files_need_updated = []
        files_correct = []
        for s3_obj_key, state in self.tracker.items():
            files_count += 1
            if state == self.COERCE_REQUIRED:
                files_need_updated.append(s3_obj_key)
            elif state == self.NO_CHANGES_NEEDED:
                files_correct.append(s3_obj_key)
            else:
                files_failed.append(s3_obj_key)
        simulate_info = {
            "already correct.": files_correct,
            "need updated.": files_need_updated,
            "failed to convert.": files_failed,
        }
        for substring, files_list in simulate_info.items():
            LOG.info(f"{len(files_list)} out of {files_count} {substring}")
            if files_list:
                LOG.info(f"File list: {files_list}")

    def _clean_local_files(self):
        for file_path in self.local_files.values():
            os.remove(file_path)

    def _check_if_complete(self):
        for state in self.tracker.values():
            if state not in [self.SENT_TO_S3_COMPLETE, self.NO_CHANGES_NEEDED]:
                return False
        with ProviderDBAccessor(self.provider_uuuid) as provider_accessor:
            context = provider_accessor.get_additional_context()
            context[self.context_key] = True

    def finalize_and_clean_up(self):
        self._check_if_complete()
        self._clean_local_files
        # We can decide if we want to record
        # failed parquet conversion


class VerifyParquetFiles:
    CONVERTER_VERSION = 1.0

    def __init__(self, schema, provider_uuid, provider_type, simulate):
        self.schema_name = schema
        self.provider_uuid = uuid.UUID(provider_uuid)
        self.provider_type = provider_type.replace("-local", "")
        self.simulate = simulate
        self.file_tracker = StateTracker(provider_uuid)
        # Provider specific vars
        self.openshift_data = False
        self.numeric_columns = []
        self.date_columns = []
        self.boolean_columns = []
        self.report_types = [None]
        self._collect_provider_type_info()

    def _get_bill_dates(self):
        # However far back we want to fix.
        return [
            ciso8601.parse_datetime("2023-09-01"),
            ciso8601.parse_datetime("2023-10-01"),
            # ciso8601.parse_datetime("2023-11-01")
        ]

    def _collect_provider_type_info(self):
        if self.provider_type == "OCI":
            self.numeric_columns = OCIReportParquetProcessor.NUMERIC_COLUMNS
            self.date_columns = OCIReportParquetProcessor.DATE_COLUMNS
            self.boolean_columns = OCIReportParquetProcessor.BOOLEAN_COLUMNS
            self.report_types = ["cost", "usage"]

    # Stolen from parquet_report_processor
    def _parquet_path_s3(self, bill_date, report_type):
        """The path in the S3 bucket where Parquet files are loaded."""
        return get_path_prefix(
            self.schema_name,
            self.provider_type,
            self.provider_uuid,
            bill_date,
            Config.PARQUET_DATA_TYPE,
            report_type=report_type,
        )

    # Stolen from parquet_report_processor
    def _parquet_daily_path_s3(self, bill_date, report_type):
        """The path in the S3 bucket where Parquet files are loaded."""
        if report_type is None:
            report_type = "raw"
        return get_path_prefix(
            self.schema_name,
            self.provider_type,
            self.provider_uuid,
            bill_date,
            Config.PARQUET_DATA_TYPE,
            report_type=report_type,
            daily=True,
        )

    # Stolen from parquet_report_processor
    def _parquet_ocp_on_cloud_path_s3(self, bill_date):
        """The path in the S3 bucket where Parquet files are loaded."""
        return get_path_prefix(
            self.schema_name,
            self.provider_type,
            self.provider_uuid,
            bill_date,
            Config.PARQUET_DATA_TYPE,
            report_type=OPENSHIFT_REPORT_TYPE,
            daily=True,
        )

    # Stolen from parquet_report_processor
    def _generate_s3_path_prefixes(self, bill_date):
        """
        generates the s3 path prefixes.
        """
        path_prefixes = set()
        for report_type in self.report_types:
            path_prefixes.add(self._parquet_path_s3(bill_date, report_type))
            path_prefixes.add(self._parquet_daily_path_s3(bill_date, report_type))
        if self.openshift_data:
            path_prefixes.add(self._parquet_ocp_on_cloud_path_s3(bill_date))
        return path_prefixes

    # Stolen from parquet_report_processor
    @property
    def local_path(self):
        local_path = Path(Config.TMP_DIR, self.schema_name, str(self.provider_uuid))
        local_path.mkdir(parents=True, exist_ok=True)
        return local_path

    # New logic to download the parquet files locally, coerce them,
    # then upload the files that need updated back to s3
    def retrieve_verify_reload_S3_parquet(self):
        """Retrieves the s3 files from s3"""
        if not self.numeric_columns or not self.date_columns:
            LOG.info("No dtype columns set.")
            return False

        s3_resource = get_s3_resource(settings.S3_ACCESS_KEY, settings.S3_SECRET, settings.S3_REGION)
        s3_bucket = s3_resource.Bucket(settings.S3_BUCKET_NAME)
        bill_dates = self._get_bill_dates()
        for bill_date in bill_dates:
            for prefix in self._generate_s3_path_prefixes(bill_date):
                for s3_object in s3_bucket.objects.filter(Prefix=prefix):
                    s3_object_key = s3_object.key
                    self.file_tracker.set_state(s3_object_key, self.file_tracker.FOUND_S3_FILE)
                    LOG.info(s3_object_key)
                    local_file_path = os.path.join(self.local_path, os.path.basename(s3_object_key))
                    s3_bucket.download_file(s3_object_key, local_file_path)
                    self.file_tracker.add_local_file(s3_object_key, local_file_path)
                    self.file_tracker.set_state(s3_object_key, self._coerce_parquet_data_type(local_file_path))
        if self.simulate:
            self.file_tracker.generate_simulate_messages()
            return False
        else:
            files_need_updated = self.file_tracker.get_files_that_need_updated()
            for s3_obj_key, converted_local_file_path in files_need_updated.items():
                try:
                    s3_bucket.Object(s3_obj_key).delete()
                    LOG.info(f"Deleted current parquet file: {s3_obj_key}")
                except ClientError as e:
                    LOG.info(f"Failed to delete {s3_object_key}: {str(e)}")
                    self.file_tracker.set_state(s3_object_key, self.file_tracker.SENT_TO_S3_FAILED)
                    continue

                # An error here would cause a data gap.
                with open(converted_local_file_path, "rb") as new_file:
                    s3_bucket.upload_fileobj(new_file, s3_obj_key)
                    LOG.info(f"Uploaded revised parquet: {s3_object_key}")
                    self.file_tracker.set_state(s3_obj_key, self.file_tracker.SENT_TO_S3_COMPLETE)
        self.file_tracker.finalize_and_clean_up()

    # Same logic as last time, but combined into one method & added state tracking
    def _coerce_parquet_data_type(self, parquet_file_path):
        """If a parquet file has an incorrect dtype we can attempt to coerce
        it to the correct type it.

        Returns a boolean indicating if the update parquet file should be sent
        to s3.
        """
        LOG.info("-----")
        LOG.info(f"checking parquet file: {parquet_file_path}")
        corrected_fields = {}
        try:
            table = pq.read_table(parquet_file_path)
            schema = table.schema
            fields = []
            for field in schema:
                if field.name in self.numeric_columns:
                    correct_data_type = pa.float64()
                elif field.name in self.date_columns:
                    correct_data_type = pa.timestamp("ms")
                elif field.name in self.boolean_columns:
                    correct_data_type = pa.bool_()
                else:
                    correct_data_type = pa.string()

                # Check if the field's type matches the desired type
                if field.type != correct_data_type:
                    # State update: Needs to be replaced.
                    LOG.info(f"{field.name} has the incorrect data type of {field.type}")
                    LOG.info(f"Updating the schema to specify {field.name} should be {correct_data_type}")
                    LOG.info("\n")
                    field = pa.field(field.name, correct_data_type)
                    corrected_fields[field.name] = correct_data_type
                fields.append(field)

            if not corrected_fields:
                # Final State: No changes needed.
                LOG.info("No conversion neccessary")
                return self.file_tracker.NO_CHANGES_NEEDED

            new_schema = pa.schema(fields)
            LOG.info("Applying the new schema to the local parquet file.")
            table = table.cast(new_schema)

            LOG.info("Saving the updates to the local parquet file.")
            # Write the table back to the Parquet file
            pa.parquet.write_table(table, parquet_file_path)
            # Signal that we need to send this update to S3.
            return self.file_tracker.COERCE_REQUIRED

        except Exception as e:
            LOG.info(f"Failed to coerce data: {e}")
            return self.file_tracker.FAILED_DTYPE_CONVERSION
