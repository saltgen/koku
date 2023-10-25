import copy
import json
import logging

import ciso8601
import pandas as pd
from dateutil.parser import ParserError

from api.models import Provider
from masu.external.downloader.ocp.ocp_csv_reader import OCPCSVReader
from masu.util.common import populate_enabled_tag_rows_with_false
from masu.util.common import verify_data_types_in_parquet_file
from masu.util.ocp.common import OCP_REPORT_TYPES

LOG = logging.getLogger(__name__)


def process_openshift_datetime(val):
    """
    Convert the date time from the Metering operator reports to a consumable datetime.
    """
    result = None
    try:
        datetime_str = str(val).replace(" +0000 UTC", "")
        result = ciso8601.parse_datetime(datetime_str)
    except ParserError:
        pass
    return result


def process_openshift_labels(label_string):
    """Convert the report string to a JSON dictionary.

    Args:
        label_string (str): The raw report string of pod labels

    Returns:
        (dict): The JSON dictionary made from the label string

    Dev Note:
        You can reference the operator here to see what queries to run
        in prometheus to see the labels.

    """
    labels = label_string.split("|") if label_string else []
    label_dict = {}

    for label in labels:
        if ":" not in label:
            continue
        try:
            key, value = label.split(":")
            key = key.replace("label_", "")
            label_dict[key] = value
        except ValueError as err:
            LOG.warning(err)
            LOG.warning("%s could not be properly split", label)
            continue
    return label_dict


def process_openshift_labels_to_json(label_val):
    return json.dumps(process_openshift_labels(label_val))


class OCPPostProcessor:
    def __init__(self, schema, csv_filepath, report_type):
        self.schema = schema
        self.enabled_tag_keys = set()
        self.report_type = report_type
        self.ocp_report_types = OCP_REPORT_TYPES
        self.csv_reader = OCPCSVReader(csv_filepath)
        self.parquet_conversion_started()

    def parquet_conversion_started(self):
        # Update a new state to show that we have made it
        # to the conversion step.
        return True

    def __add_effective_usage_columns(self, data_frame):
        """Add effective usage columns to pod data frame."""
        if self.report_type != "pod_usage":
            return data_frame
        data_frame["pod_effective_usage_cpu_core_seconds"] = data_frame[
            ["pod_usage_cpu_core_seconds", "pod_request_cpu_core_seconds"]
        ].max(axis=1)
        data_frame["pod_effective_usage_memory_byte_seconds"] = data_frame[
            ["pod_usage_memory_byte_seconds", "pod_request_memory_byte_seconds"]
        ].max(axis=1)
        return data_frame

    def check_ingress_required_columns(self, _):
        """
        Checks the required columns for ingress.
        """
        # Question: Are we notifing our ingress users
        # of when they don't have the required columns.
        # If not we could add a wrapper to notify customers
        # to this mehtod.
        return None

    def _generate_daily_data(self, data_frame):
        """Given a dataframe, group the data to create daily data."""

        data_frame = self.__add_effective_usage_columns(data_frame)

        if data_frame.empty:
            return data_frame

        report = self.ocp_report_types.get(self.report_type, {})
        group_bys = copy.deepcopy(report.get("group_by", []))
        group_bys.append(pd.Grouper(key="interval_start", freq="D"))
        aggs = report.get("agg", {})
        daily_data_frame = data_frame.groupby(group_bys, dropna=False).agg(
            {k: v for k, v in aggs.items() if k in data_frame.columns}
        )

        columns = daily_data_frame.columns.droplevel(1)
        daily_data_frame.columns = columns

        daily_data_frame.reset_index(inplace=True)

        new_cols = report.get("new_required_columns")
        for col in new_cols:
            if col not in daily_data_frame:
                daily_data_frame[col] = None

        return daily_data_frame

    def process_dataframe(self, data_frame, parquet_filepath):
        # Track New State: Parquet Conversion has started
        # TODO: See if we can track which i we are from batching
        # in the ParquetReportProcessor. We could track that
        # so that we could know where in the process we failed
        # to convert to parquet.
        label_columns = {"pod_labels", "volume_labels", "namespace_labels", "node_labels"}
        df_columns = set(data_frame.columns)
        columns_to_grab = df_columns.intersection(label_columns)
        label_key_set = set()
        for column in columns_to_grab:
            unique_labels = data_frame[column].unique()
            for label in unique_labels:
                label_key_set.update(json.loads(label).keys())
        self.enabled_tag_keys.update(label_key_set)
        data_frame.to_parquet(parquet_filepath, allow_truncated_timestamps=True, coerce_timestamps="ms", index=False)
        verify_data_types_in_parquet_file()
        return self._generate_daily_data(data_frame, parquet_filepath)

    def finalize_post_processing(self):
        """
        Uses information gather in the post processing to update the cost models.
        """
        populate_enabled_tag_rows_with_false(self.schema, self.enabled_tag_keys, Provider.PROVIDER_OCP)
        # Track New State: Parquet Conversion is complete
