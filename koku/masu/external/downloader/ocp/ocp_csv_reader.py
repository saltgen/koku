import json
import logging
import os
from dataclasses import dataclass
from dataclasses import field
from typing import List

import ciso8601
import pandas as pd
from dateutil.parser import ParserError

from masu.util.common import CSV_GZIP_EXT
from masu.util.common import safe_float

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


@dataclass
class OCPCSVReader:
    csv_filename: os.PathLike
    column_names: List[str] = field(default_factory=list)

    NUMERIC_COLUMNS = [
        "pod_usage_cpu_core_seconds",
        "pod_request_cpu_core_seconds",
        "pod_effective_usage_cpu_core_seconds",
        "pod_limit_cpu_core_seconds",
        "pod_usage_memory_byte_seconds",
        "pod_request_memory_byte_seconds",
        "pod_effective_usage_memory_byte_seconds",
        "pod_limit_memory_byte_seconds",
        "node_capacity_cpu_cores",
        "node_capacity_cpu_core_seconds",
        "node_capacity_memory_bytes",
        "node_capacity_memory_byte_seconds",
        "persistentvolumeclaim_usage_byte_seconds",
        "volume_request_storage_byte_seconds",
        "persistentvolumeclaim_capacity_byte_seconds",
        "persistentvolumeclaim_capacity_bytes",
    ]

    DATE_COLUMNS = ["report_period_start", "report_period_end", "interval_start", "interval_end"]

    @property
    def numeric_columns(self):
        return self.NUMERIC_COLUMNS

    @property
    def date_columns(self):
        return self.DATE_COLUMNS

    def __post_init__(self):
        self._collect_columns()

    def _collect_columns(self):
        """Collects the column names from the file."""
        panda_kwargs = {}
        if str(self.csv_filename).endswith(CSV_GZIP_EXT):
            panda_kwargs = {"compression": "gzip"}
        self.column_names = pd.read_csv(self.csv_filename, nrows=0, **panda_kwargs).columns

    def generate_read_panda_kwargs(self, panda_kwargs=None):
        """Generates the dtypes using the types defined in the Trino schema."""
        if panda_kwargs is None:
            panda_kwargs = {}
        converters = {
            "pod_labels": process_openshift_labels_to_json,
            "persistentvolume_labels": process_openshift_labels_to_json,
            "persistentvolumeclaim_labels": process_openshift_labels_to_json,
            "node_labels": process_openshift_labels_to_json,
            "namespace_labels": process_openshift_labels_to_json,
        }
        dtype = {}
        for column_name in self.column_names:
            if column_name in converters:
                pass
            if column_name in self.NUMERIC_COLUMNS:
                converters[column_name] = safe_float
            elif column_name in self.DATE_COLUMNS:
                converters[column_name] = process_openshift_datetime
            else:
                dtype[column_name] = str
        dtype["node_role"] = str
        panda_kwargs["converters"] = converters
        panda_kwargs["dtype"] = dtype
        return panda_kwargs
