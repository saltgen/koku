import json
import logging
import os
from dataclasses import dataclass
from dataclasses import field
from json.decoder import JSONDecodeError
from typing import List

import ciso8601
import pandas as pd

from masu.util.common import CSV_GZIP_EXT
from masu.util.common import safe_float
from masu.util.common import strip_characters_from_column_name

LOG = logging.getLogger(__name__)


def process_gcp_labels(label_string):
    """Convert the report string to a JSON dictionary.

    Args:
        label_string (str): The raw report string of pod labels

    Returns:
        (dict): The JSON dictionary made from the label string

    """
    label_dict = {}
    try:
        if label_string:
            labels = json.loads(label_string)
            label_dict = {entry.get("key"): entry.get("value") for entry in labels}
    except JSONDecodeError:
        LOG.warning("Unable to process GCP labels.")

    return json.dumps(label_dict)


def process_gcp_credits(credit_string):
    """Process the credits column, which is non-standard JSON."""
    credit_dict = {}
    try:
        gcp_credits = json.loads(credit_string.replace("'", '"').replace("None", '"None"'))
        if gcp_credits:
            credit_dict = gcp_credits[0]
    except JSONDecodeError:
        LOG.warning("Unable to process GCP credits.")

    return json.dumps(credit_dict)


@dataclass
class GCPCSVReader:
    csv_filename: os.PathLike
    column_names: List[str] = field(default_factory=list)

    NUMERIC_COLUMNS = [
        "cost",
        "currency_conversion_rate",
        "usage_amount",
        "usage_amount_in_pricing_units",
        "credit_amount",
        "daily_credits",
    ]
    DATE_COLUMNS = ["usage_start_time", "usage_end_time", "export_time", "partition_time"]
    BOOLEAN_COLUMNS = ["ocp_matched"]

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
            "labels": process_gcp_labels,
            "system_labels": process_gcp_labels,
            "credits": process_gcp_credits,
        }
        dtype = {}
        for column_name in self.column_names:
            cleaned_column_name = strip_characters_from_column_name(column_name)
            if cleaned_column_name in converters:
                pass
            if cleaned_column_name in self.NUMERIC_COLUMNS:
                converters[column_name] = safe_float
            elif cleaned_column_name in self.DATE_COLUMNS:
                converters[column_name] = ciso8601.parse_datetime
            else:
                dtype[column_name] = str
        panda_kwargs["converters"] = converters
        panda_kwargs["dtype"] = dtype
        return panda_kwargs
