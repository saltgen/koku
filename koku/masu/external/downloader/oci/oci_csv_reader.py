import logging
import os
from dataclasses import dataclass
from dataclasses import field
from typing import List

import ciso8601
import pandas as pd

from masu.util.common import CSV_GZIP_EXT
from masu.util.common import safe_float
from masu.util.common import strip_characters_from_column_name

LOG = logging.getLogger(__name__)


@dataclass
class OCICSVReader:
    csv_filename: os.PathLike
    column_names: List[str] = field(default_factory=list)

    NUMERIC_COLUMNS = ["usage_consumedquantity", "cost_mycost"]
    DATE_COLUMNS = [
        "lineitem_intervalusagestart",
        "lineitem_intervalusageend",
        "bill_billingperiodstartdate",
        "bill_billingperiodenddate",
    ]
    BOOLEAN_COLUMNS = ["resource_id_matched"]

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
        converters = {}
        dtype = {}
        for column_name in self.column_names:
            cleaned_column_name = strip_characters_from_column_name(column_name)
            if cleaned_column_name in self.NUMERIC_COLUMNS:
                converters[column_name] = safe_float
            elif cleaned_column_name in self.DATE_COLUMNS:
                converters[column_name] = ciso8601.parse_datetime
            else:
                dtype[column_name] = str
        panda_kwargs["converters"] = converters
        panda_kwargs["dtype"] = dtype
        return panda_kwargs
