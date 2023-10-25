import json
import os
from dataclasses import dataclass
from dataclasses import field
from typing import List

import ciso8601
import pandas as pd
from numpy import nan

from masu.util.common import CSV_GZIP_EXT
from masu.util.common import safe_float
from masu.util.common import strip_characters_from_column_name


def azure_json_converter(tag_str):
    """Convert either Azure JSON field format to proper JSON."""
    tag_dict = {}
    try:
        if "{" in tag_str:
            tag_dict = json.loads(tag_str)
        else:
            tags = tag_str.split('","')
            for tag in tags:
                key, value = tag.split(": ")
                tag_dict[key.strip('"')] = value.strip('"')
    except (ValueError, TypeError):
        pass

    return json.dumps(tag_dict)


def azure_date_converter(date):
    """Convert Azure date fields properly."""
    if date:
        try:
            new_date = ciso8601.parse_datetime(date)
        except ValueError:
            date_split = date.split("/")
            new_date_str = date_split[2] + date_split[0] + date_split[1]
            new_date = ciso8601.parse_datetime(new_date_str)
        return new_date
    else:
        return nan


@dataclass
class AzureCSVReader:
    csv_filename: os.PathLike
    column_names: List[str] = field(default_factory=list)

    NUMERIC_COLUMNS = {
        "usagequantity",
        "quantity",
        "resourcerate",
        "pretaxcost",
        "costinbillingcurrency",
        "effectiveprice",
        "unitprice",
        "paygprice",
    }

    DATE_COLUMNS = {"usagedatetime", "date", "billingperiodstartdate", "billingperiodenddate"}

    BOOLEAN_COLUMNS = {"resource_id_matched"}

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
        converters = {"tags": azure_json_converter, "additionalinfo": azure_json_converter}
        dtype = {}
        for column_name in self.column_names:
            cleaned_column_name = strip_characters_from_column_name(column_name)
            if cleaned_column_name in self.NUMERIC_COLUMNS:
                converters[column_name] = safe_float
            elif cleaned_column_name in self.DATE_COLUMNS:
                converters[column_name] = azure_date_converter
            else:
                dtype[column_name] = str
        panda_kwargs["converters"] = converters
        panda_kwargs["dtype"] = dtype
        return panda_kwargs
