import logging
import os
from dataclasses import dataclass
from dataclasses import field
from typing import List

import ciso8601
import pandas as pd

from masu.util.aws.common import CSV_COLUMN_PREFIX
from masu.util.aws.common import OPTIONAL_ALT_COLS
from masu.util.aws.common import OPTIONAL_COLS
from masu.util.aws.common import RECOMMENDED_ALT_COLUMNS
from masu.util.aws.common import RECOMMENDED_COLUMNS
from masu.util.common import CSV_GZIP_EXT
from masu.util.common import safe_float

LOG = logging.getLogger(__name__)


@dataclass
class AWSCSVReader:
    csv_filename: os.PathLike
    column_names: List[str] = field(default_factory=list)

    # TODO: Think about naming here, probs need to reverse these
    NUMERIC_COLUMNS = {
        "lineitem_normalizationfactor",
        "lineitem_normalizedusageamount",
        "lineitem_usageamount",
        "lineitem_unblendedcost",
        "lineitem_unblendedrate",
        "lineitem_blendedcost",
        "lineitem_blendedrate",
        "savingsplan_savingsplaneffectivecost",
        "pricing_publicondemandrate",
        "pricing_publicondemandcost",
    }

    ALT_NUMERIC_COLUMNS = {
        "lineitem/usageamount",
        "lineitem/normalizationfactor",
        "lineitem/normalizedusageamount",
        "lineitem/unblendedrate",
        "lineitem/unblendedcost",
        "lineitem/blendedrate",
        "lineitem/blendedcost",
        "pricing/publicondemandcost",
        "pricing/publicondemandrate",
    }

    DATE_COLUMNS = {
        "lineitem_usagestartdate",
        "lineitem_usageenddate",
        "bill_billingperiodstartdate",
        "bill_billingperiodenddate",
    }

    ALT_DATE_COLUMNS = {
        "bill/billingperiodstartdate",
        "bill/billingperiodenddate",
        "lineitem/usagestartdate",
        "lineitem/usageenddate",
    }

    AWS_BOOLEAN_COLUMNS = {"resource_id_matched"}

    def __post_init__(self):
        self._collect_columns()

    @property
    def numeric_columns(self):
        return self.NUMERIC_COLUMNS.union(self.ALT_NUMERIC_COLUMNS)

    @property
    def date_columns(self):
        return self.ALT_DATE_COLUMNS.union(self.DATE_COLUMNS)

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
            if column_name in self.numeric_columns:
                converters[column_name] = safe_float
            elif column_name in self.date_columns:
                converters[column_name] = ciso8601.parse_datetime
            else:
                dtype[column_name] = str
        panda_kwargs["converters"] = converters
        panda_kwargs["dtype"] = dtype
        csv_columns = RECOMMENDED_COLUMNS.union(RECOMMENDED_ALT_COLUMNS).union(OPTIONAL_COLS).union(OPTIONAL_ALT_COLS)
        panda_kwargs["usecols"] = [
            col for col in self.column_names if col in csv_columns or col.startswith(CSV_COLUMN_PREFIX)
        ]
        return panda_kwargs
