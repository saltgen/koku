import json
import logging

import ciso8601
import pandas as pd

from api.models import Provider
from masu.processor.oci.oci_report_parquet_processor import OCIReportParquetProcessor as trino_schema
from masu.util.common import populate_enabled_tag_rows_with_limit
from masu.util.common import safe_float
from masu.util.common import strip_characters_from_column_name
from reporting.provider.oci.models import TRINO_REQUIRED_COLUMNS

LOG = logging.getLogger(__name__)


def scrub_resource_col_name(res_col_name):
    return res_col_name.split(".")[-1]


def check_all_parquet_files_corrected():
    # A place holder for some type of check that notifies us
    # that we have fixed all the parquet files for this
    # account for this provider so we can switch to the new data types.
    return False


class OCIPostProcessor:
    def __init__(self, schema):
        self.schema = schema
        self.enabled_tag_keys = set()

    def get_column_converters(self, col_names, panda_kwargs):
        """
        Return source specific parquet column converters.
        """
        converters = {}
        for column_name in col_names:
            cleaned_column = strip_characters_from_column_name(column_name)
            if cleaned_column in trino_schema.NUMERIC_COLUMNS:
                converters[column_name] = safe_float
            elif cleaned_column in trino_schema.DATE_COLUMNS:
                converters[column_name] = ciso8601.parse_datetime
            elif cleaned_column in trino_schema.BOOLEAN_COLUMNS:
                converters[column_name] = bool
            else:
                converters[column_name] = str
        return converters, panda_kwargs

    def check_ingress_required_columns(self, _):
        """
        Checks the required columns for ingress.
        """
        return None

    def _generate_daily_data(self, data_frame):
        """Given a dataframe, group the data to create daily data."""

        if "cost_mycost" in data_frame:
            daily_data_frame = data_frame.groupby(
                [
                    "product_resourceid",
                    pd.Grouper(key="lineitem_intervalusagestart", freq="D"),
                    "lineitem_tenantid",
                    "product_service",
                    "product_region",
                    "tags",
                ],
                dropna=False,
            ).agg({"cost_currencycode": ["max"], "cost_mycost": ["sum"]})
        else:
            daily_data_frame = data_frame.groupby(
                [
                    "product_resourceid",
                    pd.Grouper(key="lineitem_intervalusagestart", freq="D"),
                    "lineitem_tenantid",
                    "product_service",
                    "product_region",
                    "tags",
                ],
                dropna=False,
            ).agg({"usage_consumedquantity": ["sum"]})
        columns = daily_data_frame.columns.droplevel(1)
        daily_data_frame.columns = columns
        daily_data_frame.reset_index(inplace=True)

        return daily_data_frame

    def _add_missing_columns_with_dtypes(self, data_frame):
        """Adds the missing columns with the correct dtypes."""
        if check_all_parquet_files_corrected():
            raw_columns = data_frame.columns.tolist()
            missing_columns = [col for col in TRINO_REQUIRED_COLUMNS if col not in raw_columns]
            for raw_column in missing_columns:
                cleaned_column = strip_characters_from_column_name(raw_column)
                if cleaned_column in trino_schema.NUMERIC_COLUMNS:
                    data_frame[raw_column] = data_frame[raw_column].astype(float)
                elif cleaned_column in trino_schema.BOOLEAN_COLUMNS:
                    data_frame[raw_column] = data_frame[raw_column].astype(bool)
                elif cleaned_column in trino_schema.DATE_COLUMNS:
                    data_frame[raw_column] = pd.to_datetime(data_frame[raw_column], errors="coerce")
                else:
                    data_frame[raw_column] = data_frame[raw_column].astype(str)
        return data_frame

    def process_dataframe(self, data_frame):
        """
        Consume the OCI data and add a column creating a dictionary for the oci tags
        """
        data_frame = self._add_missing_columns_with_dtypes(data_frame)
        columns = set(list(data_frame))
        columns = set(TRINO_REQUIRED_COLUMNS).union(columns)
        columns = sorted(list(columns))

        resource_tag_columns = [column for column in columns if "tags/" in column]
        unique_keys = {scrub_resource_col_name(column) for column in resource_tag_columns}
        self.enabled_tag_keys.update(unique_keys)
        tag_df = data_frame[resource_tag_columns]
        resource_tags_dict = tag_df.apply(
            lambda row: {scrub_resource_col_name(column): value for column, value in row.items() if value}, axis=1
        )
        resource_tags_dict = resource_tags_dict.where(resource_tags_dict.notna(), lambda _: [{}])

        data_frame["tags"] = resource_tags_dict.apply(json.dumps)
        # Make sure we have entries for our required columns
        data_frame = data_frame.reindex(columns=columns)

        columns = list(data_frame)
        column_name_map = {}
        drop_columns = []
        for column in columns:
            new_col_name = strip_characters_from_column_name(column)
            column_name_map[column] = new_col_name
            if "tags/" in column:
                drop_columns.append(column)
        data_frame = data_frame.drop(columns=drop_columns)
        data_frame = data_frame.rename(columns=column_name_map)
        return data_frame, self._generate_daily_data(data_frame)

    def finalize_post_processing(self):
        """
        Uses information gather in the post processing to update the cost models.
        """
        populate_enabled_tag_rows_with_limit(self.schema, self.enabled_tag_keys, Provider.PROVIDER_OCI)
