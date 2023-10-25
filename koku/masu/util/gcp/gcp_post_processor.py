import json
import logging
from json.decoder import JSONDecodeError

import pandas as pd

from api.models import Provider
from masu.external.downloader.gcp.gcp_csv_reader import GCPCSVReader
from masu.util.common import populate_enabled_tag_rows_with_false
from masu.util.common import strip_characters_from_column_name
from masu.util.common import verify_data_types_in_parquet_file

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


class GCPPostProcessor:

    INGRESS_REQUIRED_COLUMNS = {
        "billing_account_id",
        "service.id",
        "service.description",
        "sku.id",
        "sku.description",
        "usage_start_time",
        "usage_end_time",
        "project.id",
        "project.name",
        "project.ancestry_numbers",
        "location.location",
        "location.country",
        "location.region",
        "location.zone",
        "export_time",
        "cost",
        "currency",
        "currency_conversion_rate",
        "usage.amount",
        "usage.unit",
        "usage.amount_in_pricing_units",
        "usage.pricing_unit",
        "credits",
        "invoice.month",
        "cost_type",
        "partition_date",
    }

    def __init__(self, schema, csv_filepath):
        self.schema = schema
        self.enabled_tag_keys = set()
        self.csv_reader = GCPCSVReader(csv_filepath)

    def check_ingress_required_columns(self, col_names):
        """
        Checks the required columns for ingress.
        """
        if not set(col_names).issuperset(self.INGRESS_REQUIRED_COLUMNS):
            missing_columns = [x for x in self.INGRESS_REQUIRED_COLUMNS if x not in col_names]
            return missing_columns
        return None

    def _generate_daily_data(self, data_frame):
        """
        Generate daily data.
        """
        """Given a dataframe, return the data frame if its empty, group the data to create daily data."""
        if data_frame.empty:
            return data_frame

        # this parses the credits column into just the dollar amount so we can sum it up for daily rollups
        rollup_frame = data_frame.copy()
        rollup_frame["credits"] = rollup_frame["credits"].apply(json.loads)
        rollup_frame["daily_credits"] = 0.0
        for i, credit_dict in enumerate(rollup_frame["credits"]):
            rollup_frame["daily_credits"][i] = credit_dict.get("amount", 0.0)
        resource_df = rollup_frame.get("resource_name")
        try:
            if not resource_df:
                rollup_frame["resource_name"] = ""
                rollup_frame["resource_global_name"] = ""
        except Exception:
            if not resource_df.any():
                rollup_frame["resource_name"] = ""
                rollup_frame["resource_global_name"] = ""
        daily_data_frame = rollup_frame.groupby(
            [
                "invoice_month",
                "billing_account_id",
                "project_id",
                pd.Grouper(key="usage_start_time", freq="D"),
                "service_id",
                "sku_id",
                "system_labels",
                "labels",
                "cost_type",
                "location_region",
                "resource_name",
            ],
            dropna=False,
        ).agg(
            {
                "project_name": ["max"],
                "service_description": ["max"],
                "sku_description": ["max"],
                "usage_pricing_unit": ["max"],
                "usage_amount_in_pricing_units": ["sum"],
                "currency": ["max"],
                "cost": ["sum"],
                "daily_credits": ["sum"],
                "resource_global_name": ["max"],
            }
        )
        columns = daily_data_frame.columns.droplevel(1)
        daily_data_frame.columns = columns
        daily_data_frame.reset_index(inplace=True)

        return daily_data_frame

    def process_dataframe(self, data_frame, parquet_filepath):
        """Guarantee column order for GCP parquet files"""
        columns = list(data_frame)
        column_name_map = {}
        for column in columns:
            new_col_name = strip_characters_from_column_name(column)
            column_name_map[column] = new_col_name
        data_frame = data_frame.rename(columns=column_name_map)
        label_set = set()
        unique_labels = data_frame.labels.unique()
        for label in unique_labels:
            label_set.update(json.loads(label).keys())
        self.enabled_tag_keys.update(label_set)
        data_frame.to_parquet(
            parquet_filepath,
            allow_truncated_timestamps=True,
            coerce_timestamps="ms",
            index=False,
            dtype=self.data_types,
        )
        verify_data_types_in_parquet_file(
            parquet_filepath, self.csv_reader.numeric_columns, self.csv_reader.date_columns
        )
        return self._generate_daily_data(data_frame)

    def finalize_post_processing(self):
        """
        Uses information gather in the post processing to update the cost models.
        """
        populate_enabled_tag_rows_with_false(self.schema, self.enabled_tag_keys, Provider.PROVIDER_GCP)
