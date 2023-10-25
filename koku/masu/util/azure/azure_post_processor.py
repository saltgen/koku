import json
import logging

import pandas

from api.models import Provider
from masu.external.downloader.azure.azure_csv_reader import AzureCSVReader
from masu.util.azure.common import INGRESS_ALT_COLUMNS
from masu.util.azure.common import INGRESS_REQUIRED_COLUMNS
from masu.util.common import populate_enabled_tag_rows_with_limit
from masu.util.common import strip_characters_from_column_name
from masu.util.common import verify_data_types_in_parquet_file
from reporting.provider.azure.models import TRINO_COLUMNS

LOG = logging.getLogger(__name__)


class AzurePostProcessor:
    def __init__(self, schema, csv_filepath):
        self.schema = schema
        self.enabled_tag_keys = set()
        self.csv_reader = AzureCSVReader(csv_filepath)

    def check_ingress_required_columns(self):
        """
        Checks the required columns for ingress.
        """
        if not set(self.csv_reader.column_names).issuperset(INGRESS_REQUIRED_COLUMNS):
            if not set(self.csv_reader.column_names).issuperset(INGRESS_ALT_COLUMNS):
                missing_columns = [x for x in INGRESS_REQUIRED_COLUMNS if x not in self.csv_reader.column_names]
                return missing_columns
        return None

    def _generate_daily_data(self, data_frame):
        """
        Generate daily data.
        """
        return data_frame

    def process_dataframe(self, data_frame, parquet_filepath):
        columns = list(data_frame)
        column_name_map = {}

        for column in columns:
            new_col_name = strip_characters_from_column_name(column)
            column_name_map[column] = new_col_name

        data_frame = data_frame.rename(columns=column_name_map)

        columns = set(data_frame)
        columns = set(TRINO_COLUMNS).union(columns)
        columns = sorted(columns)

        data_frame = data_frame.reindex(columns=columns)

        unique_tags = set()
        for tags_json in data_frame["tags"].values:
            if pandas.notnull(tags_json):
                unique_tags.update(json.loads(tags_json))
        self.enabled_tag_keys.update(unique_tags)
        data_frame.to_parquet(parquet_filepath, allow_truncated_timestamps=True, coerce_timestamps="ms", index=False)
        verify_data_types_in_parquet_file(
            parquet_filepath, self.csv_reader.numeric_columns, self.csv_reader.date_columns
        )
        return self._generate_daily_data(data_frame)

    def finalize_post_processing(self):
        """
        Uses information gather in the post processing to update the cost models.
        """
        populate_enabled_tag_rows_with_limit(self.schema, self.enabled_tag_keys, Provider.PROVIDER_AZURE)
