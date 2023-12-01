#
# Copyright 2021 Red Hat Inc.
# SPDX-License-Identifier: Apache-2.0
#
"""Processor for Azure Parquet files."""
import ciso8601
from django.conf import settings
from django_tenants.utils import schema_context

from masu.processor.report_parquet_processor_base import ReportParquetProcessorBase
from masu.util import common as utils
from reporting.provider.azure.models import AzureCostEntryBill
from reporting.provider.azure.models import AzureCostEntryLineItemDailySummary
from reporting.provider.azure.models import TRINO_LINE_ITEM_TABLE
from reporting.provider.azure.models import TRINO_OCP_ON_AZURE_DAILY_TABLE


class AzureReportParquetProcessor(ReportParquetProcessorBase):
    NUMERIC_COLUMNS = (
        "usagequantity",
        "quantity",
        "resourcerate",
        "pretaxcost",
        "costinbillingcurrency",
        "effectiveprice",
        "unitprice",
        "paygprice",
    )
    DATE_COLUMNS = ("usagedatetime", "date", "billingperiodstartdate", "billingperiodenddate")
    BOOLEAN_COLUMNS = "resource_id_matched"
    JSON_COLUMNS = ("tags", "additionalinfo")
    CREDITS = ()

    def __init__(self, manifest_id, account, s3_path, provider_uuid, parquet_local_path):

        column_types = {
            "numeric_columns": self.NUMERIC_COLUMNS,
            "date_columns": self.DATE_COLUMNS,
            "boolean_columns": self.BOOLEAN_COLUMNS,
        }
        if "openshift" in s3_path:
            table_name = TRINO_OCP_ON_AZURE_DAILY_TABLE
        else:
            table_name = TRINO_LINE_ITEM_TABLE
        super().__init__(
            manifest_id=manifest_id,
            account=account,
            s3_path=s3_path,
            provider_uuid=provider_uuid,
            parquet_local_path=parquet_local_path,
            column_types=column_types,
            table_name=table_name,
        )

    @property
    def postgres_summary_table(self):
        """Return the mode for the source specific summary table."""
        return AzureCostEntryLineItemDailySummary

    def create_bill(self, bill_date):
        """Create bill postgres entry."""
        if isinstance(bill_date, str):
            bill_date = ciso8601.parse_datetime(bill_date)
        report_date_range = utils.month_date_range(bill_date)
        start_date, end_date = report_date_range.split("-")

        start_date_utc = ciso8601.parse_datetime(start_date).replace(hour=0, minute=0, tzinfo=settings.UTC)
        end_date_utc = ciso8601.parse_datetime(end_date).replace(hour=0, minute=0, tzinfo=settings.UTC)

        provider = self._get_provider()

        with schema_context(self._schema_name):
            AzureCostEntryBill.objects.get_or_create(
                billing_period_start=start_date_utc,
                billing_period_end=end_date_utc,
                provider_id=provider.uuid,
            )
