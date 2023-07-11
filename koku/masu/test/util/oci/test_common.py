#
# Copyright 2022 Red Hat Inc.
# SPDX-License-Identifier: Apache-2.0
#
import random
import shutil
import tempfile
from datetime import datetime

from dateutil.relativedelta import relativedelta
from django_tenants.utils import schema_context
from faker import Faker

from masu.database.oci_report_db_accessor import OCIReportDBAccessor
from masu.database.provider_db_accessor import ProviderDBAccessor
from masu.external import OCI_REGIONS
from masu.external.date_accessor import DateAccessor
from masu.test import MasuTestCase
from masu.util.oci import common as utils
from reporting.models import OCICostEntryBill


# the cn endpoints aren't supported by moto, so filter them out
OCI_REGIONS = list(filter(lambda reg: not reg.startswith("cn-"), OCI_REGIONS))
REGION = random.choice(OCI_REGIONS)

NAME = Faker().word()
BUCKET = Faker().word()
PREFIX = Faker().word()
FORMAT = random.choice(["text", "csv"])
COMPRESSION = random.choice(["ZIP", "GZIP"])


class TestOCIUtils(MasuTestCase):
    """Tests for OCI utilities."""

    fake = Faker()

    def setUp(self):
        """Set up the test."""
        super().setUp()
        self.start_date = datetime(year=2020, month=11, day=8).date()
        self.invoice = "202011"
        self.etag = "reports_cost-csv_0001000000603747.csv"
        test_report = "./koku/masu/test/data/oci/reports_cost-csv_0001000000603747.csv"
        self.local_storage = tempfile.mkdtemp()
        local_dir = f"{self.local_storage}"
        self.csv_file_name = test_report.split("/")[-1]
        self.csv_file_path = f"{local_dir}/{self.csv_file_name}"
        shutil.copy2(test_report, self.csv_file_path)

    def test_get_bill_ids_from_provider(self):
        """Test that bill IDs are returned for an OCI provider."""
        with schema_context(self.schema):
            expected_bill_ids = OCICostEntryBill.objects.values_list("id")
            expected_bill_ids = sorted(bill_id[0] for bill_id in expected_bill_ids)
        bills = utils.get_bills_from_provider(self.oci_provider_uuid, self.schema)

        with schema_context(self.schema):
            bill_ids = sorted(bill.id for bill in bills)

        self.assertEqual(bill_ids, expected_bill_ids)

        # Try with unknown provider uuid
        bills = utils.get_bills_from_provider(self.unkown_test_provider_uuid, self.schema)
        self.assertEqual(bills, [])

    def test_get_bill_ids_from_provider_with_start_date(self):
        """Test that bill IDs are returned for an OCI provider with start date."""
        date_accessor = DateAccessor()

        with ProviderDBAccessor(provider_uuid=self.oci_provider_uuid) as provider_accessor:
            provider = provider_accessor.get_provider()
        with OCIReportDBAccessor(schema=self.schema) as accessor:

            end_date = date_accessor.today_with_timezone("UTC").replace(day=1)
            start_date = end_date
            for i in range(2):
                start_date = start_date - relativedelta(months=i)

            bills = accessor.get_cost_entry_bills_query_by_provider(provider.uuid)
            with schema_context(self.schema):
                bills = bills.filter(billing_period_start__gte=end_date.date()).all()
                expected_bill_ids = [str(bill.id) for bill in bills]

        bills = utils.get_bills_from_provider(self.oci_provider_uuid, self.schema, start_date=end_date)
        with schema_context(self.schema):
            bill_ids = [str(bill.id) for bill in bills]

        self.assertEqual(bill_ids, expected_bill_ids)

    def test_get_bill_ids_from_provider_with_end_date(self):
        """Test that bill IDs are returned for an OCI provider with end date."""
        date_accessor = DateAccessor()

        with ProviderDBAccessor(provider_uuid=self.oci_provider_uuid) as provider_accessor:
            provider = provider_accessor.get_provider()
        with OCIReportDBAccessor(schema=self.schema) as accessor:

            end_date = date_accessor.today_with_timezone("UTC").replace(day=1)
            start_date = end_date
            for i in range(2):
                start_date = start_date - relativedelta(months=i)

            bills = accessor.get_cost_entry_bills_query_by_provider(provider.uuid)
            with schema_context(self.schema):
                bills = bills.filter(billing_period_start__lte=start_date.date()).all()
                expected_bill_ids = [str(bill.id) for bill in bills]

        bills = utils.get_bills_from_provider(self.oci_provider_uuid, self.schema, end_date=start_date)
        with schema_context(self.schema):
            bill_ids = [str(bill.id) for bill in bills]

        self.assertEqual(bill_ids, expected_bill_ids)

    def test_get_bill_ids_from_provider_with_start_and_end_date(self):
        """Test that bill IDs are returned for an OCI provider with both dates."""
        date_accessor = DateAccessor()

        with ProviderDBAccessor(provider_uuid=self.oci_provider_uuid) as provider_accessor:
            provider = provider_accessor.get_provider()
        with OCIReportDBAccessor(schema=self.schema) as accessor:

            end_date = date_accessor.today_with_timezone("UTC").replace(day=1)
            start_date = end_date
            for i in range(2):
                start_date = start_date - relativedelta(months=i)

            bills = accessor.get_cost_entry_bills_query_by_provider(provider.uuid)
            with schema_context(self.schema):
                bills = (
                    bills.filter(billing_period_start__gte=start_date.date())
                    .filter(billing_period_start__lte=end_date.date())
                    .all()
                )
                expected_bill_ids = [str(bill.id) for bill in bills]

        bills = utils.get_bills_from_provider(
            self.oci_provider_uuid, self.schema, start_date=start_date, end_date=end_date
        )
        with schema_context(self.schema):
            bill_ids = [str(bill.id) for bill in bills]

        self.assertEqual(bill_ids, expected_bill_ids)

    def test_detect_type(self):
        "Test that we detect the correct report type from csv"
        expected_result = "cost"
        result = utils.detect_type(self.csv_file_path)
        self.assertEqual(result, expected_result)

    def test_deduplicate_reports_for_oci_diff_manifest_ids(self):
        """Test that duplicate reports with same start and end pairs are removed"""
        expected_manifest_ids = [1, 2]
        test_report_list = [
            {
                "schema_name": "org1234567",
                "provider_type": "OCI",
                "provider_uuid": "ee89ec76-701a-4359-bf27-c97ea6742c83",
                "manifest_id": expected_manifest_ids[0],
                "tracing_id": "ee89ec76-701a-4359-bf27-c97ea6742c83:202211",
                "start": "2022-12-01",
                "end": "2022-12-31",
                "invoice_month": None,
            },
            {
                "schema_name": "org1234567",
                "provider_type": "OCI",
                "provider_uuid": "ee89ec76-701a-4359-bf27-c97ea6742c83",
                "manifest_id": expected_manifest_ids[0],
                "tracing_id": "ee89ec76-701a-4359-bf27-c97ea6742c83:202211",
                "start": "2022-12-01",
                "end": "2022-12-31",
                "invoice_month": None,
            },
            {
                "schema_name": "org1234567",
                "provider_type": "OCI",
                "provider_uuid": "ee89ec76-701a-4359-bf27-c97ea6742c83",
                "manifest_id": expected_manifest_ids[1],
                "tracing_id": "ee89ec76-701a-4359-bf27-c97ea6742c83:202212",
                "start": "2023-01-01",
                "end": "2023-01-04",
                "invoice_month": None,
            },
            {
                "schema_name": "org1234567",
                "provider_type": "OCI",
                "provider_uuid": "ee89ec76-701a-4359-bf27-c97ea6742c83",
                "manifest_id": expected_manifest_ids[1],
                "tracing_id": "ee89ec76-701a-4359-bf27-c97ea6742c83:202212",
                "start": "2023-01-01",
                "end": "2023-01-04",
                "invoice_month": None,
            },
        ]
        manifest_list = utils.deduplicate_reports_for_oci(test_report_list)
        result_manifest_ids = {
            manifest.get("manifest_id") for manifest in manifest_list if manifest.get("manifest_id")
        }
        self.assertEqual(list(result_manifest_ids), expected_manifest_ids)

    def test_deduplicate_reports_for_oci_with_diff_start_end_pair(self):
        """
        Test that duplicate reports with same manifest_id but different start and end date pairs are merged
        """
        test_report_list = [
            {
                "manifest_id": 16,
                "tracing_id": "f0029174-c191-4d99-b381-1e8355f00762: 202302",
                "schema_name": "org1234567",
                "provider_type": "OCI",
                "provider_uuid": "f0029174-c191-4d99-b381-1e8355f00762",
                "start": "2022-12-01",
                "end": "2022-12-31",
            },
            {
                "manifest_id": 16,
                "tracing_id": "f0029174-c191-4d99-b381-1e8355f00762: 202302",
                "schema_name": "org1234567",
                "provider_type": "OCI",
                "provider_uuid": "f0029174-c191-4d99-b381-1e8355f00762",
                "start": "2023-01-01",
                "end": "2023-01-31",
            },
            {
                "manifest_id": 16,
                "tracing_id": "f0029174-c191-4d99-b381-1e8355f00762: 202302",
                "schema_name": "org1234567",
                "provider_type": "OCI",
                "provider_uuid": "f0029174-c191-4d99-b381-1e8355f00762",
                "start": "2023-02-01",
                "end": "2023-02-03",
            },
            {
                "manifest_id": 16,
                "tracing_id": "f0029174-c191-4d99-b381-1e8355f00762: 202302",
                "schema_name": "org1234567",
                "provider_type": "OCI",
                "provider_uuid": "f0029174-c191-4d99-b381-1e8355f00762",
                "start": "2022-12-01",
                "end": "2022-12-31",
            },
            {
                "manifest_id": 16,
                "tracing_id": "f0029174-c191-4d99-b381-1e8355f00762: 202302",
                "schema_name": "org1234567",
                "provider_type": "OCI",
                "provider_uuid": "f0029174-c191-4d99-b381-1e8355f00762",
                "start": "2023-01-01",
                "end": "2023-01-31",
            },
            {
                "manifest_id": 16,
                "tracing_id": "f0029174-c191-4d99-b381-1e8355f00762: 202302",
                "schema_name": "org1234567",
                "provider_type": "OCI",
                "provider_uuid": "f0029174-c191-4d99-b381-1e8355f00762",
                "start": "2023-02-01",
                "end": "2023-02-03",
            },
        ]

        expected_report_list = [
            {
                "manifest_id": 16,
                "tracing_id": "f0029174-c191-4d99-b381-1e8355f00762: 202302",
                "schema_name": "org1234567",
                "provider_type": "OCI",
                "provider_uuid": "f0029174-c191-4d99-b381-1e8355f00762",
                "start": "2022-12-01",
                "end": "2023-02-03",
            },
        ]

        returned_report_list = utils.deduplicate_reports_for_oci(test_report_list)
        self.assertEqual(returned_report_list, expected_report_list)
