#
# Copyright 2023 Red Hat Inc.
# SPDX-License-Identifier: Apache-2.0
#
import json
import uuid
from unittest.mock import mock_open
from unittest.mock import patch

from subs.subs_data_messenger import SUBSDataMessenger
from subs.test import SUBSTestCase


class TestSUBSDataMessenger(SUBSTestCase):
    """Test class for the SUBSDataMessenger"""

    @classmethod
    def setUpClass(cls):
        """Set up the class."""
        super().setUpClass()
        cls.context = {"some": "context", "provider_type": "AWS-local"}
        cls.azure_context = {"some": "context", "provider_type": "Azure-local"}
        cls.tracing_id = "trace_me"
        with patch("subs.subs_data_messenger.get_s3_resource"):
            cls.messenger = SUBSDataMessenger(cls.context, cls.schema, cls.tracing_id)
            cls.azure_messenger = SUBSDataMessenger(cls.azure_context, cls.schema, cls.tracing_id)

    @patch("subs.subs_data_messenger.os.remove")
    @patch("subs.subs_data_messenger.get_producer")
    @patch("subs.subs_data_messenger.csv.DictReader")
    @patch("subs.subs_data_messenger.SUBSDataMessenger.build_subs_msg")
    def test_process_and_send_subs_message(self, mock_msg_builder, mock_reader, mock_producer, mock_remove):
        """Tests that the proper functions are called when running process_and_send_subs_message"""
        upload_keys = ["fake_key"]
        mock_reader.return_value = [
            {
                "subs_start_time": "2023-07-01T01:00:00Z",
                "subs_end_time": "2023-07-01T02:00:00Z",
                "subs_resource_id": "i-55555556",
                "subs_account": "9999999999999",
                "physical_cores": "1",
                "subs_vcpu": "2",
                "variant": "Server",
                "subs_usage": "Production",
                "subs_sla": "Premium",
                "subs_role": "Red Hat Enterprise Linux Server",
                "subs_product_ids": "479-70",
            }
        ]
        mock_op = mock_open(read_data="x,y,z")
        with patch("builtins.open", mock_op):
            self.messenger.process_and_send_subs_message(upload_keys)
        mock_msg_builder.assert_called_once()
        mock_producer.assert_called_once()

    def test_build_subs_msg(self):
        """
        Test building the kafka message body
        """
        lineitem_resourceid = "i-55555556"
        lineitem_usagestartdate = "2023-07-01T01:00:00Z"
        lineitem_usageenddate = "2023-07-01T02:00:00Z"
        lineitem_usageaccountid = "9999999999999"
        product_vcpu = "2"
        usage = "Production"
        rol = "Red Hat Enterprise Linux Server"
        sla = "Premium"
        product_ids = ["479", "70"]
        static_uuid = uuid.uuid4()
        expected_subs_json = {
            "event_id": str(static_uuid),
            "event_source": "cost-management",
            "event_type": "snapshot",
            "account_number": self.acct,
            "org_id": self.org_id,
            "service_type": "RHEL System",
            "instance_id": lineitem_resourceid,
            "timestamp": lineitem_usagestartdate,
            "expiration": lineitem_usageenddate,
            "measurements": [{"value": product_vcpu, "uom": "vCPUs"}],
            "cloud_provider": "AWS",
            "hardware_type": "Cloud",
            "product_ids": product_ids,
            "role": rol,
            "sla": sla,
            "usage": usage,
            "billing_provider": "aws",
            "billing_account_id": lineitem_usageaccountid,
        }
        expected = bytes(json.dumps(expected_subs_json), "utf-8")
        with patch("subs.subs_data_messenger.uuid.uuid4") as mock_uuid:
            mock_uuid.return_value = static_uuid
            actual = self.messenger.build_subs_msg(
                lineitem_resourceid,
                lineitem_usageaccountid,
                lineitem_usagestartdate,
                lineitem_usageenddate,
                product_vcpu,
                sla,
                usage,
                rol,
                product_ids,
            )
        self.assertEqual(expected, actual)

    @patch("subs.subs_data_messenger.get_producer")
    def test_send_kafka_message(self, mock_producer):
        """Test that we would try to send a kafka message"""
        kafka_msg = {"test"}
        self.messenger.send_kafka_message(kafka_msg)
        mock_producer.assert_called()

    def test_determine_azure_instance_id_tag(self):
        """Test getting the azure instance id from the row provided by a tag returns as expected."""
        expected_instance = "waffle-house"
        self.messenger.instance_map = {}
        my_row = {
            "resourceid": "i-55555556",
            "subs_start_time": "2023-07-01T01:00:00Z",
            "subs_end_time": "2023-07-01T02:00:00Z",
            "subs_resource_id": "i-55555556",
            "subs_account": "9999999999999",
            "physical_cores": "1",
            "subs_vcpu": "2",
            "variant": "Server",
            "subs_usage": "Production",
            "subs_sla": "Premium",
            "subs_role": "Red Hat Enterprise Linux Server",
            "subs_product_ids": "479-70",
            "subs_instance": expected_instance,
        }
        actual = self.messenger.determine_azure_instance_id(my_row)
        self.assertEqual(expected_instance, actual)

    def test_determine_azure_instance_id_from_map(self):
        """Test getting the azure instance id from the instance map returns as expected."""
        expected = "oh-yeah"
        self.messenger.instance_map["i-55555556"] = expected
        my_row = {
            "resourceid": "i-55555556",
            "subs_start_time": "2023-07-01T01:00:00Z",
            "subs_end_time": "2023-07-01T02:00:00Z",
            "subs_resource_id": "i-55555556",
            "subs_account": "9999999999999",
            "physical_cores": "1",
            "subs_vcpu": "2",
            "variant": "Server",
            "subs_usage": "Production",
            "subs_sla": "Premium",
            "subs_role": "Red Hat Enterprise Linux Server",
            "subs_product_ids": "479-70",
            "subs_instance": "fake",
        }
        actual = self.messenger.determine_azure_instance_id(my_row)
        self.assertEqual(expected, actual)

    def test_determine_azure_instance_id(self):
        """Test getting the azure instance id from mock Azure Compute Client returns as expected."""
        expected = "my-fake-id"
        self.messenger.instance_map = {}
        my_row = {
            "resourceid": "i-55555556",
            "subs_start_time": "2023-07-01T01:00:00Z",
            "subs_end_time": "2023-07-01T02:00:00Z",
            "subs_resource_id": "i-55555556",
            "subs_account": "9999999999999",
            "physical_cores": "1",
            "subs_vcpu": "2",
            "variant": "Server",
            "subs_usage": "Production",
            "subs_sla": "Premium",
            "subs_role": "Red Hat Enterprise Linux Server",
            "subs_product_ids": "479-70",
            "subs_instance": "",
            "source": self.azure_provider.uuid,
            "resourcegroup": "my-fake-rg",
        }
        with patch("subs.subs_data_messenger.AzureClientFactory") as mock_factory:
            mock_factory.return_value.compute_client.virtual_machines.get.return_value.vm_id = expected
            actual = self.messenger.determine_azure_instance_id(my_row)
        self.assertEqual(expected, actual)

    @patch("subs.subs_data_messenger.SUBSDataMessenger.determine_azure_instance_id")
    @patch("subs.subs_data_messenger.os.remove")
    @patch("subs.subs_data_messenger.get_producer")
    @patch("subs.subs_data_messenger.csv.DictReader")
    @patch("subs.subs_data_messenger.SUBSDataMessenger.build_subs_msg")
    def test_process_and_send_subs_message_azure_with_id(
        self, mock_msg_builder, mock_reader, mock_producer, mock_remove, mock_azure_id
    ):
        """Tests that the proper functions are called when running process_and_send_subs_message with Azure provider."""
        upload_keys = ["fake_key"]
        mock_reader.return_value = [
            {
                "resourceid": "i-55555556",
                "subs_start_time": "2023-07-01T01:00:00Z",
                "subs_end_time": "2023-07-01T02:00:00Z",
                "subs_resource_id": "i-55555556",
                "subs_account": "9999999999999",
                "physical_cores": "1",
                "subs_vcpu": "2",
                "variant": "Server",
                "subs_usage": "Production",
                "subs_sla": "Premium",
                "subs_role": "Red Hat Enterprise Linux Server",
                "subs_product_ids": "479-70",
                "subs_instance": "",
                "source": self.azure_provider.uuid,
                "resourcegroup": "my-fake-rg",
            }
        ]
        mock_op = mock_open(read_data="x,y,z")
        with patch("builtins.open", mock_op):
            self.azure_messenger.process_and_send_subs_message(upload_keys)
        mock_azure_id.assert_called_once()
        mock_msg_builder.assert_called_once()
        mock_producer.assert_called_once()
