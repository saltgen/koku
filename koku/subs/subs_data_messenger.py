#
# Copyright 2023 Red Hat Inc.
# SPDX-License-Identifier: Apache-2.0
#
import csv
import json
import logging
import os
import uuid
from tempfile import mkdtemp

from django.conf import settings

from api.common import log_json
from api.iam.models import Customer
from api.provider.models import Provider
from kafka_utils.utils import delivery_callback
from kafka_utils.utils import get_producer
from kafka_utils.utils import SUBS_TOPIC
from masu.prometheus_stats import KAFKA_CONNECTION_ERRORS_COUNTER
from masu.util.aws.common import get_s3_resource
from providers.azure.client import AzureClientFactory


LOG = logging.getLogger(__name__)


class SUBSDataMessenger:
    def __init__(self, context, schema_name, tracing_id):
        self.provider_type = context["provider_type"].removesuffix("-local")
        self.context = context
        self.tracing_id = tracing_id
        self.schema_name = schema_name
        self.s3_resource = get_s3_resource(
            settings.S3_SUBS_ACCESS_KEY, settings.S3_SUBS_SECRET, settings.S3_SUBS_REGION
        )
        subs_cust = Customer.objects.get(schema_name=schema_name)
        self.account_id = subs_cust.account_id
        self.org_id = subs_cust.org_id
        self.download_path = mkdtemp(prefix="subs")
        self.instance_map = {}

    def determine_azure_instance_id(self, row):
        """For Azure we have to query the instance id if it is not provided via tag."""
        if row["resourceid"] in self.instance_map:
            return self.instance_map.get(row["resourceid"])
        # this column comes from a user defined tag allowing us to avoid querying Azure if its present.
        if row["subs_instance"] != "":
            instance_id = row["subs_instance"]
        # attempt to query azure for instance id
        else:
            prov = Provider.objects.get(uuid=row["source"])
            credentials = prov.account.get("credentials")
            subscription_id = credentials.get("subscription_id")
            tenant_id = credentials.get("tenant_id")
            client_id = credentials.get("client_id")
            client_secret = credentials.get("client_secret")
            _factory = AzureClientFactory(subscription_id, tenant_id, client_id, client_secret)
            compute_client = _factory.compute_client
            response = compute_client.virtual_machines.get(
                resource_group_name=row["resourcegroup"],
                vm_name=row["subs_resource_id"],
            )
            instance_id = response.vm_id

        self.instance_map[row["resourceid"]] = instance_id
        return instance_id

    def process_and_send_subs_message(self, upload_keys):
        """
        Takes a list of object keys, reads the objects from the S3 bucket and processes a message to kafka.
        """
        for i, obj_key in enumerate(upload_keys):
            csv_path = f"{self.download_path}/subs_{self.tracing_id}_{i}.csv"
            self.s3_resource.Bucket(settings.S3_SUBS_BUCKET_NAME).download_file(obj_key, csv_path)
            with open(csv_path) as csv_file:
                reader = csv.DictReader(csv_file)
                LOG.info(
                    log_json(
                        self.tracing_id,
                        msg="iterating over records and sending kafka messages",
                        context=self.context,
                    )
                )
                msg_count = 0
                for row in reader:
                    if self.provider_type == Provider.PROVIDER_AZURE:
                        instance_id = self.determine_azure_instance_id(row)
                        if not instance_id:
                            continue
                        row["subs_resource_id"] = instance_id
                    # row["subs_product_ids"] is a string of numbers separated by '-' to be sent as a list
                    msg = self.build_subs_msg(
                        row["subs_resource_id"],
                        row["subs_account"],
                        row["subs_start_time"],
                        row["subs_end_time"],
                        row["subs_vcpu"],
                        row["subs_sla"],
                        row["subs_usage"],
                        row["subs_role"],
                        row["subs_product_ids"].split("-"),
                    )
                    self.send_kafka_message(msg)
                    msg_count += 1
            LOG.info(
                log_json(
                    self.tracing_id,
                    msg=f"sent {msg_count} kafka messages for subs",
                    context=self.context,
                )
            )
            os.remove(csv_path)

    @KAFKA_CONNECTION_ERRORS_COUNTER.count_exceptions()
    def send_kafka_message(self, msg):
        """Sends a kafka message to the SUBS topic with the S3 keys for the uploaded reports."""
        producer = get_producer()
        producer.produce(SUBS_TOPIC, value=msg, callback=delivery_callback)
        producer.poll(0)

    def build_subs_msg(
        self, instance_id, billing_account_id, tstamp, expiration, cpu_count, sla, usage, role, product_ids
    ):
        """Gathers the relevant information for the kafka message and returns the message to be delivered."""
        subs_json = {
            "event_id": str(uuid.uuid4()),
            "event_source": "cost-management",
            "event_type": "snapshot",
            "account_number": self.account_id,
            "org_id": self.org_id,
            "service_type": "RHEL System",
            "instance_id": instance_id,
            "timestamp": tstamp,
            "expiration": expiration,
            "measurements": [{"value": cpu_count, "uom": "vCPUs"}],
            "cloud_provider": self.provider_type,
            "hardware_type": "Cloud",
            "product_ids": product_ids,
            "role": role,
            "sla": sla,
            "usage": usage,
            "billing_provider": "aws",
            "billing_account_id": billing_account_id,
        }
        return bytes(json.dumps(subs_json), "utf-8")
