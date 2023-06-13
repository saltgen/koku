#
# Copyright 2021 Red Hat Inc.
# SPDX-License-Identifier: Apache-2.0
#
"""Asynchronous tasks."""
import logging

import psutil

from api.common import log_json
from masu.config import Config
from masu.external.report_downloader import ReportDownloader

LOG = logging.getLogger(__name__)


# disabled until the program flow stabilizes a bit more
# pylint: disable=too-many-arguments,too-many-locals
def _get_report_files(
    tracing_id,
    customer_name,
    authentication,
    billing_source,
    provider_type,
    provider_uuid,
    report_month,
    report_context,
    ingress_reports=None,
    ingress_report_counter=None,
):
    """
    Task to download a Report.

    Args:
        tracing_id        (String): Tracing ID for file processing.
        customer_name     (String): Name of the customer owning the cost usage report.
        access_credential (String): Credential needed to access cost usage report
                                    in the backend provider.
        report_source     (String): Location of the cost usage report in the backend provider.
        provider_type     (String): Koku defined provider type string.  Example: Amazon = 'AWS'
        provider_uuid     (String): Provider uuid.
        report_month      (DateTime): Month for report to download.
        cache_key         (String): The provider specific task cache value.

    Returns:
        files (List) List of filenames with full local path.
               Example: ['/var/tmp/masu/region/aws/catch-clearly.csv',
                         '/var/tmp/masu/base/aws/professor-hour-industry-television.csv']

    """
    # Existing schema will start with acct and we strip that prefix for use later
    # new customers include the org prefix in case an org-id and an account number might overlap
    context = {
        "schema": customer_name,
        "provider_uuid": provider_uuid,
        "provider_type": provider_type,
        "date": report_month,
        "report_month": report_month.strftime("%B %Y"),
    }
    if customer_name.startswith("acct"):
        context["account"] = customer_name[4:]
        download_acct = customer_name[4:]
    else:
        context["org_id"] = customer_name[3:]
        download_acct = customer_name

    report_context["date"] = report_month
    function_name = "masu.processor._tasks.download._get_report_files"
    LOG.info(log_json(tracing_id, msg="downloading report", context=context))
    try:
        disk = psutil.disk_usage(Config.DATA_DIR)
        disk_msg = f"{function_name}: Available disk space: {disk.free} bytes ({100 - disk.percent}%)"
    except OSError:
        disk_msg = f"{function_name}: Unable to find available disk space. {Config.DATA_DIR} does not exist"
    LOG.info(log_json(tracing_id, msg=disk_msg, context=context))

    downloader = ReportDownloader(
        customer_name=customer_name,
        credentials=authentication,
        data_source=billing_source,
        provider_type=provider_type,
        provider_uuid=provider_uuid,
        report_name=None,
        ingress_reports=ingress_reports,
        ingress_report_counter=ingress_report_counter,
        account=download_acct,
        tracing_id=tracing_id,
    )
    return downloader.download_report(report_context)
