#
# Copyright 2021 Red Hat Inc.
# SPDX-License-Identifier: Apache-2.0
#
"""OCP utility functions."""
import json
import logging
import os
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any
from typing import Union

import pandas as pd
from dateutil.relativedelta import relativedelta
from pydantic import BaseModel
from pydantic import field_validator

from api.models import Provider
from api.utils import DateHelper as dh


LOG = logging.getLogger(__name__)


class OCPReportTypes(Enum):
    """Types of OCP report files."""

    UNKNOWN = 0
    CPU_MEM_USAGE = 1
    STORAGE = 2
    NODE_LABELS = 3
    NAMESPACE_LABELS = 4


STORAGE_COLUMNS = {
    "report_period_start",
    "report_period_end",
    "interval_start",
    "interval_end",
    "namespace",
    "pod",
    "persistentvolumeclaim",
    "persistentvolume",
    "storageclass",
    "persistentvolumeclaim_capacity_bytes",
    "persistentvolumeclaim_capacity_byte_seconds",
    "volume_request_storage_byte_seconds",
    "persistentvolumeclaim_usage_byte_seconds",
    "persistentvolume_labels",
    "persistentvolumeclaim_labels",
}

STORAGE_GROUP_BY = [
    "namespace",
    "pod",
    "persistentvolumeclaim",
    "persistentvolume",
    "storageclass",
    "persistentvolume_labels",
    "persistentvolumeclaim_labels",
]

STORAGE_AGG = {
    "report_period_start": ["max"],
    "report_period_end": ["max"],
    "persistentvolumeclaim_capacity_bytes": ["max"],
    "persistentvolumeclaim_capacity_byte_seconds": ["sum"],
    "volume_request_storage_byte_seconds": ["sum"],
    "persistentvolumeclaim_usage_byte_seconds": ["sum"],
}

CPU_MEM_USAGE_COLUMNS = {
    "report_period_start",
    "report_period_end",
    "pod",
    "namespace",
    "node",
    "resource_id",
    "interval_start",
    "interval_end",
    "pod_usage_cpu_core_seconds",
    "pod_request_cpu_core_seconds",
    "pod_limit_cpu_core_seconds",
    "pod_usage_memory_byte_seconds",
    "pod_request_memory_byte_seconds",
    "pod_limit_memory_byte_seconds",
    "node_capacity_cpu_cores",
    "node_capacity_cpu_core_seconds",
    "node_capacity_memory_bytes",
    "node_capacity_memory_byte_seconds",
    "pod_labels",
}

CPU_MEM_USAGE_NEWV_COLUMNS = {
    "node_role",
}

POD_GROUP_BY = ["namespace", "node", "pod", "pod_labels"]

POD_AGG = {
    "report_period_start": ["max"],
    "report_period_end": ["max"],
    "resource_id": ["max"],
    "pod_usage_cpu_core_seconds": ["sum"],
    "pod_request_cpu_core_seconds": ["sum"],
    "pod_effective_usage_cpu_core_seconds": ["sum"],
    "pod_limit_cpu_core_seconds": ["sum"],
    "pod_usage_memory_byte_seconds": ["sum"],
    "pod_request_memory_byte_seconds": ["sum"],
    "pod_effective_usage_memory_byte_seconds": ["sum"],
    "pod_limit_memory_byte_seconds": ["sum"],
    "node_capacity_cpu_cores": ["max"],
    "node_capacity_cpu_core_seconds": ["sum"],
    "node_capacity_memory_bytes": ["max"],
    "node_capacity_memory_byte_seconds": ["sum"],
    "node_role": ["max"],
}

NODE_LABEL_COLUMNS = {
    "report_period_start",
    "report_period_end",
    "node",
    "interval_start",
    "interval_end",
    "node_labels",
}

NODE_GROUP_BY = ["node", "node_labels"]

NODE_AGG = {"report_period_start": ["max"], "report_period_end": ["max"]}


NAMESPACE_LABEL_COLUMNS = {
    "report_period_start",
    "report_period_end",
    "interval_start",
    "interval_end",
    "namespace",
    "namespace_labels",
}

NAMESPACE_GROUP_BY = ["namespace", "namespace_labels"]

NAMESPACE_AGG = {"report_period_start": ["max"], "report_period_end": ["max"]}

# new_required_columns are columns that appear in new operator reports.
# today, we cannot guarantee that all reports received will contain all
# of these new columns, so this field is used to add the necessary columns
# to the data frames.
OCP_REPORT_TYPES = {
    "storage_usage": {
        "columns": STORAGE_COLUMNS,
        "enum": OCPReportTypes.STORAGE,
        "group_by": STORAGE_GROUP_BY,
        "agg": STORAGE_AGG,
        "new_required_columns": [],
    },
    "pod_usage": {
        "columns": CPU_MEM_USAGE_COLUMNS,
        "enum": OCPReportTypes.CPU_MEM_USAGE,
        "group_by": POD_GROUP_BY,
        "agg": POD_AGG,
        "new_required_columns": CPU_MEM_USAGE_NEWV_COLUMNS,
    },
    "node_labels": {
        "columns": NODE_LABEL_COLUMNS,
        "enum": OCPReportTypes.NODE_LABELS,
        "group_by": NODE_GROUP_BY,
        "agg": NODE_AGG,
        "new_required_columns": [],
    },
    "namespace_labels": {
        "columns": NAMESPACE_LABEL_COLUMNS,
        "enum": OCPReportTypes.NAMESPACE_LABELS,
        "group_by": NAMESPACE_GROUP_BY,
        "agg": NAMESPACE_AGG,
        "new_required_columns": [],
    },
}


class OCPManifest(BaseModel):
    # Fields from the manifest payload
    uuid: str
    cluster_id: str
    version: str
    date: datetime
    files: Union[list[str], None]
    resource_optimization_files: Union[list[str], None]
    start: datetime
    end: datetime
    cr_status: dict[str, Any]
    certified: bool = False
    daily_reports: bool = False

    request_id: str

    current_file: os.PathLike = ""
    destination_dir: os.PathLike = ""
    process_complete: bool = False
    split_files: list[os.PathLike] = []
    ocp_files_to_process: dict = {}

    provider_uuid: str = ""
    provider_type: str = ""
    schema_name: str = ""
    s3_schema_name: str = ""
    account_id: str = ""
    org_id: str = ""

    source_id: str = ""
    manifest_id: int = 0

    class Config:
        validate_assignment = True
        coerce_numbers_to_str = True

    @field_validator("files", "resource_optimization_files")
    @classmethod
    def replace_none_with_empty_list(cls, v: Union[list[str], None]) -> list:
        return [] if v is None else v


class ManifestNotFound(Exception):
    pass


def get_ocp_manifest(manifest_filepath: os.PathLike, request_id: str) -> OCPManifest:
    """
    Get OCP usage report details from manifest file.

    Date range is aligned on the first day of the current
    month and ends on the first day of the next month from the
    specified date.

    Args:
        report_directory (String): base directory for report.

    Returns:
        OCPManifest

    """
    if not os.path.exists(manifest_filepath):
        msg = f"no manifest available at {manifest_filepath}"
        LOG.info(msg)
        raise ManifestNotFound(msg)

    with open(manifest_filepath) as file:
        payload_dict = json.load(file)

    return OCPManifest(request_id=request_id, **payload_dict)


def month_date_range(for_date_time):
    """
    Get a formatted date range string for the given date.

    Date range is aligned on the first day of the current
    month and ends on the first day of the next month from the
    specified date.

    Args:
        for_date_time (DateTime): The starting datetime object

    Returns:
        (String): "YYYYMMDD-YYYYMMDD", example: "19701101-19701201"

    """
    start_month = for_date_time.replace(day=1)
    end_month = start_month + relativedelta(months=+1)
    timeformat = "%Y%m%d"
    return f"{start_month.strftime(timeformat)}-{end_month.strftime(timeformat)}"


def get_local_file_name(file_path):
    """
    Return the local file name for a given report path.

    Args:
        file_path (String): report file path from manifest.

    Returns:
        (String): file name for the local file.

    """
    filename = file_path.split("/")[-1]
    date_range = file_path.split("/")[-2]
    local_file_name = f"{date_range}_{filename}"
    return local_file_name


def get_cluster_id_from_provider(provider_uuid):
    """
    Return the cluster ID given a provider UUID.

    Args:
        provider_uuid (String): provider UUID.

    Returns:
        (String): OpenShift Cluster ID

    """
    cluster_id = None
    if provider := Provider.objects.filter(uuid=provider_uuid).first():
        if not provider.authentication:
            LOG.warning(
                f"cannot find cluster-id for provider-uuid: {provider_uuid} because it does not have credentials"
            )
            return cluster_id
        cluster_id = provider.authentication.credentials.get("cluster_id")
        LOG.info(f"found cluster_id: {cluster_id} for provider-uuid: {provider_uuid}")
    return cluster_id


def get_cluster_alias_from_cluster_id(cluster_id):
    """
    Return the cluster alias of a given cluster id.

    Args:
        cluster_id (String): OpenShift Cluster ID

    Returns:
        (String): OpenShift Cluster Alias

    """
    cluster_alias = None
    credentials = {"cluster_id": cluster_id}
    if provider := Provider.objects.filter(authentication__credentials=credentials).first():
        cluster_alias = provider.name
        LOG.info(f"found cluster_alias: {cluster_alias} for cluster-id: {cluster_id}")

    return cluster_alias


def get_provider_from_cluster_id(cluster_id):
    """
    Return the provider UUID given the cluster ID.

    Args:
        cluster_id (String): OpenShift Cluster ID

    Returns:
        (Provider): provider

    """
    credentials = {"cluster_id": cluster_id}
    if provider := Provider.objects.filter(authentication__credentials=credentials).first():
        LOG.info(f"found provider: {provider.uuid} for cluster-id: {cluster_id}")
    return provider


def detect_type(report_path):
    """
    Detects the OCP report type.
    """
    columns = pd.read_csv(report_path, nrows=0).columns
    for report_type, report_def in OCP_REPORT_TYPES.items():
        report_columns = report_def.get("columns")
        if report_columns.issubset(columns):
            return report_type, report_def.get("enum")
    return None, OCPReportTypes.UNKNOWN


def match_openshift_labels(tag_dict, matched_tags):
    """Match AWS data by OpenShift label associated with OpenShift cluster."""
    tag_dict = json.loads(tag_dict)
    tag_matches = []
    for key, value in tag_dict.items():
        if not value:
            continue
        lower_tag = {key.lower(): value.lower()}
        if lower_tag in matched_tags:
            tag = json.dumps(lower_tag).replace("{", "").replace("}", "")
            tag_matches.append(tag)
    return ",".join(tag_matches)


def get_amortized_monthly_cost_model_rate(monthly_rate, start_date):
    """Given a monthly rate, determine the per-day amortized rate."""
    if monthly_rate is None:
        return None

    days_in_month = dh().days_in_month(start_date)
    return Decimal(monthly_rate) / days_in_month
