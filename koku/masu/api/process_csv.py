#
# Copyright 2023 Red Hat Inc.
# SPDX-License-Identifier: Apache-2.0
#
"""View for expired_data endpoint."""
import logging

from django.views.decorators.cache import never_cache
from rest_framework import status
from rest_framework.decorators import api_view
from rest_framework.decorators import permission_classes
from rest_framework.decorators import renderer_classes
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework.settings import api_settings

from api.provider.models import Provider
from masu.processor.orchestrator import Orchestrator
from masu.processor.tasks import GET_REPORT_FILES_QUEUE
from masu.processor.tasks import QUEUE_LIST
from masu.util.common import DateHelper

LOG = logging.getLogger(__name__)


@never_cache
@api_view(http_method_names=["GET", "DELETE"])
@permission_classes((AllowAny,))
@renderer_classes(tuple(api_settings.DEFAULT_RENDERER_CLASSES))
def reprocess_csv_reports(request):
    """trigger a task to reprocess monthly csv files into parquet files for a provider."""
    params = request.query_params
    provider_uuid = params.get("provider_uuid")
    start_date = params.get("start_date")
    summarize_reports = params.get("summarize_reports", "true").lower()
    summarize_reports = True if summarize_reports == "true" else False
    queue_name = params.get("queue") or GET_REPORT_FILES_QUEUE
    if provider_uuid is None:
        errmsg = "provider_uuid must be supplied as a parameter."
        return Response({"Error": errmsg}, status=status.HTTP_400_BAD_REQUEST)
    if start_date is None:
        errmsg = "start_date must be supplied as a parameter."
        return Response({"Error": errmsg}, status=status.HTTP_400_BAD_REQUEST)
    if queue_name not in QUEUE_LIST:
        errmsg = f"'queue' must be one of {QUEUE_LIST}."
        return Response({"Error": errmsg}, status=status.HTTP_400_BAD_REQUEST)

    provider = Provider.objects.filter(uuid=provider_uuid).first()
    schema = provider.account.get("schema_name")
    report_month = DateHelper().month_start(start_date)

    orchestrator = Orchestrator()

    async_results = orchestrator.start_manifest_processing(
        customer_name=schema,
        credentials={},
        data_source={},
        provider_type=provider.type,
        schema_name=schema,
        provider_uuid=provider_uuid,
        report_month=report_month,
        summarize_reports=False,
        reprocess_csv_reports=True,
    )
    return Response({"Download Request Task ID": str(async_results)})
