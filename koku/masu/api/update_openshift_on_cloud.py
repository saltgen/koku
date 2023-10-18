#
# Copyright 2021 Red Hat Inc.
# SPDX-License-Identifier: Apache-2.0
#
"""View for report_data endpoint."""
# flake8: noqa
import logging
from uuid import uuid4

from celery import chain
from celery import group
from django.conf import settings
from django.views.decorators.cache import never_cache
from rest_framework import status
from rest_framework.decorators import api_view
from rest_framework.decorators import permission_classes
from rest_framework.decorators import renderer_classes
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework.settings import api_settings

from api.provider.models import Provider
from api.utils import get_months_in_date_range
from masu.processor import is_customer_large
from masu.processor.tasks import PRIORITY_QUEUE
from masu.processor.tasks import PRIORITY_QUEUE_XL
from masu.processor.tasks import QUEUE_LIST
from masu.processor.tasks import update_openshift_on_cloud as update_openshift_on_cloud_task
from masu.util.common import date_range_pair

LOG = logging.getLogger(__name__)
REPORT_DATA_KEY = "Report Data Task IDs"


@never_cache
@api_view(http_method_names=["GET"])
@permission_classes((AllowAny,))
@renderer_classes(tuple(api_settings.DEFAULT_RENDERER_CLASSES))
def update_openshift_on_cloud(request):
    """Update OCP on Cloud report summary tables in the database."""
    if request.method == "GET":
        async_result = None
        async_results = []
        params = request.query_params
        openshift_provider_uuid = params.get("provider_uuid")
        schema_name = params.get("schema")
        start_date = params.get("start_date")
        end_date = params.get("end_date")
        fallback_queue = PRIORITY_QUEUE
        if is_customer_large(schema_name):
            fallback_queue = PRIORITY_QUEUE_XL
        queue_name = params.get("queue") or fallback_queue

        if openshift_provider_uuid is None:
            errmsg = "provider_uuid is a required parameter."
            return Response({"Error": errmsg}, status=status.HTTP_400_BAD_REQUEST)
        if queue_name not in QUEUE_LIST:
            errmsg = f"'queue' must be one of {QUEUE_LIST}."
            return Response({"Error": errmsg}, status=status.HTTP_400_BAD_REQUEST)
        if start_date is None:
            errmsg = "start_date is a required parameter."
            return Response({"Error": errmsg}, status=status.HTTP_400_BAD_REQUEST)
        if schema_name is None:
            errmsg = "schema is a required parameter."
            return Response({"Error": errmsg}, status=status.HTTP_400_BAD_REQUEST)

        try:
            provider = Provider.objects.get(uuid=openshift_provider_uuid)
        except Provider.DoesNotExist:
            errmsg = f"provider_uuid: {openshift_provider_uuid} does not exist."
            return Response({"Error": errmsg}, status=status.HTTP_400_BAD_REQUEST)

        if provider.type != Provider.PROVIDER_OCP:
            errmsg = "You must provider an OpenShift provider UUID."
            return Response({"Error": errmsg}, status=status.HTTP_400_BAD_REQUEST)

        infra_provider_uuid = provider.infrastructure.infrastructure_provider.uuid
        infra_type = provider.infrastructure.infrastructure_type

        months = get_months_in_date_range(start=start_date, end=end_date)
        for month in months:
            tracing_id = uuid4()
            for start, end in date_range_pair(month[0], month[1], step=settings.TRINO_DATE_STEP):
                summary_params = {
                    "schema_name": schema_name,
                    "openshift_provider_uuid": openshift_provider_uuid,
                    "infrastructure_provider_uuid": infra_provider_uuid,
                    "infrastructure_provider_type": infra_type,
                    "start_date": start,
                    "end_date": end,
                    "queue_name": queue_name,
                    "tracing_id": tracing_id,
                }
                LOG.info("Triggering update_openshift_on_cloud task with params:")
                LOG.info(params)
                summary_signature = [update_openshift_on_cloud_task.si(**summary_params)]

            summaries = group(summary_signature)
            c = chain(summaries)

            async_result = c.apply_async(queue=queue_name or fallback_queue)
            async_results.append({str(month): str(async_result)})

        return Response({REPORT_DATA_KEY: async_results})
