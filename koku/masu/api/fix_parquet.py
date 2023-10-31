#
# Copyright 2021 Red Hat Inc.
# SPDX-License-Identifier: Apache-2.0
#
"""View for expired_data endpoint."""
import logging

from django.views.decorators.cache import never_cache
from rest_framework.decorators import api_view
from rest_framework.decorators import permission_classes
from rest_framework.decorators import renderer_classes
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework.settings import api_settings

from masu.processor.orchestrator import Orchestrator

LOG = logging.getLogger(__name__)


@never_cache
@api_view(http_method_names=["GET", "DELETE"])
@permission_classes((AllowAny,))
@renderer_classes(tuple(api_settings.DEFAULT_RENDERER_CLASSES))
def fix_parquet(request):
    """Return expired data."""
    simulate = False
    orchestrator = Orchestrator()
    async_fix_results = orchestrator.fix_parquet_data_types(simulate=simulate)
    response_key = "Async jobs for fix parquet files"
    if simulate:
        response_key = response_key + " (simulated)"
    return Response({response_key: str(async_fix_results)})
