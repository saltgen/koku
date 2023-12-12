#
# Copyright 2021 Red Hat Inc.
# SPDX-License-Identifier: Apache-2.0
#
"""API views for import organization"""
# flake8: noqa
from api.cloud_accounts.views import cloud_accounts
from api.currency.view import get_currency
from api.currency.view import get_exchange_rates
from api.dataexport.views import DataExportRequestViewSet
from api.deprecated_settings.view import SettingsView
from api.forecast.views import AWSCostForecastView
from api.forecast.views import AzureCostForecastView
from api.forecast.views import GCPCostForecastView
from api.forecast.views import OCICostForecastView
from api.forecast.views import OCPAllCostForecastView
from api.forecast.views import OCPAWSCostForecastView
from api.forecast.views import OCPAzureCostForecastView
from api.forecast.views import OCPCostForecastView
from api.forecast.views import OCPGCPCostForecastView
from api.ingress.reports.view import IngressReportsDetailView
from api.ingress.reports.view import IngressReportsView
from api.metrics.views import metrics
from api.openapi.view import openapi
from api.organizations.aws.view import AWSOrgView
from api.report.all.openshift.view import OCPAllCostView
from api.report.all.openshift.view import OCPAllInstanceTypeView
from api.report.all.openshift.view import OCPAllStorageView
from api.report.aws.openshift.view import OCPAWSCostView
from api.report.aws.openshift.view import OCPAWSInstanceTypeView
from api.report.aws.openshift.view import OCPAWSStorageView
from api.report.aws.view import AWSCostView
from api.report.aws.view import AWSInstanceTypeView
from api.report.aws.view import AWSStorageView
from api.report.azure.openshift.view import OCPAzureCostView
from api.report.azure.openshift.view import OCPAzureInstanceTypeView
from api.report.azure.openshift.view import OCPAzureStorageView
from api.report.azure.view import AzureCostView
from api.report.azure.view import AzureInstanceTypeView
from api.report.azure.view import AzureStorageView
from api.report.gcp.openshift.view import OCPGCPCostView
from api.report.gcp.openshift.view import OCPGCPInstanceTypeView
from api.report.gcp.openshift.view import OCPGCPStorageView
from api.report.gcp.view import GCPCostView
from api.report.gcp.view import GCPInstanceTypeView
from api.report.gcp.view import GCPStorageView
from api.report.oci.view import OCICostView
from api.report.oci.view import OCIInstanceTypeView
from api.report.oci.view import OCIStorageView
from api.report.ocp.view import OCPCostView
from api.report.ocp.view import OCPCpuView
from api.report.ocp.view import OCPMemoryView
from api.report.ocp.view import OCPVolumeView
from api.resource_types.aws_accounts.view import AWSAccountView
from api.resource_types.aws_category.view import AWSCategoryView
from api.resource_types.aws_org_unit.view import AWSOrganizationalUnitView
from api.resource_types.aws_regions.view import AWSAccountRegionView
from api.resource_types.aws_services.view import AWSServiceView
from api.resource_types.azure_regions.view import AzureRegionView
from api.resource_types.azure_services.view import AzureServiceView
from api.resource_types.azure_subscription_guid.view import AzureSubscriptionGuidView
from api.resource_types.cost_models.view import CostModelResourceTypesView
from api.resource_types.gcp_accounts.view import GCPAccountView
from api.resource_types.gcp_projects.view import GCPProjectsView
from api.resource_types.gcp_regions.view import GCPRegionView
from api.resource_types.gcp_services.view import GCPServiceView
from api.resource_types.oci_regions.view import OCIRegionView
from api.resource_types.oci_services.view import OCIServiceView
from api.resource_types.oci_tenant_id.view import OCITenantidView
from api.resource_types.openshift_clusters.view import OCPClustersView
from api.resource_types.openshift_nodes.view import OCPNodesView
from api.resource_types.openshift_projects.view import OCPProjectsView
from api.resource_types.view import ResourceTypeView
from api.settings.aws_category_keys.view import SettingsAWSCategoryKeyView
from api.settings.aws_category_keys.view import SettingsDisableAWSCategoryKeyView
from api.settings.aws_category_keys.view import SettingsEnableAWSCategoryKeyView
from api.settings.cost_groups.view import CostGroupsView
from api.settings.tags.view import SettingsDisableTagView
from api.settings.tags.view import SettingsEnableTagView
from api.settings.tags.view import SettingsTagView
from api.settings.views import AccountSettings
from api.settings.views import UserCostTypeSettings
from api.status.views import StatusView
from api.tags.all.openshift.view import OCPAllTagView
from api.tags.aws.openshift.view import OCPAWSTagView
from api.tags.aws.view import AWSTagView
from api.tags.azure.openshift.view import OCPAzureTagView
from api.tags.azure.view import AzureTagView
from api.tags.gcp.openshift.view import OCPGCPTagView
from api.tags.gcp.view import GCPTagView
from api.tags.oci.view import OCITagView
from api.tags.ocp.view import OCPTagView
from api.user_access.view import UserAccessView
