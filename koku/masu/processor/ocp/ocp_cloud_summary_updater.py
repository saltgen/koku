#
# Copyright 2018 Red Hat, Inc.
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as
#    published by the Free Software Foundation, either version 3 of the
#    License, or (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <https://www.gnu.org/licenses/>.
#
"""Updates report summary tables in the database."""
# pylint: skip-file
import datetime
import logging

from tenant_schemas.utils import schema_context

from api.provider.models import Provider
from masu.database.aws_report_db_accessor import AWSReportDBAccessor
from masu.database.cost_model_db_accessor import CostModelDBAccessor
from masu.database.ocp_report_db_accessor import OCPReportDBAccessor
from masu.processor.ocp.ocp_cloud_updater_base import OCPCloudUpdaterBase
from masu.util.aws.common import get_bills_from_provider
from masu.util.ocp.common import get_cluster_id_from_provider

LOG = logging.getLogger(__name__)


class OCPCloudReportSummaryUpdater(OCPCloudUpdaterBase):
    """Class to update OCP report summary data."""

    def update_summary_tables(self, start_date, end_date):
        """Populate the summary tables for reporting.

        Args:
            start_date (str) The date to start populating the table.
            end_date   (str) The date to end on.

        Returns
            None

        """
        infra_map = self.get_infra_map()
        openshift_provider_uuids, infra_provider_uuids = self.get_openshift_and_infra_providers_lists(infra_map)

        if (self._provider.type == Provider.PROVIDER_OCP
                and self._provider_uuid not in openshift_provider_uuids):
            infra_map = self._generate_ocp_infra_map_from_sql(start_date, end_date)
        elif (self._provider.type in Provider.CLOUD_PROVIDER_LIST
                and self._provider_uuid not in infra_provider_uuids):
            # When running for an Infrastructure provider we want all
            # of the matching clusters to run
            infra_map = self._generate_ocp_infra_map_from_sql(start_date, end_date)

        # If running as an infrastructure provider (e.g. AWS)
        # this loop should run for all associated OpenShift clusters.
        # If running for an OpenShift provider, it should just run one time.
        for ocp_provider_uuid, infra_tuple in infra_map.items():
            infra_provider_uuid = infra_tuple[0]
            infra_provider_type = infra_tuple[1]
            if infra_provider_type in (Provider.PROVIDER_AWS, Provider.PROVIDER_AWS_LOCAL):
                self.update_aws_summary_tables(
                    ocp_provider_uuid, infra_provider_uuid, start_date, end_date)

    def update_aws_summary_tables(self, openshift_provider_uuid, aws_provider_uuid, start_date, end_date):
        """Update operations specifically for OpenShift on AWS."""
        cluster_id = get_cluster_id_from_provider(openshift_provider_uuid)
        aws_bills = get_bills_from_provider(
            aws_provider_uuid,
            self._schema,
            datetime.datetime.strptime(start_date, '%Y-%m-%d'),
            datetime.datetime.strptime(end_date, '%Y-%m-%d')
        )
        aws_bill_ids = []
        with schema_context(self._schema):
            aws_bill_ids = [str(bill.id) for bill in aws_bills]

        with CostModelDBAccessor(self._schema, aws_provider_uuid,
                                 self._column_map) as cost_model_accessor:
            markup = cost_model_accessor.get_markup()
            markup_value = float(markup.get('value', 0)) / 100

        # OpenShift on AWS
        with AWSReportDBAccessor(self._schema, self._column_map) as accessor:
            LOG.info('Updating OpenShift on AWS summary table for '
                     '\n\tSchema: %s \n\tProvider: %s \n\tDates: %s - %s'
                     '\n\tCluster ID: %s, AWS Bill IDs: %s',
                     self._schema, self._provider.uuid,
                     start_date, end_date, cluster_id, str(aws_bill_ids))
            accessor.populate_ocp_on_aws_cost_daily_summary(
                start_date,
                end_date,
                cluster_id,
                aws_bill_ids
            )
            accessor.populate_ocp_on_aws_markup_cost(markup_value, aws_bill_ids)

        with OCPReportDBAccessor(self._schema, self._column_map) as accessor:
            # This call just sends the infrastructure cost to the
            # OCP usage daily summary table
            accessor.update_summary_infrastructure_cost(cluster_id, start_date, end_date)
