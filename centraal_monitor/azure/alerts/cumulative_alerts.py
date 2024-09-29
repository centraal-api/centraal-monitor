""" Monitor cumulative alerts for Azure resources. """

from azure.identity import DefaultAzureCredential
from azure.monitor.query import LogsQueryClient
from datetime import timedelta
import logging


class CumulativeAlerts:
    def __init__(self, subscription_id, resource_group, workspace_id):
        self.subscription_id = subscription_id
        self.resource_group = resource_group
        self.workspace_id = workspace_id
        self.credentials = DefaultAzureCredential()
        self.logs_client = LogsQueryClient(self.credentials)

    def create_cumulative_exception_alert(
        self, threshold, operation_name=None, cloud_role_name=None
    ):
        query = """
        exceptions
        """
        filters = []
        if operation_name:
            filters.append(f"operation_Name == '{operation_name}'")
        if cloud_role_name:
            filters.append(f"cloud_RoleName == '{cloud_role_name}'")
        if filters:
            where_clause = " | where " + " and ".join(filters)
            query += where_clause
        query += """
        | summarize exception_count = count()
        """
        response = self.logs_client.query_workspace(
            workspace_id=self.workspace_id, query=query, timespan=timedelta(minutes=5)
        )
        total_count = 0
        if response.tables and response.tables[0].rows:
            total_count = response.tables[0].rows[0]["exception_count"]
        if total_count >= threshold:
            # Trigger an alert
            alert_message = f"""
            Cumulative Exception Alert:
            Total Exceptions: {total_count}
            Threshold: {threshold}
            Operation: {operation_name}
            Role: {cloud_role_name}
            """
            logging.error(alert_message)
            # Integrate with notification services as needed
