"""
Notification system for database changes
"""

import requests
import datetime
from utils.logger import logger


class Notifier:
    """Sends notifications about database changes"""

    def __init__(self, webhook_url, env_type="DEV"):
        """
        Initialize the notifier

        Args:
            webhook_url (str): Power Automate webhook URL for sending notifications
            env_type (str): Environment type (DEV, UAT, PROD)
        """
        self.webhook_url = webhook_url
        self.env_type = env_type
        self.today = datetime.datetime.now().strftime("%Y-%m-%d")

    def send_daily_summary(self, changes, azure_devops_link=None):
        """
        Send a daily summary of database changes

        Args:
            changes (list): List of change objects to include in summary
            azure_devops_link (str, optional): Link to Azure DevOps repository

        Returns:
            bool: True if the notification was sent successfully, False otherwise
        """
        # If no specific link provided, use generic Azure DevOps link
        if not azure_devops_link:
            azure_devops_link = "https://dev.azure.com/niagarabottling/DW%20Projects/_git"

        # Create HTML table
        html = self._create_html_report(changes, azure_devops_link)

        # Create payload for Power Automate
        payload = {
            "date": self.today,
            "html": html,
            "env": self.env_type,
            "subject": f"WMOS {self.env_type} Database Changes - Daily Summary ({self.today}) - {len(changes)} changes detected"
        }

        # Send to Power Automate
        try:
            response = requests.post(
                self.webhook_url,
                headers={"Content-Type": "application/json"},
                json=payload,
                timeout=30  # Set a timeout
            )
            response.raise_for_status()
            logger.info(f"Daily summary with {len(changes)} changes sent successfully")
            return True
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to send daily summary: {str(e)}")
            return False

    def _create_html_report(self, changes, azure_devops_link):
        """
        Create HTML report for email notification

        Args:
            changes (list): List of change objects
            azure_devops_link (str): Link to Azure DevOps repository

        Returns:
            str: HTML content for email
        """
        html = f"""
        <html>
        <head>
            <style>
                body {{
                    font-family: Arial, sans-serif;
                    font-size: 12px;
                }}
                table {{
                    border-collapse: collapse;
                    width: 100%;
                    margin-bottom: 20px;
                }}
                th, td {{
                    border: 1px solid #ddd;
                    padding: 8px;
                    text-align: left;
                }}
                th {{
                    background-color: #0078D4;
                    color: white;
                }}
                tr:nth-child(even) {{
                    background-color: #f2f2f2;
                }}
                h1, h2, h3 {{
                    color: #0078D4;
                }}
                .schema-header {{
                    background-color: #E6F2FA;
                    font-weight: bold;
                    font-size: 14px;
                }}
                .summary {{
                    margin-bottom: 20px;
                }}
                .code-changes {{
                    font-family: Consolas, monospace;
                    white-space: pre-wrap;
                    background-color: #f5f5f5;
                    padding: 5px;
                    border: 1px solid #ddd;
                    max-height: 200px;
                    overflow-y: auto;
                }}
            </style>
        </head>
        <body>
            <h1>WMOS {self.env_type} Database Changes - Daily Summary</h1>
            <div class="summary">
                <p><strong>Date:</strong> {self.today}</p>
                <p><strong>Total Changes:</strong> {len(changes)}</p>
                <a href="{azure_devops_link}" class="view-all-link">View All Changes in Azure DevOps</a>
            </div>
        """

        # Group changes by schema for better organization
        schema_changes = {}
        for change in changes:
            schema = change.get('schema', '')
            if schema not in schema_changes:
                schema_changes[schema] = []
            schema_changes[schema].append(change)

        # Add each schema section with its own table
        for schema, schema_changes_list in schema_changes.items():
            html += f"""
            <h2>Schema: {schema} ({len(schema_changes_list)} changes)</h2>
            <table>
                <tr>
                    <th>Object Name</th>
                    <th>Type</th>
                    <th>Lines Changed</th>
                    <th>Date</th>
                    <th>Changes</th>
                </tr>
            """

            # Add a row for each changed object
            for change in schema_changes_list:
                # Replace newlines with HTML breaks for proper display
                formatted_changes = change.get('changed_content', '').replace('\n', '<br>').replace('+', '<span style="color:green">+</span>')

                html += f"""
                <tr>
                    <td>{change.get('object_name', '')}</td>
                    <td>{change.get('object_type', '')}</td>
                    <td>{change.get('changed_lines', 0)}</td>
                    <td>{change.get('change_date', '')}</td>
                    <td><div class="code-changes">{formatted_changes}</div></td>
                </tr>
                """

            html += "</table>"

        html += """
        <p>This report was automatically generated by the WMOS Database Tracker.</p>
        </body>
        </html>
        """

        return html