#!/usr/bin/env python3

"""
Client for the Pegasus Agent service.
"""

import logging

import requests

from Pegasus.tools import properties, utils

logger = logging.getLogger("pegasus-agent")

API_MAX_LENGTH = 30000


class AgentClient:
    """
    Client for the Pegasus Agent service.
    """

    def __init__(self):
        """Initialize the AgentClient, loading endpoint and token from Pegasus properties."""
        self.props = properties.Properties()
        self.props.new()  # load the default properties

        # endpoint
        if self.props.property("pegasus.agent.url"):
            self.url = self.props.property("pegasus.agent.url")
        else:
            self.url = "https://agent.k.scitech.group"

        # token
        if self.props.property("pegasus.agent.token"):
            self.token = self.props.property("pegasus.agent.token")
        else:
            self.token = "default"

        self.client_version = utils.pegasus_version()

    def analyze(self, workflow_id, analyze_stdout):
        """
        Analyze a workflow using the Pegasus Agent service.

        :param workflow_id: unique identifier of the workflow to analyze
        :type workflow_id: str
        :param analyze_stdout: captured output from pegasus-analyzer (truncated to API_MAX_LENGTH)
        :type analyze_stdout: str
        :return: AI-generated analysis message from the agent service
        :rtype: str
        :raises RuntimeError: if the HTTP request fails
        """

        full_url = f"{self.url}/wf/analyze/ai/{workflow_id}"
        headers = {"X-API-Key": self.token, "Content-Type": "application/json"}
        data = {
            "client_version": self.client_version,
            "analyze_stdout": analyze_stdout[:API_MAX_LENGTH],
        }

        logger.debug(
            f"Posting to {full_url} with token '{self.token}' and data: {data}"
        )

        cout = ""
        try:
            response = requests.post(full_url, json=data, headers=headers, timeout=300)
            response.raise_for_status()
            cout += response.json()["message"]
        except requests.RequestException as e:
            raise RuntimeError(f"{e}")

        return cout

    def statistics(self, workflow_id, stats_stdout):
        """
        Summarize workflow statistics using the Pegasus Agent service.

        :param workflow_id: unique identifier of the workflow
        :type workflow_id: str
        :param stats_stdout: captured output from pegasus-statistics (truncated to API_MAX_LENGTH)
        :type stats_stdout: str
        :return: AI-generated statistics summary from the agent service
        :rtype: str
        :raises RuntimeError: if the HTTP request fails
        """

        full_url = f"{self.url}/wf/statistics/ai/{workflow_id}"
        headers = {"X-API-Key": self.token, "Content-Type": "application/json"}
        data = {
            "client_version": self.client_version,
            "statistics_stdout": stats_stdout[:API_MAX_LENGTH],
        }

        logger.debug(
            f"Posting to {full_url} with token '{self.token}' and data: {data}"
        )

        cout = ""
        try:
            response = requests.post(full_url, json=data, headers=headers, timeout=300)
            response.raise_for_status()
            cout += response.json()["message"]
        except requests.RequestException as e:
            raise RuntimeError(f"{e}")

        return cout
