#!/usr/bin/env python3

"""
Client for the the Pegasus Agent service.
"""

import logging

import requests

from Pegasus.tools import properties, utils

logger = logging.getLogger("pegasus-agent")


class AgentClient:
    """
    Client for the Pegasus Agent service.
    """

    def __init__(self):
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
        """

        full_url = f"{self.url}/wf/analyze/ai/{workflow_id}"
        headers = {"X-API-Key": self.token, "Content-Type": "application/json"}
        data = {
            "client_version": self.client_version,
            "analyze_stdout": analyze_stdout[:4999],
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
        Summarize the stats using the Pegasus Agent service.
        """

        full_url = f"{self.url}/wf/statistics/ai/{workflow_id}"
        headers = {"X-API-Key": self.token, "Content-Type": "application/json"}
        data = {
            "client_version": self.client_version,
            "statistics_stdout": stats_stdout[:9999],
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
