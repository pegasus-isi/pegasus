#!/usr/bin/env python3

"""
Client for the the Pegasus Agent service.
"""

import logging

import requests

from Pegasus.tools import utils

logger = logging.getLogger("pegasus-agent")
utils.configureLogging(level=logging.WARNING)


class AgentClient:
    """
    Client for the Pegasus Agent service.
    """

    def __init__(self, url=None):
        if url is None:
            url = "https://agent.k.scitech.group"
        self.url = url

    def analyze(self, workflow_id, analyze_stdout):
        """
        Analyze a workflow using the Pegasus Agent service.
        """

        full_url = f"{self.url}/wf/analyze/ai/{workflow_id}"
        headers = {"X-API-Key": "mysecretapikey", "Content-Type": "application/json"}
        data = {"client_version": "5.2.0dev", "analyze_stdout": analyze_stdout[:4999]}

        cout = ""
        try:
            response = requests.post(full_url, json=data, headers=headers, timeout=300)
            response.raise_for_status()
            cout += response.json()["message"]
        except requests.RequestException as e:
            raise RuntimeError(f"{e}")

        return cout
