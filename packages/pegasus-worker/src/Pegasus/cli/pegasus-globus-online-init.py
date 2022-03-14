#!/usr/bin/env python3
from __future__ import print_function

import os
from argparse import ArgumentParser

import globus_sdk
from globus_sdk.scopes import GCSCollectionScopeBuilder, TransferScopes

try:
    from configparser import ConfigParser
except Exception:
    from ConfigParser import ConfigParser

client_id = "d7382f5a-4ea3-4b69-b094-99c392fc820d"
config_file = os.path.expanduser("~/.pegasus/globus.conf")

parser = ArgumentParser(
    description="Initialize Globus OAuth Tokens - SDK Version {}".format(
        globus_sdk.__version__
    )
)
parser.add_argument(
    "-p", "--permanent", action="store_true", help="request a refreshable token",
)
parser.add_argument(
    "-e",
    "--endpoints",
    nargs="*",
    default=[],
    help="list of endpoint uuids to acquire data_consents",
)

args = parser.parse_args()

config = ConfigParser()
config.add_section("oauth")
config.set("oauth", "client_id", client_id)

pegasus_scopes = [TransferScopes.all]
for c in args.endpoints:
    sb = GCSCollectionScopeBuilder(c, known_scopes=[TransferScopes.all])
    pegasus_scopes.append("{0}[*{1}]".format(TransferScopes.all, sb.data_access))
    pegasus_scopes.append(sb.data_access)

client = globus_sdk.NativeAppAuthClient(client_id)
if args.permanent:
    client.oauth2_start_flow(refresh_tokens=True, requested_scopes=pegasus_scopes)
else:
    client.oauth2_start_flow(requested_scopes=pegasus_scopes)

authorize_url = client.oauth2_get_authorize_url()
print("Please go to this URL and login: {0}".format(authorize_url))

get_input = getattr(__builtins__, "raw_input", input)
auth_code = get_input("Please enter the code you get after login here: ").strip()
token_response = client.oauth2_exchange_code_for_tokens(auth_code)

transfer_data = token_response.by_resource_server["transfer.api.globus.org"]

# get tokens as strings and their expiration timestamp
transfer_at = str(transfer_data["access_token"])
transfer_rt = str(transfer_data["refresh_token"])
transfer_at_exp = str(transfer_data["expires_at_seconds"])

config.set("oauth", "transfer_at", transfer_at)  # transfer access token
config.set("oauth", "transfer_rt", transfer_rt)  # transfer refresh token
config.set("oauth", "transfer_at_exp", transfer_at_exp)  # transfer expiration time

cfg = open(config_file, "w+")
config.write(cfg)
cfg.close()
