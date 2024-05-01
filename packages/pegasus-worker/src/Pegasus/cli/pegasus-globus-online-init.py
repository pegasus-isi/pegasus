#!/usr/bin/env python3
from __future__ import print_function

import os
from argparse import ArgumentParser

import globus_sdk
from globus_sdk.scopes import (
    GCSCollectionScopeBuilder,
    GCSEndpointScopeBuilder,
    TransferScopes,
)

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
    help="list of endpoint uuids to acquire manage_collections consent",
)
parser.add_argument(
    "-c",
    "--collections",
    nargs="*",
    default=[],
    help="list of collection uuids to acquire data_access consent",
)
parser.add_argument(
    "-d",
    "--domains",
    nargs="*",
    default=[],
    help="list of domain requirements which must be satisfied by the identities under the globus user account",
)


args = parser.parse_args()

config = ConfigParser()
config.add_section("oauth")
config.set("oauth", "client_id", client_id)

pegasus_scopes = [TransferScopes.all]
for c in args.collections:
    sb = GCSCollectionScopeBuilder(c, known_scopes=[TransferScopes.all])
    pegasus_scopes.append("{0}[*{1}]".format(TransferScopes.all, sb.data_access))
    pegasus_scopes.append(sb.data_access)

for e in args.endpoints:
    sb = GCSEndpointScopeBuilder(e)
    pegasus_scopes.append(sb.manage_collections)

required_domains = None
for d in args.domains:
    if not required_domains:
        required_domains = [d]
    else:
        required_domains.append(d)

client = globus_sdk.NativeAppAuthClient(client_id)
if args.permanent:
    client.oauth2_start_flow(refresh_tokens=True, requested_scopes=pegasus_scopes)
else:
    client.oauth2_start_flow(requested_scopes=pegasus_scopes)

authorize_url = client.oauth2_get_authorize_url(
    session_required_single_domain=required_domains, prompt="login"
)
print("Please go to this URL and login: {}".format(authorize_url))

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
