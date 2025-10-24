# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import base64
import datetime
import json
import time

import google.auth
import urllib3
from google.auth.transport.urllib3 import Request


def encode(source):
    """Safe base64 encoding."""
    return base64.urlsafe_b64encode(source.encode("utf-8")).decode("utf-8").rstrip("=")


class TokenProvider(object):
    """
    Provides OAuth tokens from Google Cloud Application Default credentials.
    """

    HEADER = json.dumps({"typ": "JWT", "alg": "GOOG_OAUTH2_TOKEN"})

    def __init__(self, **config):
        self.credentials, _project = google.auth.default()
        self.http_client = urllib3.PoolManager()

    def get_credentials(self):
        if not self.credentials.valid:
            self.credentials.refresh(Request(self.http_client))
        return self.credentials

    def get_jwt(self, creds):
        token_data = dict(
            exp=creds.expiry.timestamp(),
            iat=datetime.datetime.now(datetime.timezone.utc).timestamp(),
            iss="Google",
            scope="kafka",
            sub=creds.service_account_email,
        )
        return json.dumps(token_data)

    def get_token(self, args):
        creds = self.get_credentials()
        token = ".".join(
            [encode(self.HEADER), encode(self.get_jwt(creds)), encode(creds.token)]
        )

        # compute expiry time explicitly
        utc_expiry = creds.expiry.replace(tzinfo=datetime.timezone.utc)
        expiry_seconds = (
            utc_expiry - datetime.datetime.now(datetime.timezone.utc)
        ).total_seconds()

        return token, time.time() + expiry_seconds
