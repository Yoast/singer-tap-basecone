"""PayPal API Client."""
# -*- coding: utf-8 -*-

import logging
from datetime import datetime, timedelta
from types import MappingProxyType
from typing import Generator, Optional

import httpx
import singer

API_SCHEME: str = 'https://'
API_BASE_URL: str = 'api.basecone.com'
API_VERSION: str = '/v1'
API_PATH_TRANSACTIONS: str = 'transactions?companyId=:id:'
API_PATH_AUTH: str = 'authentication/token'

HEADERS: MappingProxyType = MappingProxyType({
    'User-Agent': (
        'Singer Tap: GitHub.com/Yoast/singer-tap-basecone/ | By Yoast.com'
    ),
})


class Basecone(object):  # noqa: WPS230
    """Basecone API Client."""

    def __init__(  # noqa: WPS211
        self,
        username: str,
        password: str,
        company_id: str,
        client_identifier: str,
        client_apikey: str,
    ) -> None:
        """Initialize Basecone client.

        Arguments:
            username {str} -- Reporting user username
            password {str} --Reporting user password
            company_id {str} -- Adyen company account
            client_identifier {str} -- API Client Id
            client_apikey {str} -- API Client Secret
        """
        self.username: str = username
        self.password: str = password
        self.company_id: str = company_id
        self.client_identifier: str = client_identifier
        self.client_apikey: str = client_apikey
        self.token: Optional[str] = None

        # Setup reusable web client
        self.client: httpx.Client = httpx.Client(http2=True)

        # Setup logger
        self.logger: logging.RootLogger = singer.get_logger()

    def transactions(  # noqa: WPS210
        self,
        start_date: str,
    ) -> Generator[str, None, None]:
        """Paypal transaction history.

        Raises:
            ValueError: When the parameter start_date is missing

        Yields:
            Generator[dict] -- Yields Basecone transactions
        """
        # TODO: Create function that gets the JSON data

    def _authenticate(self) -> None:  # noqa: WPS210
        """Generate a bearer access token."""
        url: str = (
            f'{API_SCHEME}{API_BASE_URL}/'
            f'{API_VERSION}/{API_PATH_AUTH}'
        )
        headers: dict = {
            'Accept': 'application/json',
            'Accept-Language': 'en_US',
        }
        post_data: dict = {'grant_type': 'client_credentials'}

        now: datetime = datetime.utcnow()

        client: httpx.Client = httpx.Client(http2=True)

        response: httpx._models.Response = client.post(  # noqa: WPS437
            url,
            headers=headers,
            data=post_data,
            auth=(self.client_identifier, self.client_apikey),
        )

        # Raise error on 4xx and 5xxx
        response.raise_for_status()

        response_data: dict = response.json()

        # Save the token
        self.token = response_data.get('access_token')

        # Set experation date for token
        expires_in: int = response_data.get('expires_in', 0)
        self.token_expires = now + timedelta(expires_in)

        # Set up headers to use in API requests
        self._create_headers()

        appid: Optional[str] = response_data.get('app_id')

        self.logger.info(
            'Authentication succesfull '
            f'(appid: {appid})',
        )

    def _create_headers(self) -> None:
        """Create authenticationn headers for requests."""
        headers: dict = dict(HEADERS)
        headers['Authorization'] = headers['Authorization'].replace(
            ':accesstoken:',
            self.token,
        )
        self.headers = headers
