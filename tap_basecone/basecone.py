"""Basecone API Client."""
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
API_REPORT_PATH: str = 'transactions'
API_COMPANY_ID: str = '?companyId=:id:'
API_REPORT_DATE: str = '&transactionDate=:date:'
API_PATH_AUTH: str = 'authentication/token'

HEADERS: MappingProxyType = MappingProxyType({
    'User-Agent': (
        'Singer Tap: GitHub.com/Yoast/singer-tap-basecone/ | By Yoast.com'
    ),
    'Content-Type': 'application/json',
    'Authorization': 'Bearer :accesstoken:',
})


class Basecone(object):  # noqa: WPS230
    """Basecone API Client."""

    def __init__(  # noqa: WPS211
        self,
        username: str,
        password: str,
        company_id: str,
        client_identifier: str,
        client_secret: str,
    ) -> None:
        """Initialize Basecone client.

        Arguments:
            username {str} -- Reporting user username
            password {str} --Reporting user password
            company_id {str} -- Adyen company account
            client_identifier {str} -- API Client Id
            client_secret {str} -- API Client Secret
        """
        self.username: str = username
        self.password: str = password
        self.company_id: str = company_id
        self.client_identifier: str = client_identifier
        self.client_secret: str = client_secret
        self.token: Optional[str] = None

        # Setup reusable web client
        self.client: httpx.Client = httpx.Client(http2=True)

        # Setup logger
        self.logger: logging.RootLogger = singer.get_logger()

        # Perform authentication during initialising
        self._authenticate()

    def transactions(  # noqa: WPS210
        self,
        start_date: str,
    ) -> Generator[str, None, None]:
        """Basecone transactions.

        Arguments:
            start_date {str} -- String which contains the date

        Yields:
            Generator[dict] -- Yields Basecone transactions
        """
        self.logger.info('Stream Basecone transactions')

        parsed_date: datetime = datetime.strptime(start_date, '%Y-%m-%d')

        # Replace placeholder in reports path
        company: str = API_COMPANY_ID.replace(
            ':id:',
            self.company_id,
        )

        client: httpx.Client = httpx.Client(http2=True)

        while True:

            date: str = parsed_date.strftime('%Y-%m-%d')
            report_date: str = API_REPORT_DATE.replace(
                ':date:',
                str(date),
            )

            # Create the URL
            url: str = (
                f'{API_SCHEME}{API_BASE_URL}'
                f'{API_VERSION}{API_REPORT_PATH}'
                f'{company}{report_date}'
            )

            response: httpx._models.Response = client.get(  # noqa: WPS437
                url,
                headers=self.headers,
            )

            jsondata = response.json()

            if response.status_code == 200:

                yield from (
                    transaction
                    for transaction in jsondata['transactions']
                )
                parsed_date = parsed_date + timedelta(days=1)

            elif response.status_code == 404:  # noqa: WPS432
                self.logger.debug(
                    f'Transactions with date: {date} not '
                    'found, stopping.',
                )
                break

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
        post_data: dict = {
            'Username': self.username,
            'Password': self.password,
            'OfficeCode': self.company_id,
            'ClientId': self.client_identifier,
            'ClientSecret': self.client_secret,
        }

        client: httpx.Client = httpx.Client(http2=True)

        response: httpx._models.Response = client.post(  # noqa: WPS437
            url,
            headers=headers,
            data=post_data,
        )

        # Raise error on 4xx and 5xxx
        response.raise_for_status()

        response_data: dict = response.json()

        # Save the token
        self.token = response_data.get('token')

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
