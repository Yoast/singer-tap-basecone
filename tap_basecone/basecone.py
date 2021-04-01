"""Basecone API Client."""
# -*- coding: utf-8 -*-

import logging
from datetime import date, datetime, timedelta
from types import MappingProxyType
from typing import Callable, Generator, Optional
from tap_basecone.cleaners import CLEANERS
from dateutil.rrule import DAILY, rrule
import httpx
import singer

API_SCHEME: str = 'https://'
API_BASE_URL: str = 'api.basecone.com'
API_VERSION: str = '/v1'
API_REPORT_PATH: str = 'transactions'
API_COMPANY_ID: str = '?companyId=:id:'
API_REPORT_DATE: str = '&transactionDate=:date:'
HEADERS: MappingProxyType = MappingProxyType({
    'Authorization': 'Basic :accesstoken:',
})


class Basecone(object):  # noqa: WPS230
    """Basecone API Client."""

    def __init__(  # noqa: WPS211
        self,
        company_id: str,
        auth_token: str,
    ) -> None:
        """Initialize Basecone client.

        Arguments:
            company_id {str} -- Basecone company account
            auth_token {str} -- Base64 encoded Token
        """
        self.company_id: str = company_id
        self.auth_token: str = auth_token
        self.token: Optional[str] = None

        # Setup reusable web client
        self.client: httpx.Client = httpx.Client(http2=True)

        # Setup logger
        self.logger: logging.RootLogger = singer.get_logger()

        # Perform authentication during initialising
        self.create_header()

    def transaction_collection(  # noqa: WPS210
        self,
        **kwargs: dict,
    ) -> Generator[str, None, None]:
        """Basecone transactions.

        Arguments:
            start_date {str} -- String which contains the date

        Yields:
            Generator[dict] -- Yields Basecone transactions
        """
        self.logger.info('Stream Basecone transactions')

        cleaner: Callable = CLEANERS.get('transaction_collection', {})

        # Validate the start_date value exists
        start_date_input: str = str(kwargs.get('start_date', ''))

        if not start_date_input:
            raise ValueError('The parameter start_date is required.')

        parsed_date: datetime = datetime.strptime(start_date_input, '%Y-%m-%d')

        # Replace placeholder in reports path
        company: str = API_COMPANY_ID.replace(
            ':id:',
            self.company_id,
        )
        client: httpx.Client = httpx.Client(http2=True)
        
        for date_day in self._start_days_till_now(start_date_input):
            
            report_date: str = API_REPORT_DATE.replace(
                ':date:',
                str(date_day),
            )

            # Create the URL
            url: str = (
                f'{API_SCHEME}{API_BASE_URL}'
                f'{API_VERSION}/{API_REPORT_PATH}'
                f'{company}{report_date}'
            )

            self.logger.info(
                f'Recieving Basecone transactions from {date_day}'
            )

            response: httpx._models.Response = client.get(  # noqa: WPS437
                url,
                headers=self.headers,
            )

            jsondata = response.json()

            if response.status_code == 200:

                yield from (
                    cleaner(transaction)
                    for transaction in jsondata['transactions']
                )

            elif response.status_code == 404:  # noqa: WPS432
                self.logger.info(
                    f'Transactions with date: {date} not '
                    'found, stopping.',
                )
                break

    def create_header(self) -> None:
        """Generate a basic access token header."""

        headers: dict = dict(HEADERS)
        headers['Authorization'] = headers['Authorization'].replace(
            ':accesstoken:',
            self.auth_token,
        )
        self.headers = headers

    def _start_days_till_now(self, start_date: str) -> Generator:
        """Yield YYYY/MM/DD for every day until now.

        Arguments:
            start_date {str} -- Start date e.g. 2020-01-01

        Yields:
            Generator -- Every day until now.
        """
        # Parse input date
        year: int = int(start_date.split('-')[0])
        month: int = int(start_date.split('-')[1].lstrip())
        day: int = int(start_date.split('-')[2].lstrip())

        # Setup start period
        period: date = date(year, month, day)

        # Setup itterator
        dates: rrule = rrule(
            freq=DAILY,
            dtstart=period,
            until=datetime.utcnow(),
        )

        # Yield dates in YYYY-MM-DD format
        yield from (date_day.strftime('%Y-%m-%d') for date_day in dates)