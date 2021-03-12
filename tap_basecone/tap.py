"""Basecone tap."""
# -*- coding: utf-8 -*-
import logging
from argparse import Namespace

import pkg_resources
from singer import get_logger, utils
from singer.catalog import Catalog

from tap_basecone.basecone import Basecone
from tap_basecone.discover import discover
from tap_basecone.sync import sync

VERSION: str = pkg_resources.get_distribution('tap-basecone').version
LOGGER: logging.RootLogger = get_logger()
REQUIRED_CONFIG_KEYS: tuple = (
    'username',
    'password',
    'company_id',
    'client_identifier',
    'client_secret',
    'start_date',
)


@utils.handle_top_exception(LOGGER)
def main() -> None:
    """Run tap."""
    # Parse command line arguments
    args: Namespace = utils.parse_args(REQUIRED_CONFIG_KEYS)

    LOGGER.info(f'>>> Running tap-basecone v{VERSION}')

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog: Catalog = discover()
        catalog.dump()
        return

    # Otherwise run in sync mode
    if args.catalog:
        # Load command line catalog
        catalog = args.catalog
    else:
        # Loadt the  catalog
        catalog = discover()

    # Initialize Basecone client
    basecone: Basecone = Basecone(
        args.config['username'],
        args.config['password'],
        args.config['company_id'],
        args.config['client_identifier'],
        args.config['client_secret'],
        args.config['start_date'],
    )

    sync(basecone, args.state, catalog, args.config['start_date'])


if __name__ == '__main__':
    main()
