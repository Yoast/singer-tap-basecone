"""Streams metadata."""
# -*- coding: utf-8 -*-
from types import MappingProxyType

# Streams metadata
STREAMS: MappingProxyType = MappingProxyType({
    'transactions': {
        'key_properties': 'transactionId',
        'replication_method': 'INCREMENTAL',
        'replication_key': 'transactionId',
        'bookmark': 'start_date',
    },
})
