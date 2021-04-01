"""Tools."""
# -*- coding: utf-8 -*-
from datetime import date, datetime, timedelta
from functools import reduce
from typing import Optional


def clear_currently_syncing(state: dict) -> dict:
    """Clear the currently syncing from the state.

    Arguments:
        state (dict) -- State file

    Returns:
        dict -- New state file
    """
    return state.pop('currently_syncing', None)


def get_stream_state(state: dict, tap_stream_id: str) -> dict:
    """Return the state of the stream.

    Arguments:
        state {dict} -- The state
        tap_stream_id {str} -- The id of the stream

    Returns:
        dict -- The state of the stream
    """
    return state.get(
        'bookmarks',
        {},
    ).get(tap_stream_id)


def create_bookmark(stream_name: str, bookmark_value: str) -> str:
    """Create bookmark.

    Arguments:
        stream_name {str} -- Name of stream
        bookmark_value {str} -- Bookmark value

    Returns:
        str -- Created bookmark
    """
    if stream_name in {
        'transaction_collection',
    }:
        # Return tomorrow's date
        tomorrow: date = datetime.strptime(
            bookmark_value,
            '%Y-%m-%d',
        ).date() + timedelta(days=1)
        return tomorrow.isoformat()

def get_bookmark_value(
    stream_name: str,
    row: dict,
) -> Optional[str]:
    """Retrieve bookmark value from record.

    Arguments:
        stream_name {str} -- Stream name
        row {dict} -- Record

    Returns:
        str -- Bookmark value
    """
    if stream_name in {
        'transaction_collection',
    }:
        # YYYY-MM
        return row['transaction_date'].replace("T00:00:00", "")