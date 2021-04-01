"""Basecone cleaners."""
# -*- coding: utf-8 -*-

from types import MappingProxyType
from tap_basecone.streams import STREAMS
from typing import Any, Optional


class ConvertionError(ValueError):
    """Failed to convert value."""


def to_type_or_null(
    input_value: Any,
    data_type: Optional[Any] = None,
    nullable: bool = True,
) -> Optional[Any]:
    """Convert the input_value to the data_type.

    The input_value can be anything. This function attempts to convert the
    input_value to the data_type. The data_type can be a data type such as str,
    int or Decimal or it can be a function. If nullable is True, the value is
    converted to None in cases where the input_value == None. For example:
    a '' == None, {} == None and [] == None.

    Arguments:
        input_value {Any} -- Input value

    Keyword Arguments:
        data_type {Optional[Any]} -- Data type to convert to (default: {None})
        nullable {bool} -- Whether to convert empty to None (default: {True})

    Returns:
        Optional[Any] -- The converted value
    """
    # If the input_value is not equal to None and a data_type input exists
    if input_value and data_type:
        # Convert the input value to the data_type
        try:
            return data_type(input_value)
        except ValueError as err:
            raise ConvertionError(
                f'Could not convert {input_value} to {data_type}: {err}',
            )

    # If the input_value is equal to None and Nullable is True
    elif not input_value and nullable:
        # Convert '', {}, [] to None
        return None

    # If the input_value is equal to None, but nullable is False
    # Return the original value
    return input_value


def clean_row(row: dict, mapping: dict) -> dict:
    """Clean the row according to the mapping.

    The mapping is a dictionary with optional keys:
    - map: The name of the new key/column
    - type: A data type or function to apply to the value of the key
    - nullable: Whether to convert empty values, such as '', {} or [] to None

    Arguments:
        row {dict} -- Input row
        mapping {dict} -- Input mapping

    Returns:
        dict -- Cleaned row
    """
    cleaned: dict = {}

    key: str
    key_mapping: dict

    # For every key and value in the mapping
    for key, key_mapping in mapping.items():

        # Retrieve the new mapping or use the original
        new_mapping: str = key_mapping.get('map') or key

        # Convert the value
        cleaned[new_mapping] = to_type_or_null(
            row[key],
            key_mapping.get('type'),
            key_mapping.get('null', True),
        )

    return cleaned


def clean_transaction_collection(
    input_data: dict,
) -> dict:
    """Clean transaction collection input_data.

    Arguments:
        input_data {dict} -- input input_data

    Returns:
        dict -- cleaned input_data
    """
    # Get the mapping from the STREAMS
    mapping: Optional[dict] = STREAMS['transaction_collection'].get(
        'mapping',
    )

    cleaned_data = {
        'type': input_data.get('type'),
        'description': input_data.get('description'),
        'dueDate': input_data.get('dueDate'),
        'invoiceNumber': input_data.get('invoiceNumber'),
        'purchaseOrderNumber': input_data.get('purchaseOrderNumber'),
        'supplier_id': input_data.get('supplier', {}).get('supplierId'),
        'supplier_code': input_data.get('supplier', {}).get('code'),
        'supplier_name': input_data.get('supplier', {}).get('name'),
        'paymentCondition': input_data.get('paymentCondition'),
        'isInPaymentBatch': str(input_data.get('isInPaymentBatch')),
        'isCreditNote': str(input_data.get('isCreditNote')),
        'totalAmount': input_data.get('totalAmount'),
        'transactionID': input_data.get('transactionId'),
        'documentID': input_data.get('documentId'),
        'targetCompany': input_data.get('targetCompany', {}).get('code'),
        'destinationCompany': input_data.get('destinationCompany', {}).get('code'),
        'transactionNumber': input_data.get('transactionNumber'),
        'transactionDate': input_data.get('transactionDate'),
        'generalledger_id': input_data.get('generalLedger', {}).get('generalLedgerId'),
        'generalledger_code': input_data.get('generalLedger', {}).get('code'),
        'period': input_data.get('period'),
        'currency_id': input_data.get('currency', {}).get('currencyId'),
        'currency_code': input_data.get('currency', {}).get('code'),
        'additionalField1': input_data.get('additionalField1'),
        'additionalField2': input_data.get('additionalField2'),
        'additionalField3': input_data.get('additionalField3'),
        'isFinalBooking': str(input_data.get('isFinalBooking')),
        'bookYear': input_data.get('bookYear'),
    }
    return clean_row(cleaned_data, mapping)


# Collect all cleaners
CLEANERS: MappingProxyType = MappingProxyType({
    'transaction_collection': clean_transaction_collection,
})
