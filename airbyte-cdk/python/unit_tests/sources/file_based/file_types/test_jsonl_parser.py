#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import io
import json
from typing import Any, Dict, Mapping

import pandas as pd
import pyarrow as pa
import pytest
from airbyte_cdk.sources.file_based.exceptions import RecordParseError
from airbyte_cdk.sources.file_based.file_types import JsonlParser


@pytest.mark.parametrize(
    "data, expected_schema",
    [
        pytest.param(
            {
                'col1': [1, 2],
                "col2": [1.1, 2.2],
                "col3": ["val1", "val2"],
                "col4": [True, False],
                "col5": [[1, 2], [3, 4]],
                "col6": [{"col6a": 1}, {"col6b": 2}],
                "col7": [None, None]
            }, {
                "col1": {"type": "integer"},
                "col2": {"type": "number"},
                "col3": {"type": "string"},
                "col4": {"type": "boolean"},
                "col5": {"type": "array"},
                "col6": {"type": "object"},
                "col7": {"type": "null"},
            }, id="various_column_types"
        ),
        pytest.param(
            {
                'col1': [1, None],
                "col2": [1.1, None],
                "col3": ["val1", None],
                "col4": [True, None],
                "col5": [[1, 2], None],
                "col6": [{"col6a": 1}, None],
            }, {
                "col1": {"type": "number"},
                "col2": {"type": "number"},
                "col3": {"type": "string"},
                "col4": {"type": "boolean"},
                "col5": {"type": "array"},
                "col6": {"type": "object"},
            }, id="null_values_in_columns"
        ),
        pytest.param({}, {}, id="no_records"),
    ]
)
def test_type_mapping(record: Dict[str, Any], expected_schema: Mapping[str, str]) -> None:
    if expected_schema is None:
        with pytest.raises(ValueError):
            JsonlParser().infer_schema_for_record(record)
    else:
        assert JsonlParser.infer_schema_for_record(record) == expected_schema
