#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#


from unittest import TestCase
import freezegun
from airbyte_cdk.test.mock_http import HttpMocker

from ..config import NOW, START_DATE
from ..request_builder import get_stream_request
from ..response_builder import (
    NEXT_PAGE_TOKEN,
    successfull_incomplete_response, 
    get_stream_response, 
    get_stream_record,
)
from ..utils import (
    config, 
    read_full_refresh, 
    read_incremental, 
    get_cursor_value_from_state_message,
)

_STREAM_NAME = "onetimes"
_CURSOR_FIELD = "updated_at"


@freezegun.freeze_time(NOW.isoformat())
class TestFullRefresh(TestCase):
    @HttpMocker()
    def test_given_one_page_when_read_then_return_records(self, http_mocker: HttpMocker) -> None:
        http_mocker.get(
            get_stream_request(_STREAM_NAME).with_updated_at_min(START_DATE).build(),
            get_stream_response(_STREAM_NAME).with_record(get_stream_record(_STREAM_NAME, "id", _CURSOR_FIELD)).build(),
        )
        output = read_full_refresh(config(), _STREAM_NAME)
        assert len(output.records) == 1

    @HttpMocker()
    def test_given_multiple_pages_when_read_then_return_records(self, http_mocker: HttpMocker) -> None:
        
        http_mocker.get(
            get_stream_request(_STREAM_NAME).with_next_page_token(NEXT_PAGE_TOKEN).build(),
            get_stream_response(_STREAM_NAME).with_record(get_stream_record(_STREAM_NAME, "id", _CURSOR_FIELD)).build(),
        )
        http_mocker.get(
            get_stream_request(_STREAM_NAME).with_updated_at_min(START_DATE).build(),
            get_stream_response(_STREAM_NAME).with_pagination().with_record(get_stream_record(_STREAM_NAME, "id", _CURSOR_FIELD)).build(),
        )

        output = read_full_refresh(config(), _STREAM_NAME)
        assert len(output.records) == 2

    @HttpMocker()
    def test_retry_incomplete_response_with_success_status(self, http_mocker: HttpMocker) -> None:
        http_mocker.get(
            get_stream_request(_STREAM_NAME).with_updated_at_min(START_DATE).build(),
            [
                successfull_incomplete_response(_STREAM_NAME),
                get_stream_response(_STREAM_NAME).with_record(get_stream_record(_STREAM_NAME, "id", _CURSOR_FIELD)).build(),
            ]
        )
        read_full_refresh(config(), _STREAM_NAME, expected_exception=True)


@freezegun.freeze_time(NOW.isoformat())
class TestIncremental(TestCase):
    @HttpMocker()
    def test_state_message_produced_while_read_and_state_match_latest_record(self, http_mocker: HttpMocker) -> None:
        min_cursor_value = "2024-01-01T00:00:00+00:00"
        max_cursor_value = "2024-02-01T00:00:00+00:00"

        http_mocker.get(
            get_stream_request(_STREAM_NAME).with_updated_at_min(START_DATE).build(),
            get_stream_response(_STREAM_NAME).with_record(get_stream_record(_STREAM_NAME, "id", _CURSOR_FIELD).with_cursor(min_cursor_value)).with_record(
                get_stream_record(_STREAM_NAME, "id", _CURSOR_FIELD).with_cursor(max_cursor_value)).build(),
        )

        output = read_incremental(config(), _STREAM_NAME)
        test_cursor_value = get_cursor_value_from_state_message(output, _STREAM_NAME, _CURSOR_FIELD)
        assert test_cursor_value == max_cursor_value

    @HttpMocker()
    def test_retry_incomplete_response_with_success_status(self, http_mocker: HttpMocker) -> None:
        min_cursor_value = "2024-01-01T00:00:00+00:00"
        max_cursor_value = "2024-02-01T00:00:00+00:00"
        
        http_mocker.get(
            get_stream_request(_STREAM_NAME).with_updated_at_min(START_DATE).build(),
            [
                successfull_incomplete_response(_STREAM_NAME),
                get_stream_response(_STREAM_NAME).with_record(get_stream_record(_STREAM_NAME, "id", _CURSOR_FIELD).with_cursor(min_cursor_value)).with_record(
                get_stream_record(_STREAM_NAME, "id", _CURSOR_FIELD).with_cursor(max_cursor_value)).build(),
            ]
        )
        read_incremental(config(), _STREAM_NAME, expected_exception=True)