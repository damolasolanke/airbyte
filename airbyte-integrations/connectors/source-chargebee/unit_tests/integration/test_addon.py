# Copyright (c) 2023 Airbyte, Inc., all rights reserved.

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional
from unittest import TestCase

import freezegun
from airbyte_cdk.test.catalog_builder import CatalogBuilder
from airbyte_cdk.test.entrypoint_wrapper import EntrypointOutput, read
from airbyte_cdk.test.mock_http import HttpMocker, HttpResponse
from airbyte_cdk.test.mock_http.response_builder import (
    FieldPath,
    NestedPath,
    HttpResponseBuilder,
    RecordBuilder,
    create_record_builder,
    create_response_builder,
    find_template,
)
from airbyte_cdk.test.state_builder import StateBuilder
from airbyte_protocol.models import ConfiguredAirbyteCatalog, FailureType, SyncMode
from .config import ConfigBuilder
from .pagination import ChargebeePaginationStrategy
from .request_builder import ChargebeeRequestBuilder
from .response_builder import a_response_with_status
from source_chargebee import SourceChargebee

_STREAM_NAME = "addon"
_SITE = "test-site"
_SITE_API_KEY = "test-api-key"
_PRODUCT_CATALOG = "1.0"
_PRIMARY_KEY = "id"
_CURSOR_FIELD = "updated_at"
_NO_STATE = {}
_NOW = datetime.now(timezone.utc)

def _a_request() -> ChargebeeRequestBuilder:
    return ChargebeeRequestBuilder.addon_endpoint(_SITE, _SITE_API_KEY)

def _config() -> ConfigBuilder:
    return ConfigBuilder().with_site(_SITE).with_site_api_key(_SITE_API_KEY).with_product_catalog(_PRODUCT_CATALOG)

def _catalog(sync_mode: SyncMode) -> ConfiguredAirbyteCatalog:
    return CatalogBuilder().with_stream(_STREAM_NAME, sync_mode).build()

def _source() -> SourceChargebee:
    return SourceChargebee()

def _a_record() -> RecordBuilder:
    return create_record_builder(
        find_template(_STREAM_NAME, __file__),
        FieldPath("list"),
        record_id_path=NestedPath([_STREAM_NAME, _PRIMARY_KEY]),
        record_cursor_path=NestedPath([_STREAM_NAME, _CURSOR_FIELD])
    )

def _a_response() -> HttpResponseBuilder:
    return create_response_builder(
        find_template(_STREAM_NAME, __file__),
        FieldPath("list"),
        pagination_strategy=ChargebeePaginationStrategy()
    )

def _read(
    config_builder: ConfigBuilder,
    sync_mode: SyncMode,
    state: Optional[Dict[str, Any]] = None,
    expecting_exception: bool = False
) -> EntrypointOutput:
    catalog = _catalog(sync_mode)
    config = config_builder.build()
    return read(_source(), config, catalog, state, expecting_exception)

@freezegun.freeze_time(_NOW.isoformat())
class FullRefreshTest(TestCase):

    def setUp(self) -> None:
        self._now = _NOW
        self._now_in_seconds = int(self._now.timestamp())
        self._start_date = _NOW - timedelta(days=28)
        self._start_date_in_seconds = int(self._start_date.timestamp())

    @staticmethod
    def _read(config: ConfigBuilder, expecting_exception: bool = False) -> EntrypointOutput:
        return _read(config, SyncMode.full_refresh, expecting_exception=expecting_exception)

    @HttpMocker()
    def test_given_one_page_of_records_read_and_returned(self, http_mocker: HttpMocker) -> None:
        # Tests simple read
        http_mocker.get(
            _a_request().with_any_query_params().build(),
            _a_response().with_record(_a_record()).with_record(_a_record()).build()
        )
        output = self._read(_config().with_start_date(self._start_date))
        assert len(output.records) == 2

    @HttpMocker()
    def test_given_multiple_pages_of_records_read_and_returned(self, http_mocker: HttpMocker) -> None:
        # Tests pagination
        print("todo")

    @HttpMocker()
    def test_given_http_status_400_when_read_then_stream_is_ignored(self, http_mocker: HttpMocker) -> None:
        # Tests 400 status error handling
        http_mocker.get(
            _a_request().with_any_query_params().build(),
            a_response_with_status(400)
        )
        output = self._read(_config().with_start_date(self._start_date), expecting_exception=True)
        assert len(output.get_stream_statuses(f"{_STREAM_NAME}s")) == 0


    @HttpMocker()
    def test_given_http_status_401_when_the_stream_is_incomplete(self, http_mocker: HttpMocker) -> None:
        # Test 401 status error handling
        http_mocker.get(
            _a_request().with_any_query_params().build(),
            a_response_with_status(401),
        )
        output = self._read(_config().with_start_date(self._start_date), expecting_exception=True)
        assert output.errors[-1].trace.error.failure_type == FailureType.system_error

    @HttpMocker()
    def test_given_rate_limited_when_read_then_retry_and_return_records(self, http_mocker: HttpMocker) -> None:
        # Tests backoff/retry with rate limiting
        http_mocker.get(
            _a_request().with_any_query_params().build(),
            [
                a_response_with_status(429),
                _a_response().with_record(_a_record()).build(),
            ],
        )
        output = self._read(_config().with_start_date(self._start_date))
        assert len(output.records) == 1

    @HttpMocker()
    def test_given_http_status_500_once_before_200_when_read_then_retry_and_return_records(self, http_mocker: HttpMocker) -> None:
        # Tests retry with 500 status
        http_mocker.get(
            _a_request().with_any_query_params().build(),
            [a_response_with_status(500), _a_response().with_record(_a_record()).build()],
        )
        output = self._read(_config().with_start_date(self._start_date))
        assert len(output.records) == 1


    @HttpMocker()
    def test_given_http_status_500_on_availability_when_read_then_raise_system_error(self, http_mocker: HttpMocker) -> None:
        # Tests 500 status error handling
        http_mocker.get(
            _a_request().with_any_query_params().build(),
            a_response_with_status(500),
        )
        output = self._read(_config(), expecting_exception=True)
        assert output.errors[-1].trace.error.failure_type == FailureType.system_error

@freezegun.freeze_time(datetime.now(timezone.utc))
class IncrementalTest(TestCase):

    def setUp(self) -> None:
        print("todo")

    @staticmethod
    def _read(config: ConfigBuilder, state: Dict[str, Any], expecting_exception: bool = False) -> EntrypointOutput:
        return _read(config, SyncMode.incremental, state, expecting_exception=expecting_exception)

    @HttpMocker()
    def test_given_no_initial_state_when_read_then_return_state_based_on_cursor_field(self, http_mocker: HttpMocker) -> None:
        # Tests setting state
        print("todo")

    @HttpMocker()
    def test_given_state_when_read_then_use_state_for_query_params(self, http_mocker: HttpMocker) -> None:
        # Tests updating query params with state
        print("todo")

    @HttpMocker()
    def test_given_state_more_recent_than_cursor_when_read_then_return_state_based_on_cursor_field(self, http_mocker: HttpMocker) -> None:
        # Tests properly setting state with cursor field instead of more recent state
        print("todo")
