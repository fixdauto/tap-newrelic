"""Stream class for tap-newrelic."""
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

import inflection
import pendulum
import requests
from singer_sdk.authenticators import APIAuthenticatorBase, SimpleAuthenticator
from singer_sdk.streams import GraphQLStream
from singer_sdk.streams.rest import RESTStream
from singer_sdk.typing import (
    BooleanType,
    DateTimeType,
    IntegerType,
    NumberType,
    PropertiesList,
    Property,
    StringType,
)


def unix_timestamp_to_datetime(timestamp: int) -> datetime:
    """Convert unix timestamp in integer miliseconds to a datetime object."""
    return pendulum.from_timestamp(timestamp / 1000)


def snake_case(row: dict) -> dict:
    """Convert object keys to snake case."""
    return {inflection.underscore(k): v for k, v in row.items()}


class NewRelicStream(GraphQLStream):
    """NewRelic stream class."""

    primary_keys = ["id"]
    replication_method = "INCREMENTAL"
    replication_key = "timestamp"
    is_timestamp_replication_key = True
    is_sorted = True
    _latest_timestamp: Optional[datetime] = None
    _latest_id: Optional[List[Any]] = None

    datetime_format = "%Y-%m-%d %H:%M:%S"
    query = """
        query ($accountId: Int!, $query: Nrql!) {
          actor {
            account(id: $accountId) {
              nrql(query: $query) {
                results
              }
            }
          }
        }
    """
    records_jsonpath: str = "$.data.actor.account.nrql.results[*]"
    nqrl_query: str

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self.config["api_url"]

    @property
    def authenticator(self) -> APIAuthenticatorBase:
        """Return or set the authenticator for managing HTTP auth headers.

        If an authenticator is not specified, REST-based taps will simply pass
        `http_headers` as defined in the stream class.

        Returns
        -------
            Authenticator instance that will be used to authenticate all outgoing
            requests.

        """
        return SimpleAuthenticator(
            stream=self, auth_headers={"API-Key": self.config.get("api_key")}
        )

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[datetime]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        If paging is supported, developers may override with specific paging logic.

        Args:
            context: Stream partition or context dictionary.
            next_page_token: Token, page number or any request argument to request the
                next page of data.

        Returns
        -------
            Dictionary of URL query parameters to use in the request.

        """
        next_page_timestamp = pendulum.instance(
            next_page_token
            or self.get_starting_timestamp(context)
            or pendulum.from_timestamp(0)
        )
        self._latest_timestamp = next_page_timestamp
        # NQRL only supports timestamps to second resolution
        next_page_timestamp = next_page_timestamp.set(microsecond=0)
        replication_key_signpost = self.get_replication_key_signpost(context)
        assert isinstance(replication_key_signpost, datetime)
        nqrl = self.nqrl_query.format(
            next_page_timestamp.strftime(self.datetime_format),
            replication_key_signpost.strftime(self.datetime_format),
        )
        self.logger.debug(nqrl)
        return {
            "accountId": self.config.get("account_id"),
            "query": nqrl,
        }

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[datetime]
    ) -> Optional[datetime]:
        """Return token identifying next page or None if all records have been read.

        Args:
            response: A raw `requests.Response`_ object.
            previous_token: Previous pagination reference.

        Returns
        -------
            Reference value to retrieve next page.

        .. _requests.Response:
            https://docs.python-requests.org/en/latest/api/#requests.Response

        """
        assert self._latest_timestamp is not None
        # TODO: parses the response twice, which is a little gross
        if len([v for v in self.parse_response(response)]) == 0:
            return None
        if previous_token and self._latest_timestamp == previous_token:
            return None

        return self._latest_timestamp

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows.

        Args:
            response: A raw `requests.Response`_ object.

        Yields
        ------
            One item for every item found in the response.

        .. _requests.Response:
            https://docs.python-requests.org/en/latest/api/#requests.Response

        """
        # For some reason, GraphQLStream re-implements parse_response,
        # not supporting jsonpath. Here we delegate specifically to
        # RESTStream's version which does support jsonpath
        return RESTStream.parse_response(self, response)

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        """As needed, append or transform raw data to match expected structure.

        Optional. This method gives developers an opportunity to "clean up" the results
        prior to returning records to the downstream tap - for instance: cleaning,
        renaming, or appending properties to the raw record result returned from the
        API.

        Developers may also return `None` from this method to filter out
        invalid or not-applicable records from the stream.

        Args:
            row: Individual record in the stream.
            context: Stream partition or context dictionary.

        Returns
        -------
            The resulting record dict, or `None` if the record should be excluded.

        """
        row["timestamp"] = unix_timestamp_to_datetime(row["timestamp"])
        if self._check_duplicates(row):
            self.logger.info(f"skipping duplicate {row['timestamp']}")
            return None

        row["timestamp"] = row[
            "timestamp"
        ].isoformat()  # from datetime object to string
        return snake_case(row)

    def _check_duplicates(self, row: dict) -> bool:
        # Annoyingly, NewRelic resources have timestamps in millisecond resolution
        # but NQRL only supports querying to second resolution.
        # This means sometimes you get duplicate rows from the same second which
        # breaks GraphQLStream's detection of out-of-order rows. We can simply skip
        # these rows because they've already been posted.
        timestamp = row["timestamp"]
        ids = [row[k] for k in self.primary_keys]
        if self._latest_timestamp:
            if timestamp < self._latest_timestamp:
                return True
            if timestamp == self._latest_timestamp and self._latest_id == ids:
                # special case, two events can have identical timestamps
                return True
        self._latest_timestamp = timestamp
        self._latest_id = ids
        return False


class SyntheticCheckStream(NewRelicStream):
    """Stream for reading Synthetic check results.

    https://docs.newrelic.com/docs/synthetics/
    """

    name = "synthetic_checks"

    nqrl_query = (
        "SELECT * FROM SyntheticCheck SINCE '{}' UNTIL '{}' "
        "ORDER BY timestamp LIMIT MAX"
    )

    schema = PropertiesList(
        Property("duration", NumberType),
        Property("entity_guid", StringType),
        Property("has_user_defined_headers", BooleanType),
        Property("id", StringType),
        Property("location", StringType),
        Property("location_label", StringType),
        Property("minion", StringType),
        Property("minion_container_system", StringType),
        Property("minion_container_system_version", StringType),
        Property("minion_deployment_mode", StringType),
        Property("minion_id", StringType),
        Property("monitor_extended_type", StringType),  # TODO: enum
        Property("monitor_id", StringType),
        Property("monitor_name", StringType),
        Property("error", StringType),
        Property("result", StringType),  # TODO: enum
        Property("secure_credentials", StringType),
        Property("timestamp", DateTimeType),
        Property("total_request_body_size", IntegerType),
        Property("total_request_header_size", IntegerType),
        Property("total_response_body_size", IntegerType),
        Property("total_response_header_size", IntegerType),
        Property("type", StringType),  # TODO: enum
        Property("type_label", StringType),
    ).to_dict()
