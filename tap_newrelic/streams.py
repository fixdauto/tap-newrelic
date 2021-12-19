"""Stream class for tap-newrelic."""
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Union

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


def unix_timestamp_to_iso8601(timestamp):
    return str(pendulum.from_timestamp(timestamp / 1000))


class NewRelicStream(GraphQLStream):
    """NewRelic stream class."""

    primary_keys = ["id"]
    replication_method = "INCREMENTAL"
    replication_key = "timestamp"
    is_timestamp_replication_key = True
    is_sorted = True
    latest_timestamp = None

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

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self.config["api_url"]

    @property
    def authenticator(self) -> APIAuthenticatorBase:
        return SimpleAuthenticator(
            stream=self, auth_headers={"API-Key": self.config.get("api_key")}
        )

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        if not next_page_token:
            next_page_token = self.get_starting_timestamp(context)
            self.latest_timestamp = next_page_token.isoformat()
        # NQRL only supports timestamps to second resolution
        next_page_token = next_page_token.set(microsecond=0)
        nqrl = self.nqrl_query.format(
            next_page_token.strftime(self.datetime_format),
            self.get_replication_key_signpost(context).strftime(self.datetime_format),
        )
        self.logger.debug(nqrl)
        return {
            "accountId": self.config.get("account_id"),
            "query": nqrl,
        }

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Any:
        latest = pendulum.parse(self.latest_timestamp)
        # TODO: parses the response twice, which is a little gross
        if len([v for v in self.parse_response(response)]) == 0:
            return None
        if previous_token and latest == previous_token:
            return None

        return latest

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        # For some reason, GraphQLStream re-implements parse_response,
        # not supporting jsonpath. Here we delegate specifically to
        # RESTStream's version which does support jsonpath
        return RESTStream.parse_response(self, response)

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        row["timestamp"] = unix_timestamp_to_iso8601(row["timestamp"])
        if self._check_duplicates(row):
            self.logger.info(f"skipping duplicate {row['timestamp']}")
            return None

        # snake case
        return {inflection.underscore(k): v for k, v in row.items()}

    def _check_duplicates(self, row: dict) -> bool:
        # Annoyingly, NewRelic resources have timestamps in millisecond resolution
        # but NQRL only supports querying to second resolution.
        # This means sometimes you get duplicate rows from the same second which
        # breaks GraphQLStream's detection of out-of-order rows. We can simply skip
        # these rows because they've already been posted.
        timestamp = row["timestamp"]
        ids = [row[k] for k in self.primary_keys]
        if self.latest_timestamp:
            if timestamp < self.latest_timestamp:
                return True
            if timestamp == self.latest_timestamp and self.latest_id == ids:
                # special case, two events can have identical timestamps
                return True
        self.latest_timestamp = timestamp
        self.latest_id = ids
        return False


class SyntheticCheckStream(NewRelicStream):
    name = "synthetic_checks"

    nqrl_query = "SELECT * FROM SyntheticCheck SINCE '{}' UNTIL '{}' ORDER BY timestamp LIMIT MAX"

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
