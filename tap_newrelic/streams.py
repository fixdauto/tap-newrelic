"""Stream class for tap-newrelic."""
import inflection
import pendulum
import requests

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk.streams import GraphQLStream

from singer_sdk.authenticators import (
    APIAuthenticatorBase,
    SimpleAuthenticator,
)

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

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self.config["api_url"]

    @property
    def authenticator(self) -> APIAuthenticatorBase:
        return SimpleAuthenticator(
            stream=self,
            auth_headers={
                "API-Key": self.config.get("api_key")
            }
        )

    def get_url_params(self, partition, next_page_token: Optional[DateTimeType] = None) -> dict:
        if not next_page_token:
            next_page_token = self.get_starting_timestamp(partition)
            self.latest_timestamp = next_page_token.isoformat()
        # NQRL only supports timestamps to second resolution
        next_page_token = next_page_token.set(microsecond=0)
        nqrl = self.nqrl_query.format(
            next_page_token.strftime(self.datetime_format),
            self.get_replication_key_signpost(partition).strftime(self.datetime_format),
        )
        self.logger.debug(nqrl)
        return {
            "accountId": self.config.get("account_id"),
            "query": nqrl,
        }

    def get_next_page_token(self, response, previous_token):
        latest = pendulum.parse(self.latest_timestamp)
        if self.results_count == 0:
            return None
        if previous_token and latest == previous_token:
            return None

        return latest

    def parse_response(self, response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        resp_json = response.json()
        try:
            results = resp_json["data"]["actor"]["account"]["nrql"]["results"]
            self.results_count = len(results)
            for row in results:
                latest_row = self.transform(row)
                if self.latest_timestamp and latest_row["timestamp"] < self.latest_timestamp:
                    # Because NRQL doesn't take timestamps down to miliseconds, sometimes you get
                    # duplicate rows from the same second which breaks GraphQLStream's detection
                    # of out-of-order rows. We can simply skip these rows because they've already
                    # been posted
                    self.logger.info(f"skipping duplicate {latest_row['timestamp']}")
                    continue
                self.latest_timestamp = latest_row["timestamp"]
                yield latest_row
        except Exception as err:
            self.logger.warn(f"Problem with response: {resp_json}")
            raise err

    def transform(self, row: dict, partition: Optional[dict] = None) -> dict:
        row["timestamp"] = unix_timestamp_to_iso8601(row["timestamp"])
        return { inflection.underscore(k): v for k, v in row.items() }


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
        Property("monitor_extended_type", StringType), # TODO: enum
        Property("monitor_id", StringType),
        Property("monitor_name", StringType),
        Property("error", StringType),
        Property("result", StringType), # TODO: enum
        Property("secure_credentials", StringType),
        Property("timestamp", DateTimeType),
        Property("total_request_body_size", IntegerType),
        Property("total_request_header_size", IntegerType),
        Property("total_response_body_size", IntegerType),
        Property("total_response_header_size", IntegerType),
        Property("type", StringType), # TODO: enum
        Property("type_label", StringType),
    ).to_dict()
