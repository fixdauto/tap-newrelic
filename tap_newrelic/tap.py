"""NewRelic tap class."""

from typing import List

from singer_sdk import Stream, Tap
from singer_sdk.typing import (
    ArrayType,
    DateTimeType,
    IntegerType,
    ObjectType,
    PropertiesList,
    Property,
    StringType,
)

from tap_newrelic.streams import CustomQueryStream, LogStream, SyntheticCheckStream

STREAM_TYPES = [
    SyntheticCheckStream,
    LogStream,
]


class TapNewRelic(Tap):
    """NewRelic tap class."""

    name = "tap-newrelic"

    config_jsonschema = PropertiesList(
        Property("api_key", StringType, required=True),
        Property("api_url", StringType, default="https://api.newrelic.com/graphql"),
        Property("account_id", IntegerType, required=True),
        Property("start_date", DateTimeType, required=True),
        Property(
            "custom_queries",
            ArrayType(
                ObjectType(
                    Property("name", StringType),
                    Property(
                        "query",
                        StringType,
                        description="The NRQL query to execute. "
                        "Required to be able to end with "
                        "`SINCE X UNTIL Y ORDER BY TIMESTAMP LIMIT MAX`",
                    ),
                )
            ),
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        base_streams: List[Stream] = [
            stream_class(tap=self) for stream_class in STREAM_TYPES
        ]
        custom_streams: List[Stream] = [
            CustomQueryStream(tap=self, name=custom["name"])
            for custom in self.config.get("custom_queries", [])
        ]
        return base_streams + custom_streams


# CLI Execution:

cli = TapNewRelic.cli
