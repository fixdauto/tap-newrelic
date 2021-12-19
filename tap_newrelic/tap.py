"""NewRelic tap class."""

from pathlib import Path
from typing import List

from singer_sdk import Stream, Tap
from singer_sdk.typing import (
    ArrayType,
    BooleanType,
    DateTimeType,
    IntegerType,
    NumberType,
    ObjectType,
    PropertiesList,
    Property,
    StringType,
)

from tap_newrelic.streams import SyntheticCheckStream

STREAM_TYPES = [
    SyntheticCheckStream,
]


class TapNewRelic(Tap):
    """NewRelic tap class."""

    name = "tap-newrelic"

    config_jsonschema = PropertiesList(
        Property("api_key", StringType, required=True),
        Property("api_url", StringType, default="https://api.newrelic.com/graphql"),
        Property("account_id", IntegerType, required=True),
        Property("start_date", DateTimeType, required=True),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]


# CLI Execution:

cli = TapNewRelic.cli
