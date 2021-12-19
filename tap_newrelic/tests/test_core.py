"""Tests standard tap features using the built-in SDK tests library."""

import datetime
import json
import os
import uuid

import pendulum
from singer_sdk.testing import get_standard_tap_tests

from tap_newrelic.streams import SyntheticCheckStream
from tap_newrelic.tap import TapNewRelic

with open(".secrets/config.json") as f:
    SECRETS = json.load(f)

SAMPLE_CONFIG = {
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
    **SECRETS,
}

# Run standard built-in tap tests from the SDK:
def test_standard_tap_tests():
    """Run standard tap tests from the SDK."""
    tests = get_standard_tap_tests(TapNewRelic, config=SAMPLE_CONFIG)
    for test in tests:
        test()


with open(
    os.path.join(os.path.dirname(__file__), "fixtures", "sample_event.json")
) as f:
    SAMPLE_EVENT = json.load(f)


def encode_timestamp(s):
    return int(pendulum.parse(s).timestamp() * 1000)


def test_same_second_events_on_page_break():
    tap = TapNewRelic(config=SAMPLE_CONFIG)
    stream = SyntheticCheckStream(tap=tap)

    output = []

    timestamps = [
        # first page
        [
            {
                "timestamp": "2021-01-01T00:00:00.500",  # event 1
                "id": str(uuid.uuid4()),
            },
            {
                "timestamp": "2021-01-01T00:00:01.500",  # event 2
                "id": "5ea94a43-ca97-4d65-9b48-af86eb93512c",
            },
        ],
        # second page
        [
            {
                "timestamp": "2021-01-01T00:00:01.500",  # picks up event 2 again
                "id": "5ea94a43-ca97-4d65-9b48-af86eb93512c",
            },
            {
                "timestamp": "2021-01-01T00:00:01.750",  # event 3
                "id": str(uuid.uuid4()),
            },
            {
                "timestamp": "2021-01-01T00:00:01.750",  # event 4, same time as event 3
                "id": str(uuid.uuid4()),
            },
            {
                "timestamp": "2021-01-01T00:00:02.500",  # event 5
                "id": "a8d5c9db-c08d-4dc5-88fb-920e32389a53",
            },
        ],
    ]

    for page in timestamps:
        for event in page:
            row = {
                **SAMPLE_EVENT,
                "id": event["id"],
                "timestamp": encode_timestamp(event["timestamp"]),
            }
            processed_row = stream.post_process(row)
            output.append(processed_row)

    assert output[0]["timestamp"] == "2021-01-01T00:00:00.500000+00:00"  # first event
    assert output[1]["timestamp"] == "2021-01-01T00:00:01.500000+00:00"  # second event
    assert output[1]["id"] == "5ea94a43-ca97-4d65-9b48-af86eb93512c"
    # should not re-emit duplicate row
    assert output[2] == None

    assert output[3]["id"] != "5ea94a43-ca97-4d65-9b48-af86eb93512c"
    assert output[3]["timestamp"] == "2021-01-01T00:00:01.750000+00:00"
    # should output new row with same timestamp
    assert output[4]["timestamp"] == "2021-01-01T00:00:01.750000+00:00"
    assert output[5]["timestamp"] == "2021-01-01T00:00:02.500000+00:00"
    assert output[5]["id"] == "a8d5c9db-c08d-4dc5-88fb-920e32389a53"

    assert stream.latest_timestamp == "2021-01-01T00:00:02.500000+00:00"
    assert stream.latest_id == ["a8d5c9db-c08d-4dc5-88fb-920e32389a53"]
