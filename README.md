# tap-newrelic

`tap-newrelic` is a Singer tap for NewRelic.

Build with the [Singer SDK](https://gitlab.com/meltano/singer-sdk).

It uses the [NerdGraph API](https://docs.newrelic.com/docs/apis/nerdgraph/get-started/introduction-new-relic-nerdgraph/) to fetch data using [NRQL](https://docs.newrelic.com/docs/query-your-data/nrql-new-relic-query-language/get-started/nrql-syntax-clauses-functions/).

Currently, only synthetics checks data (`SyntheticCheck`) and logs (`Log`) are natively supported, however the tap supports custom queries for any query that is orderable by `timestamp`. In addition it
should be streightforward to add [other data sources](https://docs.newrelic.com/docs/query-your-data/nrql-new-relic-query-language/get-started/introduction-nrql-new-relics-query-language/#what-you-can-query), PRs accepted.

## Installation

```bash
pip install tap-newrelic
```

## Configuration

### Accepted Config Options

```js
{
  // required:
  "api_key": "ABCD-XXXXXXXXXXXXXXXXXXXXXXXXXXX",
  "account_id": 12345678,
  "start_date": "2021-01-00T00:00:00Z",
  // optional, defaults to https://api.newrelic.com/graphql
  "api_url": "https://api.eu.newrelic.com/graphql",
  // optional, a list of custom queries to run
  "custom_queries": {
    "name": "my_custom_event",
    "query": "SELECT * FROM my_custom_event"
  }
}
```

A full list of supported settings and capabilities for this
tap is available by running:

```bash
tap-newrelic --about
```

### Source Authentication and Authorization

Use of the API requires an [API Key](https://docs.newrelic.com/docs/apis/nerdgraph/get-started/introduction-new-relic-nerdgraph/#explorer).

## Usage

You can easily run `tap-newrelic` by itself or in a pipeline using [Meltano](www.meltano.com).

### Executing the Tap Directly

```bash
tap-newrelic --version
tap-newrelic --help
tap-newrelic --config CONFIG --discover > ./catalog.json
```

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tap_newrelic/tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `tap-newrelic` CLI interface directly using `poetry run`:

```bash
poetry run tap-newrelic --help
```

### Testing with [Meltano](meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-newrelic
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-newrelic --version
# OR run a test `elt` pipeline:
meltano elt tap-newrelic target-jsonl
```

### Singer SDK Dev Guide

See the [dev guide](https://gitlab.com/meltano/singer-sdk/-/blob/main/docs/dev_guide.md) for more instructions on how to use the Singer SDK to
develop your own taps and targets.
