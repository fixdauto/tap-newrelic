# 0.2.2

- Add support for changing `key_properties` of custom query streams

# 0.2.1

- Fix: re-enable support for python3.7 environments [#3](https://github.com/fixdauto/tap-newrelic/pull/3)

# 0.2.0

- Add support for `Log` stream
- Add support for custom query streams using the `custom_queries` config option
- Update SDK to `0.6.1`

# 0.1.2

- Update SDK to `0.3.17` [#2](https://github.com/fixdauto/tap-newrelic/pull/2)
- Add support for Python versions up to 3.10 [#2](https://github.com/fixdauto/tap-newrelic/pull/2)
- Fix a bug where the same record would be emitted at the beginning of every page. [#2](https://github.com/fixdauto/tap-newrelic/pull/2)

# 0.1.1

- Fix for InvalidStreamSortException from dates in state with sub-second resolution [#1](https://github.com/fixdauto/tap-newrelic/pull/1)

# 0.0.1

Initial Release
