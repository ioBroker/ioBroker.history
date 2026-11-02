![Logo](admin/history.png)
# ioBroker.history

![Number of Installations](http://iobroker.live/badges/history-installed.svg)
![Number of Installations](http://iobroker.live/badges/history-stable.svg)
[![NPM version](http://img.shields.io/npm/v/iobroker.history.svg)](https://www.npmjs.com/package/iobroker.history)

![Test and Release](https://github.com/ioBroker/iobroker.history/workflows/Test%20and%20Release/badge.svg)
[![Translation status](https://weblate.iobroker.net/widgets/adapters/-/history/svg-badge.svg)](https://weblate.iobroker.net/engage/adapters/?utm_source=widget)
[![Downloads](https://img.shields.io/npm/dm/iobroker.history.svg)](https://www.npmjs.com/package/iobroker.history)

This adapter saves state history in a two-staged process.

**This adapter uses Sentry libraries to automatically report exceptions and code errors to the developers.** For more details and for information on how to disable the error reporting, see [Sentry-Plugin Documentation](https://github.com/ioBroker/plugin-sentry#plugin-sentry)! Sentry reporting is used starting with js-controller 3.0.

## Configuration
* [English description](docs/en/README.md)
* [Deutsche Beschreibung](docs/de/README.md)

<!--
	Placeholder for the next version (at the beginning of the line):
	### **WORK IN PROGRESS**
-->

## Changelog

### **WORK IN PROGRESS**
- (copilot) Adapter requires node.js >= 22.19.0 now
### 5.2.0 (2026-10-29)
* (@GermanBluefox) Fixed the crash `Cannot read properties of undefined (reading 'val'/'ts')` in `getHistory` (#575, #490, #470, #396)
* (@GermanBluefox) `getHistory` with `aggregate: 'none'` and `count`/`limit` returns now the oldest entries from the files, if `returnNewestEntries` is not set (#530, #331, #210)
* (@GermanBluefox) `getHistory` accepts `intervalBorders`: the client can send the borders of the intervals itself, e.g. the local days of a chart. With `count` or `step` every interval after a switch of the daylight saving time ended an hour away from the midnight, so a value written at 23:59:59 was shown on the next day (ioBroker/ioBroker.echarts#1311)
* (@GermanBluefox) The number of parallel history queries can be limited now (default: 3). Further queries are queued (#536)
* (@GermanBluefox) Updated packages

### 5.1.2 (2026-10-23)
* (@GermanBluefox) The timed relog (`changesRelogInterval`) starts right after the adapter start also when "Write NULL values on start/stop boundaries" (`writeNulls`) is off, not only with the first update of the state

### 5.1.1 (2026-10-13)
* (@GermanBluefox) Values stored with `storeState` for a datapoint without history settings no longer get an additional `null` value when the object of the datapoint changes

### 5.1.0 (2026-10-13)
* (@GermanBluefox) Admin 7.6.20+ is required. Added Ukrainian translation. Removed the obsolete "subscribe" attribute
* (@GermanBluefox) Added the "Statistics" tab: values, size on disk, files and status of every datapoint, and a cleanup of the data of datapoints that are not logged any more
* (@GermanBluefox) The data browser shows now if a value to edit or delete was not found, and the error of a failed save is no longer hidden by the reload of the table

### 5.0.1 (2026-08-26)
* (@joltcoke) Fixed average and total returning null for every interval that contains a null value: parseFloat(null) is NaN and poisoned the sum of the whole interval (thanks to @joltcoke, ioBroker/ioBroker.sql#526). As the result was NaN and not null, ignoreNull could not act on it either
* (@joltcoke) Fixed min returning a wrong value if the interval contains a null, minmax losing the minimum if the interval starts with a null, and percentile/quantile counting a null as 0

[Older changelogs can be found there](CHANGELOG_OLD.md)

## License

The MIT License (MIT)

Copyright (c) 2014-2026 Bluefox <dogafox@gmail.com>, Apollon77

Copyright (c) 2016 Smiling_Jack

Copyright (c) 2014 hobbyquaker

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
