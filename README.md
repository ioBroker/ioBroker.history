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
### 5.0.0 (2026-08-26)
* (ioBroker-Bot) Adapter requires js-controller >= 6.0.11 now.
* (simatec) Responsive Design added
* (@GermanBluefox) Adapter requires node.js >= 22 now.
* (@GermanBluefox) Added the data browser to the configuration, so the stored values can be viewed, edited and deleted.
* (@GermanBluefox) The aggregation is used now from `@iobroker/aggregate` and is shared with the SQL and InfluxDB adapters.

### 4.0.0 (2026-03-10)
* (iobroker-bot) Adapter requires node.js >= 20 now.
* (@GermanBluefox) Migrated to TypeScript

### 3.0.1 (2023-10-24)
* (tuxyme) activated the round option when averaging

### 3.0.0 (2023-09-19)
* (foxriver76) fix `history2db.js` with controller v5
* (bluefox) Minimal node.sj version is 16
* (bluefox) Added support for `count` aggregate type on getHistory

### 2.2.6 (2023-08-23)
* (Apollon77) Fix getHistory when aggregations were used in some cases

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
