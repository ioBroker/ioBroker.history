![Logo](admin/history.png)
# ioBroker.history
===============================
[![NPM version](http://img.shields.io/npm/v/iobroker.history.svg)](https://www.npmjs.com/package/iobroker.history)
[![Downloads](https://img.shields.io/npm/dm/iobroker.history.svg)](https://www.npmjs.com/package/iobroker.history)

[![NPM](https://nodei.co/npm/iobroker.history.png?downloads=true)](https://nodei.co/npm/iobroker.history/)

This adapter saves state history in a two-staged process. 
At first data points are stored in RAM, as soon as they reach maxLength they will be stored on disk.

To set up some data points to be stored they must be configured in admin "Objects" Tab (last button).

## Settings

- **Storage directory** - Path to the directory, where the files will be stored. It can be done relative to "iobroker-data" or absolute, like "/mnt/history" or "D:/History"
- **Maximal number of stored in RAM values** - After this number of values reached in RAM they will be saved on disk.
- **Store origin of value** - If "from" field will be stored too. Can save place on disk.
- **De-bounce interval** - Protection against too often changes of some value. 
- **Storage retention** - How many values in the past will be stored on disk.

## Changelog
### 0.4.0 (2016-05-05)
* (bluefox) use aggregation file from sql adapter
* (bluefox) fix the values storage on exit
* (bluefox) store all cached data every 5 minutes
* (bluefox) support of ms

### 0.2.1 (2015-12-14)
* (bluefox) add description of settings
* (bluefox) place aggregate function into separate file to enable sharing with other adapters
* (smiling-Jack) Add generate Demo data
* (smiling-Jack) get history in own fork
* (bluefox) add storeAck flag
* (bluefox) mockup for onchange

### 0.2.0 (2015-11-15)
* (Smiling_Jack) save and load in adapter and not in js-controller
* (Smiling_Jack) aggregation of data points
* (Smiling_Jack) support of storage path

### 0.1.3 (2015-02-19)
* (bluefox) fix small error in history (Thanks on Dschaedl)
* (bluefox) update admin page

### 0.1.2 (2015-01-20)
* (bluefox) enable save&close button by config

### 0.1.1 (2015-01-10)
* (bluefox) check if state was not deleted

### 0.1.0 (2015-01-02)
* (bluefox) enable npm install

### 0.0.8 (2014-12-25)
* (bluefox) support of de-bounce interval

### 0.0.7 (2014-11-01)
* (bluefox) store every change and not only lc != ts

### 0.0.6 (2014-10-19)
* (bluefox) add configuration page

## License

The MIT License (MIT)

Copyright (c) 2014-2015 Bluefox, Smiling_Jack

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
