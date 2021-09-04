/* jshint -W097 */// jshint strict:false
/*jslint node: true */
"use strict";

//noinspection JSUnresolvedFunction

//usage: nodejs history2db.js [<DB-Instance>] [<Loglevel>] [<Date-to-start>|0] [<path-to-Data>] [<delayMultiplicator>] [--logChangesOnly [<relog-Interval(s)>]] [--ignoreExistingDBValues]
//usage: nodejs history2db.js influxdb.0 info 20161001 /path/to/data
const utils   = require('@iobroker/adapter-core'); // Get common adapter utils

var fs        = require('fs');
var path      = require('path');
var dataDir   = path.normalize(utils.controllerDir + '/' + require(utils.controllerDir + '/lib/tools').getDefaultDataDir());
var historydir = dataDir + 'history-data';

var earliestDBValue = {};
var earliesValCachefile = __dirname + '/earliestDBValues.json';
var earliesValCachefileExists = false;

var existingDBValues = {};
var existingDataCachefile = __dirname + '/existingDBValues.json';
var processNonExistingValues = false;

var existingTypes = {};
var existingTypesCachefile = __dirname + '/existingDBTypes.json';
var existingTypesCachefileExists = false;

var processAllDPs = false;
var simulate = false;

var dbInstance = "";
var endDay = 0; // 20160917; // or 0
var ignoreEarliesDBValues = false;
var logChangesOnly = false;
var logChangesOnlyTime = 60*60*1000;
var delayMultiplicator = 1;
var processCounter = 0;

if (process.argv[2]) {
    dbInstance = process.argv[2];
    if ((process.argv[4]) && (parseInt(process.argv[4],10) > 0)) endDay = parseInt(process.argv[4], 10);
    if (process.argv[5]) historydir = process.argv[5];
    if ((process.argv[6]) && (!isNaN(parseFloat(process.argv[6])))) delayMultiplicator = parseFloat(process.argv[6]);
    if (process.argv.indexOf('--ignoreExistingDBValues') !== -1) ignoreEarliesDBValues = true;
    if (process.argv.indexOf('--processNonExistingValuesOnly') !== -1) {
        ignoreEarliesDBValues = true;
        processNonExistingValues = true;
    }
    if (process.argv.indexOf('--processAllDPs') !== -1) processAllDPs = true;
    if (process.argv.indexOf('--simulate') !== -1) simulate = true;
    var logchangesPos = process.argv.indexOf('--logChangesOnly');
    if (logchangesPos !== -1) {
        logChangesOnly = true;
        if (process.argv[logchangesPos+1]) {
            var logTime = parseInt(process.argv[logchangesPos+1], 10);
            if ((! isNaN(logTime)) &&  (logTime > 0)) {
                logChangesOnlyTime = logTime * 60000;
            }
        }
    }
    process.argv[2] = "--install";
}
else {
    console.log("ERROR: DB-Instance missing");
    process.exit();
}
console.log('Send Data to ' + dbInstance);
if (endDay !== 0) console.log('Start at ' + endDay);
console.log('Use historyDir ' + historydir);
if (delayMultiplicator != 1) console.log('Use Delay multiplicator ' + delayMultiplicator);
if (logChangesOnly) console.log('Log changes only once per ' + (logChangesOnlyTime/60000) + ' minutes');

var adapter = utils.Adapter('history');

var breakIt = false;

var stdin = process.stdin;
// without this, we would only get streams once enter is pressed
stdin.setRawMode( true );
// resume stdin in the parent process (node app won't quit all by itself
// unless an error or process.exit() happens)
stdin.resume();
// i don't want binary, do you?
stdin.setEncoding( 'utf8' );
// on any data into stdin
stdin.on( 'data', function( key ){
  // ctrl-c ( end of text )
  if ( key === 'x' || key === '\u0003') {
    breakIt = true;
  }
  // write the key to stdout all normal like
  console.log('Received Keypress: ' + key);
});

adapter.on('ready', function () {
    main();
});

function main() {
    if (processNonExistingValues) {
        try {
            if (fs.statSync(existingDataCachefile).isFile()) {
                var exFileContent = fs.readFileSync(existingDataCachefile);
                existingDBValues = JSON.parse(exFileContent);
                ignoreEarliesDBValues = true;
                console.log('ExistingDBValues initialized from cache ' + Object.keys(existingDBValues).length);
            }
        }
        catch (err) {
            console.log("ExistingValues File not existing, but should be used. EXIT");
            process.exit();
        }
    }
    var fileContent;
    try {
        if (fs.statSync(earliesValCachefile).isFile()) {
            fileContent = fs.readFileSync(earliesValCachefile);
            earliestDBValue = JSON.parse(fileContent);
            console.log('EarliesDBValues initialized from cache ' + Object.keys(earliestDBValue).length);
            earliesValCachefileExists = true;
            if (ignoreEarliesDBValues) {
                var dateNow = Date.now();
                for (var id in earliestDBValue) {
                    if (!earliestDBValue.hasOwnProperty(id)) continue;
                    earliestDBValue[id] = dateNow;
                }
                console.log('EarliesDBValues overwritten with ' + dateNow);
            }
        }
        else {
            earliesValCachefileExists = false;
        }
    }
    catch (err) {
        console.log('No stored earliesDBValues found');
    }
    try {
        if (fs.statSync(existingTypesCachefile).isFile()) {
            fileContent = fs.readFileSync(existingTypesCachefile);
            existingTypes = JSON.parse(fileContent);
            console.log('ExistingDBTypes initialized from cache ' + Object.keys(existingTypes).length);
            existingTypesCachefileExists = true;
        }
        else {
            existingTypesCachefileExists = false;
        }
    }
    catch (err) {
        console.log('No stored existingDBTypes found');
    }
    processFiles();
}

var allFiles = {};

function processFiles() {
    console.log('started processFiles with ' + Object.keys(earliestDBValue).length + ' known db values');

    if (endDay === 0) {
        var endDayTs = 0;
        for (var id in earliestDBValue) {
            if (!earliestDBValue.hasOwnProperty(id)) continue;
            if (earliestDBValue[id] > endDayTs) {
                endDayTs = earliestDBValue[id];
                //console.log('new minimum = ' + id + '(' + endDayTs + ')');
            }
        }
        endDay = parseInt(ts2day(endDayTs), 10);
    }
    console.log('We start earliest at ' + endDay);

    // get list of directories
    var dayList = getDirectories(historydir).sort(function (a, b) {
        return b - a;
    });

    for (var i = 0; i < dayList.length; i++) {
        var day = parseInt(dayList[i], 10);
        if ((!isNaN(day)) && (day<=endDay)) {
            var dir = historydir + '/' + dayList[i].toString() + '/';
            allFiles[dayList[i].toString()]={};
            allFiles[dayList[i].toString()].dirname = dir;
            allFiles[dayList[i].toString()].files = getFiles(dir);
        }
    }
    processFile();
}

function processFile() {
    if (breakIt) finish(true);
    if (Object.keys(allFiles).length === 0) finish(true);

    var day = parseInt(Object.keys(allFiles)[Object.keys(allFiles).length-1], 10);

    if (allFiles[day].files.length>0) {
        var dir = allFiles[day].dirname;
        var file = allFiles[day].files.shift();
        var id = file.substring(8,file.length-5);
        var weatherunderground_special_handling = ((id.indexOf('weatherunderground') !== -1) && (id.indexOf('current.precip') !== -1));
        console.log('Day ' + day + ' - ' + file );

        if (earliesValCachefileExists) {
            if ((!earliestDBValue[id]) || (earliestDBValue[id] === 0)) {
                console.log('    Ignore ID ' + file +': ' + id);
                setTimeout(processFile,10);
                return;
            }
        }
        if (!earliesValCachefileExists || processAllDPs) {
            if (!earliestDBValue[id]) earliestDBValue[id] = Date.now();
        }
        if (processNonExistingValues) {
            existingDBValues[id] && console.log("Check: "+day+" / pos " + existingDBValues[id].indexOf(day) /*+ " :" +JSON.stringify(existingDBValues[id])*/);
            if ((existingDBValues[id]) && (existingDBValues[id].indexOf(day) !== -1)) {
                console.log('    Ignore existing ID ' + file +': ' + id);
                setTimeout(processFile,10);
                return;
            }
        }

        var fileData;
        try {
            var fileContent = fs.readFileSync(dir + '/' + file);

            fileData = JSON.parse(fileContent, function (key, value) {
                if (key === 'ts') {
                    if (value < 946681200000) value *= 1000;
                }
                else if (key === 'ack') {
                    value = !!value;
                }
                else if (key === 'val') {
                    if ((weatherunderground_special_handling) && (typeof value === 'string')) value = parseInt(value, 10);
                }
                return value;
            });
        } catch (e) {
            console.log('Cannot parse file ' + dir + '/' + file + ': ' + e.message);
        }

        //console.log('    File ' + j +': ' + id + ' --> ' + fileData.length);

        if (fileData && fileData[fileData.length - 1].ts >= earliestDBValue[id]) {
            var k;
            for (k = 0; k < fileData.length; k++) {
                if (fileData[k].ts >= earliestDBValue[id]) break;
            }
            fileData = fileData.slice(0,k);
            console.log('cut filedata to ' + fileData.length);
        }
        var lastValue = null;
        var lastTime = null;
        if (fileData && fileData.length > 0) {
            var sendData = {};
            sendData.id = id;
            sendData.state = [];
            for (var j = 0; j< fileData.length; j++) {
                if (fileData[j].ts < earliestDBValue[id]) earliestDBValue[id] = fileData[j].ts;
                if ((lastValue === null) || (fileData[j].val != lastValue) || (!logChangesOnly) || ((logChangesOnly) && (Math.abs(fileData[j].ts-lastTime) > logChangesOnlyTime))) {
                    sendData.state.push(fileData[j]);
                    lastValue = fileData[j].val;
                    lastTime = fileData[j].ts;
                    //console.log('use value = ' + fileData[j].val)
                }
                //else console.log('not use value = ' + fileData[j].val)
            }
            console.log('  datapoints reduced from ' + fileData.length + ' --> ' + sendData.state.length);
            if (existingTypesCachefileExists) {
                if (!existingTypes[id]) {
                    existingTypes[id] = typeof sendData.state[sendData.state.length-1].val;
                    console.log('  used last value to initialize type: ' + existingTypes[id]);
                }
                var sortedOut = 0;
                for (var jj = 0; jj< sendData.state.length; jj++) {
                    var currType = typeof sendData.state[jj].val;
                    if (currType != existingTypes[id]) {
                        switch (existingTypes[id]) {
                            case 'number':      switch (currType) {
                                                    case 'boolean': if (sendData.state[jj].val === false) sendData.state[jj].val = 0;
                                                                        else sendData.state[jj].val = 1;
                                                                    break;
                                                    case 'string':  if (sendData.state[jj].val === "true") {
                                                                        sendData.state[jj].val = 1;
                                                                    } else if (sendData.state[jj].val === "false") {
                                                                        sendData.state[jj].val = 0;
                                                                    } else {
                                                                        sendData.state[jj].val = parseFloat(sendData.state[jj].val);
                                                                        if (isNaN(sendData.state[jj].val)) {
                                                                            sendData.state[jj].val = null;
                                                                            sortedOut++;
                                                                        }
                                                                    }
                                                                    break;
                                                    default:        sendData.state[jj].val = null; // value will be sorted out!
                                                                    sortedOut++;
                                                }
                                                break;
                            case 'boolean':     switch (currType) {
                                                    case 'number':  if (sendData.state[jj].val === 0) sendData.state[jj].val = false;
                                                                        else sendData.state[jj].val = true;
                                                                    break;
                                                    case 'string':  if (sendData.state[jj].val === "true") {
                                                                        sendData.state[jj].val = true;
                                                                    } else if (sendData.state[jj].val === "false") {
                                                                        sendData.state[jj].val = false;
                                                                    } else {
                                                                        sendData.state[jj].val = parseInt(sendData.state[jj].val);
                                                                        if (sendData.state[jj].val === 0) sendData.state[jj].val = false;
                                                                            else sendData.state[jj].val = true;
                                                                    }
                                                                    break;
                                                    default:        sendData.state[jj].val = null; // value will be sorted out!
                                                                    sortedOut++;
                                                }
                                                break;
                        }
                        console.log('  type mismatch ' + existingTypes[id] + ' vs. ' + currType + ': fixed=' + (sendData.state[jj].val !== null) + ' --> ' + sendData.state[jj].val);
                    }
                }
                console.log('  sorted out ' + sortedOut + ' values');
                if (sortedOut === sendData.state.length) {
                    sendData.state = [];
                }
            }
            if (sendData.state.length > 0) {
                processCounter++;
                if (processNonExistingValues) {
                    if (!existingDBValues[id]) existingDBValues[id] = [];
                    existingDBValues[id].push(day);
                }
                if (!simulate) {
                    adapter.sendTo(dbInstance, "storeState", sendData, function (result) {
                        if (result.error) {
                            console.error(result.error);
                            finish(false);
                        }
                        if (result.success && !result.connected) {
                            console.error('Data stored but db not available anymore, break. ' + JSON.stringify(result));
                            finish(true);
                        }
                        var delay = 300;
                        if (result.success && result.seriesBufferFlushPlanned) {
                            delay = 1500; // 1,5 seconds
                            if (result.seriesBufferCounter > 1000) delay += 500*(result.seriesBufferCounter/1000);
                        }
                        delay = delay * delayMultiplicator;
                        setTimeout(processFile, delay);
                    });
                } else {
                    console.log('  SIMULATE: Not really writing ... ' + sendData.state.length + ' values for ' + id);
                    setTimeout(processFile, 10);
                }
            }
            else {
                setTimeout(processFile,10);
            }
        }
        else {
            setTimeout(processFile,10);
        }
    }
    else {
        delete allFiles[day];
        if (!ignoreEarliesDBValues && !simulate) fs.writeFileSync(earliesValCachefile, JSON.stringify(earliestDBValue));
        if (processNonExistingValues && !simulate) fs.writeFileSync(existingDataCachefile, JSON.stringify(existingDBValues));
        if (existingTypesCachefileExists && !simulate) fs.writeFileSync(existingTypesCachefile, JSON.stringify(existingTypes));
        console.log("Day end");
        var dayDelay = 30000;
        if (processCounter < 10) dayDelay = 1000;
        if (processCounter === 0) dayDelay = 10;
        processCounter = 0;
        setTimeout(processFile, dayDelay);
    }
}


function finish(updateData) {
    console.log('DONE');
    if (updateData && !ignoreEarliesDBValues && !simulate) fs.writeFileSync(earliesValCachefile, JSON.stringify(earliestDBValue));
    if (updateData && processNonExistingValues && !simulate) fs.writeFileSync(existingDataCachefile, JSON.stringify(existingDBValues));
    if (updateData && existingTypesCachefileExists && !simulate) fs.writeFileSync(existingTypesCachefile, JSON.stringify(existingTypes));
    process.exit();
}


function getDirectories(path) {
    try {
        return fs.readdirSync(path).filter(function (file) {
            return fs.statSync(path + '/' + file).isDirectory();
        });
    }
    catch (e) {
        return [];
    }
}

function getFiles(path) {
    try {
        return fs.readdirSync(path).filter(function (file) {
            return fs.statSync(path + '/' + file).isFile();
        });
    }
    catch (e) {
        return [];
    }
}

function ts2day(ts) {
    if (ts < 946681200000) ts *= 1000;
    var dateObj = new Date(ts);

    var text = dateObj.getFullYear().toString();
    var v = dateObj.getMonth() + 1;
    if (v < 10) text += '0';
    text += v.toString();

    v = dateObj.getDate();
    if (v < 10) text += '0';
    text += v.toString();

    return text;
}

process.on('SIGINT', function () {
    breakIt = true;
});

process.on('uncaughtException', function (err) {
    breakIt = true;
});
