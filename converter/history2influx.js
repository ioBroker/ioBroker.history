/* jshint -W097 */// jshint strict:false
/*jslint node: true */
"use strict";

//noinspection JSUnresolvedFunction

//usage: nodejs history2influx.js [<InfluxDB-Instance>] [<Loglevel>] [<DatePath-to-end>] [</path-to-Data>]
//usage: nodejs history2influx.js influxdb.0 info 20161001 /path/to/data
var utils  = require(__dirname + '/../lib/utils'); // Get common adapter utils

var fs        = require('fs');
var path      = require('path');
var dataDir   = path.normalize(utils.controllerDir + '/' + require(utils.controllerDir + '/lib/tools').getDefaultDataDir());
var historydir = dataDir + 'history-data';

var earliestDBValue = {};
var earliesValCachefile = __dirname + '/earliestDBValues.json'

var influxInstance = "influxdb.0";
var endDay = 0; // 20160917; // or 0
if (process.argv[2] && (process.argv[2].indexOf('influxdb') === 0)) {
    influxInstance = process.argv[2];
    if (process.argv[4]) endDay = parseInt(process.argv[4], 10);
    if (process.argv[5]) historydir = process.argv[5];
    process.argv[2] = "--install";
}
console.log('Send Data to ' + influxInstance);
if (endDay !== 0) console.log('Start at ' + endDay);
console.log('Use historyDir ' + historydir);
var adapter = utils.adapter('history');

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
    var inited = false;
    try {
        if (fs.statSync(earliesValCachefile).isFile()) {
            var fileContent = fs.readFileSync(earliesValCachefile);
            earliestDBValue = JSON.parse(fileContent);
            console.log('EarliesDBValues initialized from cache ' + Object.keys(earliestDBValue).length);
            inited= true;
        }
    }
    catch (err) {
        console.log('No stored earliesDBValues found');
        var counter = 0;
        adapter.sendTo(influxInstance, "query", "SHOW MEASUREMENTS", function(result) {
            if (result.error) {
                console.error(result.error);
            } else {
                // show result
                console.log('Rows: ' + result.result[0].length);
                var dp_list = result.result[0];

                function getEarliestDBTime() {
                    if (breakIt) process.exit();
                    if (dp_list.length>0) {
                        counter++;
                        if (counter%100 === 0) {
                            setTimeout(getEarliestDBTime,5000);
                        }
                        else {
                            var dp = dp_list.shift();
                            adapter.sendTo(influxInstance, "query", "SELECT FIRST(value) AS val FROM \"" + dp.name + "\"", function(resultDP) {
                                if (resultDP.error) {
                                    console.error(resultDP.error);
                                } else {
                                    earliestDBValue[dp.name] = resultDP.result[0][0].ts;
                                    if (earliestDBValue[dp.name] < 946681200000) earliestDBValue[dp.name] = 0; // mysterious timestamp, ignore
                                    console.log('ID: ' + dp.name + ', Rows: ' + JSON.stringify(resultDP.result[0]) + ' --> ' + earliestDBValue[dp.name]);
                                }
                                getEarliestDBTime();
                            });
                        }
                    }
                    else {
                        fs.writeFileSync(earliesValCachefile, JSON.stringify(earliestDBValue));
                        processFiles();
                    }
                }

                getEarliestDBTime();
            }
        });
    }
    if (inited) processFiles();
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
        console.log('Day ' + day + ' - ' + file);

        if ((!earliestDBValue[id]) || (earliestDBValue[id]==0)) {
            console.log('    Ignore ID ' + file +': ' + id);
            setTimeout(processFile,0);
        }

        try {
            var fileContent = fs.readFileSync(dir + '/' + file);

            var fileData = JSON.parse(fileContent, function (key, value) {
                if (key === 'time') {
                    if (value < 946681200000) value *= 1000;
                }
                else if (key === 'ack') {
                    value = !!value;
                }
                return value;
            });
        } catch (e) {
            console.log('Cannot parse file ' + dir + '/' + file + ': ' + e.message);
        }

        //console.log('    File ' + j +': ' + id + ' --> ' + fileData.length);

        if (fileData[fileData.length-1].ts >= earliestDBValue[id]) {
            var k;
            for (k = 0; k < fileData.length; k++) {
                if (fileData[k].ts >= earliestDBValue[id]) break;
            }
            fileData = fileData.slice(0,k);
            console.log('cut filedata to ' + fileData.length);
        }
        if (fileData.length > 0) {
            var sendData = {};
            sendData.id = id;
            sendData.state = fileData;
            for (var j = 0; j< sendData.state.length; j++) {
                if (sendData.state[j].ts < earliestDBValue[id]) earliestDBValue[id] = sendData.state[j].ts;
            }
            adapter.sendTo(influxInstance, "storeState", sendData, function (result) {
                if (result.error) {
                    console.error(result.error);
                    finish(false);
                }
                setTimeout(processFile,200);
            });

        }
        else {
            setTimeout(processFile,0);
        }
    }
    else {
        delete allFiles[day];
        setTimeout(processFile,0);
    }
}


function finish(updateData) {
    console.log('DONE');
    if (updateData) fs.writeFileSync(earliesValCachefile, JSON.stringify(earliestDBValue));
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
