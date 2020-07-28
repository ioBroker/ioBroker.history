/* jshint -W097 */// jshint strict:false
/*jslint node: true */
"use strict";

//noinspection JSUnresolvedFunction

//usage: nodejs analyzeinflux.js <InfluxDB-Instance>  [<Loglevel>]
//usage: nodejs analyzeinflux.js influxdb.0 info
const utils   = require('@iobroker/adapter-core'); // Get common adapter utils

var fs        = require('fs');
var path      = require('path');

var deepAnalyze = false;
var influxInstance = "influxdb.0";
if (process.argv.indexOf('--deepAnalyze') !== -1) deepAnalyze = true;
if (process.argv[2] && (process.argv[2].indexOf('influxdb') === 0)) {
    influxInstance = process.argv[2];
}
process.argv[2] = "--install";
console.log('Query Data from ' + influxInstance);
if (deepAnalyze) console.log('Do deep analysis to find holes in data');

var earliestDBValue = {};
var earliesValCachefile = __dirname + '/earliestDBValues.json';
var existingData = {};
var existingDataCachefile = __dirname + '/existingDBValues.json';
var existingTypes = {};
var existingTypesCachefile = __dirname + '/existingDBTypes.json';

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
    var inited = false;
    var counter = 0;
    adapter.sendTo(influxInstance, "query", "SHOW MEASUREMENTS", function(result) {
        if (result.error) {
            console.error(result.error);
        } else {
            // show result
            console.log('Datapoints found: ' + result.result[0].length);
            var dp_list = result.result[0];

            function analyze() {
                if (breakIt) process.exit();
                if (dp_list.length>0) {
                    counter++;
                    if (counter%100 === 0) {
                        setTimeout(analyze,5000);
                    }
                    else {
                        var dp = dp_list.shift();
                        var query = "SELECT FIRST(ack) AS val FROM \"" + dp.name + "\"";
                        if (deepAnalyze) {
                            query += ";SELECT count(ack) AS val FROM \"" + dp.name + "\" where time<now() group by time(1d)";
                            query += ";SELECT LAST(value) as val FROM \"" + dp.name + "\"";
                        }
                        adapter.sendTo(influxInstance, "query", query, function(resultDP) {
                            if (resultDP.error) {
                                console.error(resultDP.error);
                            } else {
                                if (resultDP.result[0]) {
                                    earliestDBValue[dp.name] = resultDP.result[0][0].ts;
                                    if (earliestDBValue[dp.name] < 946681200000) earliestDBValue[dp.name] = Date.now(); // mysterious timestamp, ignore
                                    console.log('FirstVal ID: ' + dp.name + ', Rows: ' + JSON.stringify(resultDP.result[0]) + ' --> ' + new Date(earliestDBValue[dp.name]).toString());
                                }
                                if ((deepAnalyze) && (resultDP.result[1])) {
                                    existingData[dp.name]=[];
                                    for (var j = 0;j < resultDP.result[1].length; j++) {
                                        if (resultDP.result[1][j].val > 0) {
                                            var ts = new Date(resultDP.result[1][j].ts);
                                            existingData[dp.name].push(parseInt(ts2day(ts), 10));
                                        }
                                    }
                                    console.log('DayVals ID: '+dp.name+': '+JSON.stringify(existingData[dp.name]));
                                }
                                if ((deepAnalyze) && (resultDP.result[2]) && (resultDP.result[2][0])) {
                                    existingTypes[dp.name]=typeof resultDP.result[2][0].val;
                                    console.log('ValType ID: '+dp.name+': '+JSON.stringify(existingTypes[dp.name]));
                                }
                            }
                            setTimeout(analyze,500);
                        });
                    }
                }
                else {
                    if (deepAnalyze) {
                        fs.writeFileSync(existingDataCachefile, JSON.stringify(existingData));
                        fs.writeFileSync(existingTypesCachefile, JSON.stringify(existingTypes));
                    }
                    fs.writeFileSync(earliesValCachefile, JSON.stringify(earliestDBValue));
                    process.exit();
                }
            }

            analyze();
        }
    });
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
    console.log(err);
    breakIt = true;
});
