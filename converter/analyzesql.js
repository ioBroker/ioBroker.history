/* jshint -W097 */
/* jshint strict: false */
/* jslint node: true */
"use strict";

//noinspection JSUnresolvedFunction

//usage: nodejs analyzesql.js <SQL-Instance>  [<Loglevel>]
//usage: nodejs analyzesql.js sql.0 info
const utils   = require('@iobroker/adapter-core'); // Get common adapter utils

var fs        = require('fs');

var deepAnalyze = false;
var dbInstance = "sql.0";
if (process.argv.indexOf('--deepAnalyze') !== -1) deepAnalyze = true;
if (process.argv[2] && (process.argv[2].indexOf('sql') === 0)) {
    dbInstance = process.argv[2];
}
process.argv[2] = "--install";
console.log('Query Data from ' + dbInstance);
if (deepAnalyze) {
    console.log('Deep Analyze not supported');
    process.exit();
}

var earliestDBValue = {};
var earliesValCachefile = __dirname + '/earliestDBValues.json';
//var existingData = {};
//var existingDataCachefile = __dirname + '/existingDBValues.json';
var existingTypes = {};
var existingTypesCachefile = __dirname + '/existingDBTypes.json';

var adapter = utils.Adapter('history');

adapter.on('ready', function () {
    main();
});

function main() {
    console.log('Send');
    adapter.sendTo(dbInstance, "getDpOverview", "", function(result) {
        console.log(JSON.stringify(result));
        if (result.error) {
            console.error(result.error);
        } else {
            // show result
            console.log('Datapoints found: ' + result.result.length);
            console.log(JSON.stringify(result.result));
            for (var id in result.result) {
                earliestDBValue[id] = result.result[id].ts;
                if (earliestDBValue[id] < 946681200000) earliestDBValue[id] = Date.now(); // mysterious timestamp, ignore
                if (result.result[id].type !== 'undefined') existingTypes[id] = result.result[id].type;
            }
            fs.writeFileSync(existingTypesCachefile, JSON.stringify(existingTypes));
            fs.writeFileSync(earliesValCachefile, JSON.stringify(earliestDBValue));
        }
        process.exit();
    });
}

process.on('SIGINT', function () {
    process.exit();
});

process.on('uncaughtException', function (err) {
    console.log(err);
    process.exit();
});
