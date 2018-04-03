'use strict';
// todo     add cache data
// todo     error tests
// todo     clean up

var fs = require('fs');
var Aggregate = require(__dirname + '/aggregate.js');

var gOptions;

if (typeof module === 'undefined' || !module || !module.parent) {
    gOptions = JSON.parse(process.argv[2]);
}

var finished = false;

if (typeof module === 'undefined' || !module || !module.parent) {
    process.on('message', function (msg){
        if (msg[0] == 'cacheData'){
            if (msg[1]) Aggregate.aggregation(gOptions, msg[1]);

            if (finished) {
                response(gOptions);
            } else {
                finished = true;
            }
        }
    });
}

function getDirectories(path) {
    return fs.readdirSync(path).filter(function (file) {
        return fs.statSync(path + '/' + file).isDirectory();
    });
}

function tsSort(a, b) {
    return b.ts - a.ts;
}

function getFilenameForID(path, date, id) {
    const safeId = id.replace(/[\u0000:\/\\]/g, "~");
    return `${path}${date.toString()}/history.${safeId}.json`;
}

function getFileData(options) {
    var dayStart = parseInt(ts2day(options.start), 10);
    var dayEnd   = parseInt(ts2day(options.end), 10);
    var fileData = [];

    // get list of directories
    var dayList = getDirectories(options.path).sort(function (a, b) {
        return a - b;
    });

    // get all files in directory
    for (var i = 0; i < dayList.length; i++) {
        var day = parseInt(dayList[i], 10);

        if (!isNaN(day) && day > 20100101 && day >= dayStart && day <= dayEnd) {
            const file = getFilenameForID(options.path, dayList[i], options.id);

            if (fs.existsSync(file)) {
                try {
                    fileData = JSON.parse(fs.readFileSync(file)).sort(tsSort);
                } catch (e) {
                    fileData = null;
                }
                if (fileData) Aggregate.aggregation(options, fileData);
            }
        }
        if (day > dayEnd) break;
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

function response(options){
    Aggregate.finishAggregation(options);
    if (typeof module === 'undefined' || !module || !module.parent) {
        if (process.send) process.send(['response', options.result, options.overallLength, options.step]);
        setTimeout(function () {
            process.exit();
        }, 500);
    } else {
        return ['response', options.result, options.overallLength, options.step];
    }
}

if (typeof module === 'undefined' || !module || !module.parent) {
    Aggregate.initAggregate(gOptions);
    if (process.send) {
        process.send(['getCache', gOptions]);
    } else {
        finished = true;
    }
    getFileData(gOptions);

    if (finished) {
        response(gOptions);
    } else {
        finished = true;
    }
}


if (typeof module !== 'undefined' && module.parent) {
    module.exports.initAggregate     = Aggregate.initAggregate;
    module.exports.aggregation       = Aggregate.aggregation;
    module.exports.finishAggregation = Aggregate.finishAggregation;
    module.exports.getFileData       = getFileData;
    module.exports.ts2day            = ts2day;
    module.exports.response          = response;
    module.exports.getFilenameForID  = getFilenameForID;

    // how to use:
    // var options = initAggregate(options);
    // getFileData(options);
    // var result = response(options);
}
