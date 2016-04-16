// todo     add cache data
// todo     error tests
// todo     clean up

var fs = require('fs');

var gOptions;
var gSettings;

if (typeof module === 'undefined' || !module || !module.parent) {
    gOptions = JSON.parse(process.argv[2]);
}

var finished = false;

if (typeof module === 'undefined' || !module || !module.parent) {
    process.on('message', function (msg){
        if (msg[0] == 'cacheData'){
            if (msg[1]) aggregation(msg[1], gSettings);

            if (finished) {
                response(gSettings);
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

function getFileData(settings) {

    var dayStart = ts2day(settings.start);
    var dayEnd   = ts2day(settings.end);
    var dayList  = getDirectories(settings.path);
    var fileData;

    for (var i in dayList) {
        var day = parseInt(dayList[i], 10);

        if (!isNaN(day) && day > 20100101 && day >= dayStart && day <= dayEnd) {
            var file = settings.path + dayList[i].toString() + '/history.' + settings.id + '.json';

            if (fs.existsSync(file)) {
                try {
                    fileData = JSON.parse(fs.readFileSync(file));
                } catch (e) {
                    fileData = null;
                }
                if (fileData) aggregation(fileData, settings);
            }
        }
    }
}

function initAggregate(options) {
    var settings = {};

    settings.start = options.start;
    settings.end   = options.end;
    // if less 2000.01.01 00:00:00
    if (settings.start < 946681200000) {
        settings.start *= 1000;

        if (options.step !== null && options.step !== undefined) {
            options.step *= 1000;
        }
    }

    // if less 2000.01.01 00:00:00
    if (settings.end < 946681200000) settings.end *= 1000;

    settings.step = 1; // 1 Step is 1 second
    if (options.step !== null && options.step !== undefined) {
        settings.step = options.step;
    } else {
        settings.step = (settings.end - settings.start) / options.count;
    }


    // Limit 2000
    if ((settings.end - settings.start) / settings.step > options.limit) {
        settings.step = (settings.end - settings.start) / options.limit;
    }

    settings.end   += settings.step;
    settings.start -= settings.step;

    settings.maxIndex      = ((settings.end - settings.start) / settings.step) - 1;
    settings.result        = [];
    settings.averageCount  = [];
    settings.aggregate     = options.aggregate || 'average';
    settings.overallLength = 0;
    settings.value         = 0;
    settings.path          = options.path;
    settings.id            = options.id;
    settings.count         = options.count;

    for (var i = 0; i <= settings.maxIndex; i++) {
        settings.result[i] = {
            ts:  Math.round(settings.start + ((i + 0.5) * settings.step)),
            val: null
        };

        if (settings.aggregate == 'average') settings.averageCount[i] = 0;
    }

    return settings;
}

function aggregation(data, settings) {
    var index;
    for (var i in data) {
        // if less 2000.01.01 00:00:00
       if (data[i].ts < 946681200000) data[i].ts *= 1000;

        index = Math.round((data[i].ts - settings.start) / settings.step);

        if (index > -1 && index <= settings.maxIndex) {
            settings.overallLength++;
            if (settings.aggregate == 'max') {

                if (settings.result[index] == null || settings.result[index].val < data[i].val) settings.result[index].val = data[i].val;

            } else if (settings.aggregate == 'min') {

                if (settings.result[index] == null || settings.result[index].val > data[i].val) settings.result[index].val = data[i].val;

            } else if (settings.aggregate == 'average') {

                if (settings.value === null) settings.value = 0;
                settings.result[index].val += data[i].val;
                settings.averageCount[index]++;

            } else if (settings.aggregate == 'total') {

                if (settings.value === null) settings.value = 0;
                settings.result[index].val += parseFloat(data[i].val);

            }
        }
    }
    return null;
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

function response(settings){
    if (settings.aggregate == 'average') {
        for (var i = 0; i < settings.result.length; i++) {
            settings.result[i].val = (settings.result[i].val !== null) ? Math.round(settings.result[i].val / settings.averageCount[i] * 100) / 100 : null;
        }
    }
    if (typeof module === 'undefined' || !module || !module.parent) {
        if (process.send) process.send(['response', settings.result, settings.overallLength, settings.step]);
        setTimeout(function () {
            process.exit();
        }, 500);
    } else {
        return ['response', settings.result, settings.overallLength, settings.step];
    }
}

if (typeof module === 'undefined' || !module || !module.parent) {
    gSettings = initAggregate(gOptions);
    if (process.send) {
        process.send(['getCache', gSettings]);
    } else {
        finished = true;
    }
    getFileData(gSettings);

    if (finished) {
        response(gSettings);
    } else {
        finished = true;
    }
}


if (typeof module !== 'undefined' && module.parent) {
    module.exports.initAggregate = initAggregate;
    module.exports.getFileData   = getFileData;
    module.exports.ts2day        = ts2day;
    module.exports.aggregation   = aggregation;
    module.exports.response      = response;

    // how to use:
    // var settings = initAggregate(options);
    // getFileData(settings);
    // aggregation(cachedData, settings);
    // var result = response(settings);
}
