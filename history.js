/* jshint -W097 */// jshint strict:false
/*jslint node: true */
"use strict";

var utils   = require(__dirname + '/lib/utils'); // Get common adapter utils
var path    = require('path');
var dataDir = path.normalize(utils.controllerDir + '/' + require(utils.controllerDir + '/lib/tools').getDefaultDataDir());
var fs      = require('fs');

var history = {};

var adapter = utils.adapter({

    name: 'history',

    objectChange: function (id, obj) {
        if (obj && obj.common && obj.common.history && obj.common.history[adapter.namespace]) {
            history[id] = obj.common.history;
            adapter.log.info('enabled logging of ' + id);
        } else {
            if (history[id]) {
                adapter.log.info('disabled logging of ' + id);
                delete history[id];
            }
        }
    },

    stateChange: function (id, state) {
        pushHistory(id, state);
    },

    unload: function (callback) {
        finish(callback);
    },

    ready: function () {
        main();
    },

    message: function (obj) {
        processMessage(obj);
    }
});

process.on('SIGINT', function () {
    if (adapter && adapter.setState) {
        finish();
    }
});

function finish(callback) {
    for (var id in history) {
        if (history[id][adapter.namespace].list && history[id][adapter.namespace].list.length) {
            adapter.log.debug('Store th rest for ' + id);
            appendFile(id, history[id][adapter.namespace].list);
        }
    }
    if (callback) callback();
}

function processMessage(msg) {
    if (msg.command == 'getHistory') {
        getHistory(msg);
    }
}

function main() {
    adapter.config.storeDir = adapter.config.storeDir || 'history';
    adapter.config.storeDir = adapter.config.storeDir.replace(/\\/g, '/');
    // remove last "/"
    if (adapter.config.storeDir[adapter.config.storeDir.length - 1] == '/') {
        adapter.config.storeDir = adapter.config.storeDir.substring(0, adapter.config.storeDir.length - 1);
    }

    if (adapter.config.storeDir[0] !== '/' && !adapter.config.storeDir.match(/^\w:\//)) {
        adapter.config.storeDir = dataDir + adapter.config.storeDir;
    }
    adapter.config.storeDir += '/';

    adapter.objects.getObjectView('history', 'state', {}, function (err, doc) {
        if (doc && doc.rows) {
            for (var i = 0, l = doc.rows.length; i < l; i++) {
                if (doc.rows[i].value) {
                    var id = doc.rows[i].id;
                    history[id] = doc.rows[i].value;
                    // convert old value
                    if (history[id].enabled !== undefined) {
                        history[id] = history[id].enabled ? {'history.0': history[id]} : null;
                        if (!history[id]) {
                            delete history[id];
                            continue;
                        }
                    }
                    if (!history[id][adapter.namespace] || history[id][adapter.namespace].enabled === false) {
                        delete history[id];
                    } else {
                        adapter.log.info('enabled logging of ' + id);
                        history[id][adapter.namespace].maxLength   = parseInt(history[id][adapter.namespace].maxLength || adapter.config.maxLength, 10) || 960;
                        history[id][adapter.namespace].retention   = parseInt(history[id][adapter.namespace].retention || adapter.config.retention, 10) || 0;
                        history[id][adapter.namespace].debounce    = parseInt(history[id][adapter.namespace].debounce  || adapter.config.debounce,  10) || 1000;
                        history[id][adapter.namespace].changesOnly = history[id][adapter.namespace].changesOnly === 'true' || history[id][adapter.namespace].changesOnly === true;

                        // add one day if retention is too small
                        if (history[id][adapter.namespace].retention <= 604800) {
                            history[id][adapter.namespace].retention += 86400;
                        }
                    }
                }
            }
        }
    });

    adapter.subscribeForeignStates('*');
    adapter.subscribeForeignObjects('*');
}

function pushHistory(id, state) {
    // Push into redis
    if (history[id]) {
        var settings = history[id][adapter.namespace];

        if (!settings || !state) return;
        
        if (history[id].state && settings.changesOnly && (state.ts !== state.lc)) return;

        history[id].state = state;

        // Do not store values ofter than 1 second
        if (!history[id].timeout) {

            history[id].timeout = setTimeout(function (_id) {
                if (!history[_id]) return;
                var _settings = history[_id][adapter.namespace];
                // if it was not deleted in this time
                if (_settings) {
                    history[_id].timeout = null;
                    history[_id].list = history[_id].list || [];
                    if (typeof history[_id].state.val === 'string') {
                        var f = parseFloat(history[_id].state.val);
                        if (f.toString() == history[_id].state.val) {
                            history[_id].state.val = f;
                        } else if (history[_id].state.val === 'true') {
                            history[_id].state.val = true;
                        } else if (history[_id].state.val === 'false') {
                            history[_id].state.val = false;
                        }
                    }
                    history[_id].list.push(history[_id].state);

                    if (history[id].list.length > _settings.maxLength) {
                        adapter.log.info('moving ' + history[id].list.length + ' entries to file');
                        appendFile(_id, history[_id].list);
                        checkRetention(_id);
                    }
                }
            }, settings.debounce, id);
        }
    }
}

function checkRetention(id) {
    if (history[id][adapter.namespace].retention) {
        var d = new Date();
        var dt = d.getTime();
        // check every 6 hours
        if (!history[id].lastCheck || dt - history[id].lastCheck >= 21600000/* 6 hours */) {
            history[id].lastCheck = dt;
            // get list of directories
            var dayList = getDirectories(adapter.config.storeDir).sort(function (a, b) {
                return a - b;
            });
            // calculate date
            d.setSeconds(-(history[id][adapter.namespace].retention));
            var day = ts2day(Math.round(d.getTime() / 1000));
            for (var i = 0; i < dayList.length; i++) {
                if (dayList[i] < day) {
                    var file = adapter.config.storeDir + dayList[i] + '/history.' + id + '.json';
                    if (fs.existsSync(file)) {
                        adapter.log.info('Delete old history "' + file + '"');
                        try {
                            fs.unlinkSync(file);
                        } catch(ex) {
                            adapter.log.error('Cannot delete file "' + file + '": ' + ex);
                        }
                        var files = fs.readdirSync(adapter.config.storeDir + dayList[i]);
                        if (!files.length) {
                            adapter.log.info('Delete old history dir "' + adapter.config.storeDir + dayList[i] + '"');
                            try {
                                fs.unlink(adapter.config.storeDir + dayList[i]);
                            } catch(ex) {
                                adapter.log.error('Cannot delete directory "' + adapter.config.storeDir + dayList[i] + '": ' + ex);
                            }
                        }
                    }
                } else {
                    break;
                }
            }
        }
    }
}

function appendFile(id, states) {
    var day = ts2day(states[states.length - 1].ts);

    var file = adapter.config.storeDir + day + '/history.' + id + '.json';
    var data;

    var i;
    for (i = states.length - 1; i >= 0; i--) {
        if (!states[i]) continue;
        if (ts2day(states[i].ts) !== day) {
            break
        }
    }
    data = states.splice(i - states.length + 1);

    if (fs.existsSync(file)) {
        try {
            data = JSON.parse(fs.readFileSync(file)).concat(data);
        } catch (err) {
            adapter.log.error('Cannot read file ' + file + ': ' + err);
        }
    }

    try {
        // create directory
        if (!fs.existsSync(adapter.config.storeDir + day)) {
            fs.mkdirSync(adapter.config.storeDir + day);
        }
        fs.writeFileSync(file, JSON.stringify(data, null, 2));
    } catch (ex) {
        adapter.log.error('Cannot store file ' + file + ': ' + ex);
    }

    if (states.length) {
        appendFile(id, states);
    }
}

function getCachedData(id, options, callback) {
    var res = [];
    var cache = [];
    if(history[id]){
         res = history[id].list;
         cache = [];
        // todo can be optimized
        if (res) {
            var iProblemCount = 0;
            for (var i = res.length - 1; i >= 0 ; i--) {
                if (!res[i]) {
                    iProblemCount++;
                    continue;
                }
                if (options.start && res[i].ts < options.start) {
                    break;
                } else if (res[i].ts > options.end) {
                    continue;
                }
                cache.unshift(res[i]);

                if (!options.start && cache.length >= options.count) {
                    break;
                }
            }
            if (iProblemCount) adapter.log.warn('got null states ' + iProblemCount + ' times for ' + id);

            adapter.log.debug('got ' + res.length + ' datapoints for ' + id);
        } else {
            //if (err != 'Not exists') {
            //    adapter.log.error(err);
            //} else {
            adapter.log.debug('datapoints for ' + id + ' do not yet exist');
            //}
        }
    }

    options.length = cache.length;
    callback(cache, !options.start && cache.length >= options.count);
}

function getFileData(id, options, callback) {

    var day_start = options.start ? ts2day(options.start) : null;
    var day_end = ts2day(options.end);
    var data = [];

    // get list of directories
    var dayList = getDirectories(adapter.config.storeDir).sort(function (a, b) {
        return a - b;
    });

    // get all files in directory
    for (var i = dayList.length - 1; i >= 0; i--) {
        var day = parseInt(dayList[i]);

        if (day_start && day < day_start) {
            break;
        } else
        if ((!day_start || day >= day_start) && day <= day_end) {
            var file = adapter.config.storeDir + dayList[i].toString() + '/history.' + id + '.json';
            if (fs.existsSync(file)) {
                try {
                    data = data.concat(JSON.parse(fs.readFileSync(file)));
                } catch (e) {
                    adapter.log.error('Cannot parse file ' + file + ': ' + e.message);
                }
                // if we need "count" entries
                if (!day_start && (options.length + data.length > options.count)) {
                    break;
                }
            }
        }
    }

    callback(data);
}

function aggregate(data, options) {
    if (data && data.length) {
        if (typeof data[0].val !== 'number') {
            return {result: data, step: 0, sourceLength: data.length};
        }
        var start = new Date(options.start * 1000);
        var end   = new Date(options.end * 1000);

        var step = 1; // 1 Step is 1 second
        if (options.step) {
            step = options.step;
        } else{
            step = Math.round((options.end - options.start) / options.count) ;
        }

        // Limit 2000
        if ((options.end - options.start) / step > options.limit){
            step = Math.round((options.end - options.start)/ options.limit);
        }

        var stepEnd;
        var i = 0;
        var result = [];
        var iStep = 0;
        options.aggregate = options.aggregate || 'max';
        
        while (i < data.length && new Date(data[i].ts * 1000) < end) {
            stepEnd = new Date(start);
            var x = stepEnd.getSeconds();
            stepEnd.setSeconds(x + step);

            if (stepEnd < start) {
                // Summer time
                stepEnd.setHours(start.getHours() + 2);
            }

            // find all entries in this time period
            var value = null;
            var count = 0;

            while (i < data.length && new Date(data[i].ts * 1000) < stepEnd) {
                if (options.aggregate == 'max') {
                    // Find max
                    if (value === null || data[i].val > value) value = data[i].val;
                } else if (options.aggregate == 'min') {
                    // Find min
                    if (value === null || data[i].val < value) value = data[i].val;
                } else if (options.aggregate == 'average') {
                    if (value === null) value = 0;
                    value += data[i].val;
                    count++;
                } else if (options.aggregate == 'total') {
                    // Find sum
                    if (value === null) value = 0;
                    value += parseFloat(data[i].val);
                }
                i++;
            }

            if (options.aggregate == 'average') {
                if (!count) {
                    value = null;
                } else {
                    value /= count;
                    value = Math.round(value * 100) / 100;
                }
            }
            if (value !== null || !options.ignoreNull) {
                result[iStep] = {ts: stepEnd.getTime() / 1000};
                result[iStep].val = value;
                iStep++;
            }

            start = stepEnd;
        }

        return {result: result, step: step, sourceLength: data.length};
    } else {
        return {result: [], step: 0, sourceLength: 0};
    }
}

function sortByTs(a, b) {
    var aTs = a.ts;
    var bTs = b.ts;
    return ((aTs < bTs) ? -1 : ((aTs > bTs) ? 1 : 0));
}

function sendResponse(msg, options, data, startTime) {
    var aggregateData;
    data = data.sort(sortByTs);
    if (options.count && !options.start && data.length > options.count) {
        data.splice(0, data.length - options.count);
    }
    if (data[0]) {
        options.start = options.start || data[0].ts;

        if (!options.aggregate || options.aggregate === 'none') {
            aggregateData = {result: data, step: 0, sourceLength: data.length};
        } else {
            aggregateData = aggregate(data, options);
        }

        adapter.log.info('Send: ' + aggregateData.result.length + ' of: ' + aggregateData.sourceLength + ' in: ' + (new Date().getTime() - startTime) + 'ms');
        adapter.sendTo(msg.from, msg.command, {
            result: aggregateData.result,
            step: aggregateData.step,
            error: null
        }, msg.callback);
    } else {
        adapter.log.info('No Data');
        adapter.sendTo(msg.from, msg.command, {result: [].result, step: null, error: null}, msg.callback);
    }


}

function getHistory(msg) {
    var startTime = new Date().getTime();
    var id = msg.message.id;
    var options = {
        start:      msg.message.options.start,
        end:        msg.message.options.end || Math.round((new Date()).getTime() / 1000) + 5000,
        step:       parseInt(msg.message.options.step) || null,
        count:      parseInt(msg.message.options.count) || 500,
        ignoreNull: msg.message.options.ignoreNull,
        aggregate:  msg.message.options.aggregate || 'average', // One of: max, min, average, total
        limit:      msg.message.options.limit || adapter.config.limit || 2000
    };

    if (options.start > options.end){
        var _end = options.end;
        options.end   = options.start;
        options.start =_end;
    }

    if (!options.start && !options.count) {
        options.start = Math.round((new Date()).getTime() / 1000) - 5030; // - 1 year
    }

    getCachedData(id, options, function (cacheData, isFull) {
        // if all data read
        if (isFull && cacheData.length) {
            sendResponse(msg, options, cacheData, startTime);
        } else {
            getFileData(id, options, function (fileData) {
                sendResponse(msg, options, cacheData.concat(fileData), startTime);
            });
        }
    });
}

function getDirectories(path) {
    return fs.readdirSync(path).filter(function (file) {
        return fs.statSync(path + '/' + file).isDirectory();
    });
}

function ts2day(ts) {
    var dateObj = new Date(ts * 1000);

    var text = dateObj.getFullYear().toString();
    var v = dateObj.getMonth() + 1;
    if (v < 10) text += '0';
    text += v.toString();

    v = dateObj.getDate();
    if (v < 10) text += '0';
    text += v.toString();

    return text;
}

