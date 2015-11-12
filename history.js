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
    // remove last /
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
                    history[doc.rows[i].id] = doc.rows[i].value;
                    // convert old value
                    if (history[doc.rows[i].id].enabled !== undefined) {
                        history[doc.rows[i].id] = history[doc.rows[i].id].enabled ? {'history.0': history[doc.rows[i].id]} : {};
                    }
                    if (!history[doc.rows[i].id][adapter.namespace] || history[doc.rows[i].id][adapter.namespace].enabled === false) {
                        delete history[doc.rows[i].id];
                    } else {
                        adapter.log.info('enabled logging of ' + doc.rows[i].id);
                        history[doc.rows[i].id][adapter.namespace].maxLength = parseInt(history[doc.rows[i].id][adapter.namespace].maxLength || adapter.config.maxLength, 10);
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
                    history[_id].list.push(history[_id].state);

                    if (history[id].list.length > _settings.maxLength) {
                        adapter.log.info('moving ' + history[id].list.length + ' entries to file');
                        appendFile(_id, history[_id].list);
                    }
                }
            }, settings.debounce || 1000, id);
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

    /*adapter.getForeignObject(cid, function (err, res) {
        var obj;
        if (err || !res) {
            obj = {
                type: 'history',
                common: {
                    source: id,
                    day: day,
                    data: []
                },
                native: {}
            };
        } else {
            obj = res;
        }

        if (!obj.common) {
            adapter.log.error('invalid object ' + id);
            return;
        }

        if (!obj.common.data) obj.common.data = [];

        for (var i = states.length - 1; i >= 0; i--) {
            if (!states[i]) continue;
            if (ts2day(states[i].ts) === day) {
                obj.common.data.unshift(states[i]);
            } else {
                break;
            }
        }

        adapter.setForeignObject(cid, obj, function () {
            adapter.log.info('store ' + states.length + ' history datapoints from RAM history.' + id + ' to file ' + cid);
        });

        if (i >= 0) {
            adapter.log.info((i + 1) + ' remaining datapoints of history.' + id);
            appendFile(id, states.slice(0, (i + 1)));
        }
    });*/
}

function getCachedData(id, options, callback) {
    var res = history[id].list;
    var cache = [];
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
        if (err != 'Not exists') {
            adapter.log.error(err);
        } else {
            adapter.log.debug('datapoints for ' + id + ' do not yet exist');
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
    var day_list = getDirectories(adapter.config.storeDir).sort(function (a, b) {
        return a - b;
    });

    // get all files in directory
    for (var i = day_list.length - 1; i >= 0; i--) {
        var day = parseInt(day_list[i]);

        if (day_start && day < day_start) {
            break;
        } else
        if ((!day_start || day >= day_start) && day <= day_end) {
            var file = adapter.config.storeDir + day_list[i].toString() + '/history.' + id + '.json';
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
            if (value || options.getNull ) {
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

    options.start = options.start || data[0].ts;

    if (!options.aggregate || options.aggregate === 'none') {
        aggregateData = {result: data, step: 0, sourceLength: data.length};
    } else {
        aggregateData = aggregate(data, options);
    }

    adapter.log.info('Send: ' + aggregateData.result.length + ' of: ' + aggregateData.sourceLength + ' in: ' + (new Date().getTime() - startTime) + 'ms');
    adapter.sendTo(msg.from, msg.command, {result: aggregateData.result, step: aggregateData.step, error: null}, msg.callback);

}

function getHistory(msg) {
    var startTime = new Date().getTime();
    var id = msg.message.id;
    var options = {
        start:      msg.message.options.start,
        end:        msg.message.options.end || Math.round((new Date()).getTime() / 1000) + 5000,
        step:       parseInt(msg.message.options.step) || null,
        count:      parseInt(msg.message.options.count) || 500,
        getNull:    msg.message.options.getNull,
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

