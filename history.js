/* jshint -W097 */// jshint strict:false
/*jslint node: true */
"use strict";
var cp      = require('child_process');
var utils   = require(__dirname + '/lib/utils'); // Get common adapter utils
var path    = require('path');
var dataDir = path.normalize(utils.controllerDir + '/' + require(utils.controllerDir + '/lib/tools').getDefaultDataDir());
var fs      = require('fs');

var history = {};
var subscribeAll = false;

var adapter = utils.adapter({

    name: 'history',

    objectChange: function (id, obj) {
        if (obj && obj.common && obj.common.history && obj.common.history[adapter.namespace]) {
            var state   = history[id] ? history[id].state   : null;
            var list    = history[id] ? history[id].list    : null;
            var timeout = history[id] ? history[id].timeout : null;

            if (!history[id] && !subscribeAll) {
                // unsubscribe
                for (var _id in history) {
                    adapter.unsubscribeForeignStates(_id);
                }
                subscribeAll = true;
                adapter.subscribeForeignStates('*');
            }

            history[id] = obj.common.history;
            history[id].state   = state;
            history[id].list    = list;
            history[id].timeout = timeout;

            history[id][adapter.namespace].maxLength   = parseInt(history[id][adapter.namespace].maxLength || adapter.config.maxLength, 10) || 960;
            history[id][adapter.namespace].retention   = parseInt(history[id][adapter.namespace].retention || adapter.config.retention, 10) || 0;
            history[id][adapter.namespace].debounce    = parseInt(history[id][adapter.namespace].debounce  || adapter.config.debounce,  10) || 1000;
            history[id][adapter.namespace].changesOnly = history[id][adapter.namespace].changesOnly === 'true' || history[id][adapter.namespace].changesOnly === true;

            // add one day if retention is too small
            if (history[id][adapter.namespace].retention <= 604800) {
                history[id][adapter.namespace].retention += 86400;
            }

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
    } else if (msg.command == 'generateDemo') {
        generateDemo(msg)
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

    // create directory
    if (!fs.existsSync(adapter.config.storeDir)) {
        fs.mkdirSync(adapter.config.storeDir);
    }

    adapter.objects.getObjectView('history', 'state', {}, function (err, doc) {
        var count = 0;
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
                        count++;
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
        if (count < 20) {
            for (var _id in history) {
                adapter.subscribeForeignStates(_id);
            }
        } else {
            subscribeAll = true;
            adapter.subscribeForeignStates('*');
        }
    });

    adapter.subscribeForeignObjects('*');
}

function generateDemo(msg) {
    var options = [
        msg.message.curve   || 'sin',                         // 0 curve
        msg.message.end     || new Date().toDateString(),     // 1 end
        msg.message.start   || new Date().setDate(-1),        // 2 start
        msg.message.step    || 60,                            // 3 step
        msg.message.id      || 'Demo_Data',                   // 4 id
        adapter.config.storeDir                               // 5 path
    ];

    function fork(options) {

        var newDay;

        var path = options[5];
        var fileName = '/history.' + adapter.namespace + '.' + options[4] + '.json';

        var data    = [];
        var start   = new Date(options[2]).getTime();
        var end     = new Date(options[1]).getTime();
        var value   = 1;
        var sin     = 0.1;
        var up      = true;
        var curve   = options[0];

        var step    = options[3] * 1000;
        var oldDay;

        if (end < start) {
            var tmp = end; end = start; start = tmp;
        }

        end = new Date(end).setHours(24);

        function generate() {
            oldDay = new Date(start).getDay();
            for (start; start <= end;) {

                newDay = new Date(start + step).getDay();
                if (newDay != oldDay) {
                    save();
                    break;
                }

                data.push({
                    'ts': new Date(start).getTime() / 1000,
                    'val': value,
                    'q': 0,
                    'ack': true
                });

                if (curve =='sin') {
                    if (sin == 6.2) {
                        sin = 0
                    } else {
                        sin = Math.round((sin + 0.1) * 10) / 10;
                    }
                    value = Math.round(Math.sin(sin) * 10000) / 100;
                } else if (curve == 'dec') {
                    value++;
                } else if (curve == 'inc') {
                    value--;
                } else {
                    if (up == true) {
                        value++;
                    } else {
                        value--;
                    }
                }
                start += step;
            }
        }

        function save() {
            try {
                if (!fs.existsSync(path + ts2day(start))) {
                    fs.mkdirSync(path + ts2day(start));
                } 
            } catch (err) {
                adapter.log.error(err);
            }

            fs.writeFile(path + ts2day(start) + fileName, JSON.stringify(data), 'utf8', function (err, res) {
                data = [];
                up = !up;

                data.push({
                    'ts':   new Date(start).getTime() / 1000,
                    'val':  value,
                    'q':    0,
                    'ack':  true
                });

                if (curve == 'sin') {
                    if (sin == 6.2) {
                        sin = 0
                    } else {
                        sin = Math.round((sin + 0.1) * 10) / 10;
                    }
                    value = Math.round(Math.sin(sin) * 10000) / 100;
                } else if (curve == 'dec') {
                    value++;
                } else if (curve == 'inc') {
                    value--;
                } else {
                    if (up == true) {
                        value++;
                    } else {
                        value--;
                    }
                }

                start += step;

                if (start < end){
                    generate()
                } else {
                    var history = {};
                    history[adapter.namespace] = {
                        enabled:        false,
                        changesOnly:    true,
                        debounce:       1000,
                        maxLength:      960,
                        retention:      0
                    };

                    adapter.setObject('demo.' + options[4], {
                        type: 'state',
                        common: {
                            name:       options[4],
                            type:       'state',
                            enabled:    false,
                            history:    history
                        }
                    });
                    adapter.sendTo(msg.from, msg.command, 'finish', msg.callback);
                }
            });
        }

        function ts2day(ts) {
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
        
        //var x = new Date().getTime();
        
        generate();
    }
    
    fork(options);
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
                if (!history[_id] || !history[_id].state) return;

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
                    if (history[_id].state.lc !== undefined) delete history[_id].state.lc;
                    if (!adapter.config.storeAck && history[_id].state.ack !== undefined) {
                        delete history[_id].state.ack;
                    } else {
                        history[_id].state.ack = history[_id].state.ack ? 1 : 0;
                    }
                    if (!adapter.config.storeFrom && history[_id].state.from !== undefined) delete history[_id].state.from;

                    history[_id].list.push(history[_id].state);

                    if (history[id].list.length > _settings.maxLength) {
                        adapter.log.info('moving ' + history[id].list.length + ' entries from '+ id +' to file');
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
                        } catch (ex) {
                            adapter.log.error('Cannot delete file "' + file + '": ' + ex);
                        }
                        var files = fs.readdirSync(adapter.config.storeDir + dayList[i]);
                        if (!files.length) {
                            adapter.log.info('Delete old history dir "' + adapter.config.storeDir + dayList[i] + '"');
                            try {
                                fs.unlinkSync(adapter.config.storeDir + dayList[i]);
                            } catch (ex) {
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
            break;
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

function getCachedData(options, callback) {
    var res = [];
    var cache = [];
    if (history[options.id]) {
         res = history[options.id].list;
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
                if (options.ack) res[i].ack = !!res[i];
                cache.unshift(res[i]);

                if (!options.start && cache.length >= options.count) {
                    break;
                }
            }
            if (iProblemCount) adapter.log.warn('got null states ' + iProblemCount + ' times for ' + options.id);

            adapter.log.debug('got ' + res.length + ' datapoints for ' + options.id);
        } else {
            //if (err != 'Not exists') {
            //    adapter.log.error(err);
            //} else {
            adapter.log.debug('datapoints for ' + options.id + ' do not yet exist');
            //}
        }
    }

    options.length = cache.length;
    callback(cache, !options.start && cache.length >= options.count);
}

function getFileData(options, callback) {
    var dayEnd = parseInt(ts2day(options.end));
    var data   = [];

    // get list of directories
    var dayList = getDirectories(adapter.config.storeDir).sort(function (a, b) {
        return b - a;
    });

    // get all files in directory
    for (var i in dayList) {
        var day = parseInt(dayList[i]);

        if (!isNaN(day) && day > 20100101 && day <= dayEnd) {
            var file = options.path + dayList[i].toString() + '/history.' + options.id + '.json';

            if (fs.existsSync(file)) {
                try {
                    var _data = JSON.parse(fs.readFileSync(file)).sort(function (a, b) {
                        return b.ts - a.ts;
                    });

                    for (var ii in _data) {
                        if (options.ack) _data[ii].ack = !!_data[ii];
                        data.push(_data[ii]);
                        if (data.length >= options.count) break;
                    }
                } catch (e) {
                    console.log('Cannot parse file ' + file + ': ' + e.message);
                }
            }
        }
        if (data.length >= options.count) break;
    }
    
    callback(data);
}

function sortByTs(a, b) {
    var aTs = a.ts;
    var bTs = b.ts;
    return ((aTs < bTs) ? -1 : ((aTs > bTs) ? 1 : 0));
}

function getHistory(msg) {
    var startTime = new Date().getTime();
    var options = {
        id:         msg.message.id ? msg.message.id : null,
        path:       adapter.config.storeDir,
        start:      msg.message.options.start,
        end:        msg.message.options.end || Math.round((new Date()).getTime() / 1000) + 5000,
        step:       parseInt(msg.message.options.step) || null,
        count:      parseInt(msg.message.options.count) || 300,
        from:       msg.message.options.from || false,
        ack:        msg.message.options.ack  || false,
        q:          msg.message.options.from || false,
        ignoreNull: msg.message.options.ignoreNull,
        aggregate:  msg.message.options.aggregate || 'average', // One of: max, min, average, total
        limit:      msg.message.options.limit || adapter.config.limit || 2000
    };

    if (options.start > options.end) {
        var _end      = options.end;
        options.end   = options.start;
        options.start = _end;
    }

    if (!options.start && options.count) {
        getCachedData(options, function (cacheData, isFull) {
            // if all data read
            if (isFull && cacheData.length) {
                cacheData = cacheData.sort(sortByTs);
                adapter.log.debug('Send: ' + cacheData.length + ' values in: ' + (new Date().getTime() - startTime) + 'ms');
                adapter.sendTo(msg.from, msg.command, {
                    result: cacheData,
                    step:   null,
                    error:  null
                }, msg.callback);
            } else {
                options.count -= cacheData.length;
                getFileData(options, function (fileData) {
                    cacheData = cacheData.concat(fileData);
                    cacheData = cacheData.sort(sortByTs);
                    adapter.log.debug('Send: ' + cacheData.length + ' values in: ' + (new Date().getTime() - startTime) + 'ms');
                    adapter.sendTo(msg.from, msg.command, {
                        result: cacheData,
                        step:   null,
                        error:  null
                    }, msg.callback);
                });
            }
        });
    }else{
        var gh = cp.fork(__dirname + '/lib/getHistory.js', [JSON.stringify(options)], {silent: false});

        gh.on('message', function (data) {
            if (data[0] == 'getCache') {
                getCachedData(options, function (cacheData) {
                    gh.send(['cacheData', cacheData]);
                });
            } else if (data[0] == 'response') {
                if (data[1]) {
                    adapter.log.debug('Send: ' + data[1].length + ' of: ' + data[2] + ' in: ' + (new Date().getTime() - startTime) + 'ms');
                    adapter.sendTo(msg.from, msg.command, {
                        result: data[1],
                        step:   data[3],
                        error:  null
                    }, msg.callback);
                } else {
                    adapter.log.info('No Data');
                    adapter.sendTo(msg.from, msg.command, {result: [].result, step: null, error: null}, msg.callback);
                }
            }
        });

        setTimeout(function(){
            try {
                gh.kill('SIGINT')
            }
            catch (err){
                adapter.log.error(err);
            }
        }, 120000);
    }
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

