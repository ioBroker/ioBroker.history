/* jshint -W097 */// jshint strict:false
/*jslint node: true */
"use strict";
var cp         = require('child_process');
var utils      = require(__dirname + '/lib/utils'); // Get common adapter utils
var path       = require('path');
var dataDir    = path.normalize(utils.controllerDir + '/' + require(utils.controllerDir + '/lib/tools').getDefaultDataDir());
var fs         = require('fs');
var GetHistory = require(__dirname + '/lib/getHistory.js');
var Aggregate = require(__dirname + '/lib/aggregate.js');

var history = {};
var subscribeAll = false;
var bufferChecker = null;

var adapter = utils.adapter({

    name: 'history',

    objectChange: function (id, obj) {
        if (obj && obj.common &&
            (
                // todo remove history sometime (2016.08) - Do not forget object selector in io-package.json
                (obj.common.history && obj.common.history[adapter.namespace] && obj.common.history[adapter.namespace].enabled) ||
                (obj.common.custom  && obj.common.custom[adapter.namespace]  && obj.common.custom[adapter.namespace].enabled)
            )
        ) {
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

            // todo remove history somewhen (2016.08)
            history[id] = obj.common.custom || obj.common.history;
            history[id].state   = state;
            history[id].list    = list;
            history[id].timeout = timeout;

            if (!history[id][adapter.namespace].maxLength && history[id][adapter.namespace].maxLength !== '0' && history[id][adapter.namespace].maxLength !== 0) {
                history[id][adapter.namespace].maxLength = parseInt(adapter.config.maxLength, 10) || 960;
            } else {
                history[id][adapter.namespace].maxLength = parseInt(history[id][adapter.namespace].maxLength, 10);
            }
            if (!history[id][adapter.namespace].retention && history[id][adapter.namespace].retention !== '0' && history[id][adapter.namespace].retention !== 0) {
                history[id][adapter.namespace].retention = parseInt(adapter.config.retention, 10) || 0;
            } else {
                history[id][adapter.namespace].retention = parseInt(history[id][adapter.namespace].retention, 10) || parseInt(adapter.config.retention, 10) || 0;
            }
            if (!history[id][adapter.namespace].debounce && history[id][adapter.namespace].debounce !== '0' && history[id][adapter.namespace].debounce !== 0) {
                history[id][adapter.namespace].debounce = parseInt(adapter.config.debounce, 10) || 1000;
            } else {
                history[id][adapter.namespace].debounce = parseInt(history[id][adapter.namespace].debounce, 10);
            }
            history[id][adapter.namespace].changesOnly = history[id][adapter.namespace].changesOnly === 'true' || history[id][adapter.namespace].changesOnly === true;
            if (history[id][adapter.namespace].changesRelogInterval !== undefined && history[id][adapter.namespace].changesRelogInterval !== null && history[id][adapter.namespace].changesRelogInterval !== '') {
                history[id][adapter.namespace].changesRelogInterval = parseInt(history[id][adapter.namespace].changesRelogInterval, 10) || 0;
            } else {
                history[id][adapter.namespace].changesRelogInterval = adapter.config.changesRelogInterval;
            }
            if (history[id].relogTimeout) clearTimeout(history[id].relogTimeout);
            if (history[id][adapter.namespace].changesRelogInterval > 0) {
                history[id].relogTimeout = setTimeout(reLogHelper, (history[id][adapter.namespace].changesRelogInterval * 500 * Math.random()) + history[id][adapter.namespace].changesRelogInterval * 500, id);
            }
            if (history[id][adapter.namespace].changesMinDelta !== undefined && history[id][adapter.namespace].changesMinDelta !== null && history[id][adapter.namespace].changesMinDelta !== '') {
                history[id][adapter.namespace].changesMinDelta = parseFloat(history[id][adapter.namespace].changesMinDelta.toString().replace(/,/g, '.')) || 0;
            } else {
                history[id][adapter.namespace].changesMinDelta = adapter.config.changesMinDelta;
            }


            // add one day if retention is too small
            if (history[id][adapter.namespace].retention && history[id][adapter.namespace].retention <= 604800) {
                history[id][adapter.namespace].retention += 86400;
            }

            adapter.log.info('enabled logging of ' + id);
        } else {
            if (history[id]) {
                adapter.log.info('disabled logging of ' + id);
                if (history[id].relogTimeout) clearTimeout(history[id].relogTimeout);
                if (history[id].timeout) clearTimeout(history[id].timeout);
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

function storeCached() {
    for (var id in history) {
        if (history[id].list && history[id].list.length) {
            adapter.log.debug('Store the rest for ' + id);
            appendFile(id, history[id].list);
        }
    }
}

function finish(callback) {
    if (bufferChecker) clearInterval(bufferChecker);
    for (var id in history) {
        if (history[id].relogTimeout) {
            clearTimeout(history[id].relogTimeout);
        }
        if (history[id].timeout) {
            clearTimeout(history[id].timeout);
        }
    }


    storeCached();
    if (callback) callback();
}

function processMessage(msg) {
    if (msg.command == 'getHistory') {
        getHistory(msg);
    } else if (msg.command == 'generateDemo') {
        generateDemo(msg);
    } else if (msg.command === 'storeState') {
        storeState(msg);
    } else if (msg.command === 'enableHistory') {
        enableHistory(msg);
    } else if (msg.command === 'disableHistory') {
        disableHistory(msg);
    } else if (msg.command === 'getEnabledDPs') {
        getEnabledDPs(msg);
    }
}

function fixSelector(callback) {
    // fix _design/custom object
    adapter.getForeignObject('_design/custom', function (err, obj) {
        if (!obj || obj.views.state.map.indexOf('common.history') === -1 || obj.views.state.map.indexOf('common.custom') === -1) {
            obj = {
                _id: '_design/custom',
                language: 'javascript',
                views: {
                    state: {
                        map: 'function(doc) { if (doc.type===\'state\' && (doc.common.custom || doc.common.history)) emit(doc._id, doc.common.custom || doc.common.history) }'
                    }
                }
            };
            adapter.setForeignObject('_design/custom', obj, function (err) {
                if (callback) callback(err);
            });
        } else {
            if (callback) callback(err);
        }
    });
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

    if (adapter.config.changesRelogInterval !== null && adapter.config.changesRelogInterval !== undefined) {
        adapter.config.changesRelogInterval = parseInt(adapter.config.changesRelogInterval, 10);
    } else {
        adapter.config.changesRelogInterval = 0;
    }

    if (adapter.config.changesMinDelta !== null && adapter.config.changesMinDelta !== undefined) {
        adapter.config.changesMinDelta = parseFloat(adapter.config.changesMinDelta.toString().replace(/,/g, '.'));
    } else {
        adapter.config.changesMinDelta = 0;
    }

    // create directory
    if (!fs.existsSync(adapter.config.storeDir)) {
        fs.mkdirSync(adapter.config.storeDir);
    }

    fixSelector(function () {
        adapter.objects.getObjectView('custom', 'state', {}, function (err, doc) {
            var count = 0;
            if (doc && doc.rows) {
                for (var i = 0, l = doc.rows.length; i < l; i++) {
                    if (doc.rows[i].value) {
                        var id = doc.rows[i].id;
                        history[id] = doc.rows[i].value;

                        // todo remove it somewhen (2016.08)
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
                            if (!history[id][adapter.namespace].maxLength && history[id][adapter.namespace].maxLength !== '0' && history[id][adapter.namespace].maxLength !== 0) {
                                history[id][adapter.namespace].maxLength = parseInt(adapter.config.maxLength, 10) || 960;
                            } else {
                                history[id][adapter.namespace].maxLength = parseInt(history[id][adapter.namespace].maxLength, 10);
                            }
                            if (!history[id][adapter.namespace].retention && history[id][adapter.namespace].retention !== '0' && history[id][adapter.namespace].retention !== 0) {
                                history[id][adapter.namespace].retention = parseInt(adapter.config.retention, 10) || 0;
                            } else {
                                history[id][adapter.namespace].retention = parseInt(history[id][adapter.namespace].retention, 10) || parseInt(adapter.config.retention, 10) || 0;
                            }
                            if (!history[id][adapter.namespace].debounce && history[id][adapter.namespace].debounce !== '0' && history[id][adapter.namespace].debounce !== 0) {
                                history[id][adapter.namespace].debounce = parseInt(adapter.config.debounce, 10) || 1000;
                            } else {
                                history[id][adapter.namespace].debounce = parseInt(history[id][adapter.namespace].debounce, 10);
                            }
                            history[id][adapter.namespace].changesOnly = history[id][adapter.namespace].changesOnly === 'true' || history[id][adapter.namespace].changesOnly === true;
                            if (history[id][adapter.namespace].changesRelogInterval !== undefined && history[id][adapter.namespace].changesRelogInterval !== null && history[id][adapter.namespace].changesRelogInterval !== '') {
                                history[id][adapter.namespace].changesRelogInterval = parseInt(history[id][adapter.namespace].changesRelogInterval, 10) || 0;
                            } else {
                                history[id][adapter.namespace].changesRelogInterval = adapter.config.changesRelogInterval;
                            }
                            if (history[id][adapter.namespace].changesRelogInterval > 0) {
                                history[id].relogTimeout = setTimeout(reLogHelper, (history[id][adapter.namespace].changesRelogInterval * 500 * Math.random()) + history[id][adapter.namespace].changesRelogInterval * 500, id);
                            }
                            if (history[id][adapter.namespace].changesMinDelta !== undefined && history[id][adapter.namespace].changesMinDelta !== null && history[id][adapter.namespace].changesMinDelta !== '') {
                                history[id][adapter.namespace].changesMinDelta = parseFloat(history[id][adapter.namespace].changesMinDelta) || 0;
                            } else {
                                history[id][adapter.namespace].changesMinDelta = adapter.config.changesMinDelta;
                            }

                            // add one day if retention is too small
                            if (history[id][adapter.namespace].retention && history[id][adapter.namespace].retention <= 604800) {
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

            // store all buffered data every 10 minutes to not lost the data
            bufferChecker = setInterval(function () {
                storeCached();
            }, 10 * 60000);
        });
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
                    ts:  new Date(start).getTime(),
                    val: value,
                    q:   0,
                    ack: true
                });

                if (curve =='sin') {
                    if (sin == 6.2) {
                        sin = 0;
                    } else {
                        sin = Math.round((sin + 0.1) * 10) / 10;
                    }
                    value = Math.round(Math.sin(sin) * 10000) / 100;
                } else if (curve == 'dec') {
                    value++;
                } else if (curve == 'inc') {
                    value--;
                } else {
                    if (up === true) {
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
                if (!fs.existsSync(path + GetHistory.ts2day(start))) {
                    fs.mkdirSync(path + GetHistory.ts2day(start));
                }
            } catch (err) {
                adapter.log.error(err);
            }

            fs.writeFile(path + GetHistory.ts2day(start) + fileName, JSON.stringify(data), 'utf8', function (err, res) {
                data = [];
                up = !up;

                data.push({
                    ts:   new Date(start).getTime(),
                    val:  value,
                    q:    0,
                    ack:  true
                });

                if (curve == 'sin') {
                    if (sin == 6.2) {
                        sin = 0;
                    } else {
                        sin = Math.round((sin + 0.1) * 10) / 10;
                    }
                    value = Math.round(Math.sin(sin) * 10000) / 100;
                } else if (curve == 'dec') {
                    value++;
                } else if (curve == 'inc') {
                    value--;
                } else {
                    if (up === true) {
                        value++;
                    } else {
                        value--;
                    }
                }

                start += step;

                if (start < end){
                    generate();
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

        //var x = new Date().getTime();

        generate();
    }

    fork(options);
}

function pushHistory(id, state, timerRelog) {
    if (timerRelog === undefined) timerRelog = false;
    // Push into history
    if (history[id]) {
        var settings = history[id][adapter.namespace];

        if (!settings || !state) return;

        if (state.ts < 946681200000) state.ts *= 1000;
        if (state.lc < 946681200000) state.lc *= 1000;

        if (typeof state.val === 'string') {
            var f = parseFloat(state.val);
            if (f == state.val) {
                state.val = f;
            }
        }
        if (history[id].state && settings.changesOnly && !timerRelog) {
            if (settings.changesRelogInterval === 0) {
                if (state.ts !== state.lc) {
                    adapter.log.debug('value not changed ' + id + ', last-value=' + history[id].state.val + ', new-value=' + state.val + ', ts=' + state.ts);
                    return;
                }
            } else if (history[id].lastLogTime) {
                if ((state.ts !== state.lc) && (Math.abs(history[id].lastLogTime - state.ts) < settings.changesRelogInterval * 1000)) {
                    adapter.log.debug('value not changed ' + id + ', last-value=' + history[id].state.val + ', new-value=' + state.val + ', ts=' + state.ts);
                    return;
                }
                if (state.ts !== state.lc) {
                    adapter.log.debug('value-changed-relog ' + id + ', value=' + state.val + ', lastLogTime=' + history[id].lastLogTime + ', ts=' + state.ts);
                }
            }
            if ((settings.changesMinDelta !== 0) && (typeof state.val === 'number') && (Math.abs(history[id].state.val - state.val) < settings.changesMinDelta)) {
                adapter.log.debug('Min-Delta not reached ' + id + ', last-value=' + history[id].state.val + ', new-value=' + state.val + ', ts=' + state.ts);
                return;
            }
            else if (typeof state.val === 'number') {
                adapter.log.debug('Min-Delta reached ' + id + ', last-value=' + history[id].state.val + ', new-value=' + state.val + ', ts=' + state.ts);
            }
            else {
                adapter.log.debug('Min-Delta ignored because no number ' + id + ', last-value=' + history[id].state.val + ', new-value=' + state.val + ', ts=' + state.ts);
            }
        }

        if (history[id].relogTimeout) {
            clearTimeout(history[id].relogTimeout);
            history[id].relogTimeout = null;
        }
        if (settings.changesRelogInterval > 0) {
            history[id].relogTimeout = setTimeout(reLogHelper, settings.changesRelogInterval * 1000, id);
        }

        if (timerRelog) {
            state.ts = new Date().getTime();
            adapter.log.debug('timed-relog ' + id + ', value=' + state.val + ', lastLogTime=' + history[id].lastLogTime + ', ts=' + state.ts);
        } else {
            // only store state if really changed
            history[id].state = state;
        }
        history[id].lastLogTime = state.ts;

        if (settings.debounce) {
            // Discard changes in debounce time to store last stable value
            if (history[id].timeout) clearTimeout(history[id].timeout);
            history[id].timeout = setTimeout(pushHelper, settings.debounce, id);
        } else {
            pushHelper(id);
        }
    }
}

function reLogHelper(_id) {
    if (!history[_id]) {
        adapter.log.info('non-existing id ' + _id);
        return;
    }
    history[_id].relogTimeout = null;
    adapter.getForeignState(_id, function (err, state) {
        if (err) {
            adapter.log.info('init timed Relog: can not get State for ' + _id + ' : ' + err);
        }
        else if (!state) {
            adapter.log.info('init timed Relog: disable relog because state not set so far ' + _id + ': ' + JSON.stringify(state));
        }
        else {
            adapter.log.debug('init timed Relog: getState ' + _id + ':  Value=' + state.val + ', ack=' + state.ack + ', ts=' + state.ts  + ', lc=' + state.lc);
            // only if state is still not set
            if (!history[_id].state) {
                history[_id].state = state;
                pushHistory(_id, history[_id].state, true);
            }
        }
    });
}

function pushHelper(_id) {
    if (!history[_id] || !history[_id].state) return;
    var _settings = history[_id][adapter.namespace];
    // if it was not deleted in this time
    if (_settings) {
        history[_id].timeout = null;
        history[_id].list = history[_id].list || [];

        if (typeof history[_id].state.val === 'string') {
            var f = parseFloat(history[_id].state.val);
            if (f == history[_id].state.val) {
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

        if (history[_id].list.length > _settings.maxLength) {
            adapter.log.debug('moving ' + history[_id].list.length + ' entries from '+ _id +' to file');
            appendFile(_id, history[_id].list);
            checkRetention(_id);
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
            var day = GetHistory.ts2day(d.getTime());
            for (var i = 0; i < dayList.length; i++) {
                if (dayList[i] < day) {
                    var file = adapter.config.storeDir + dayList[i] + '/history.' + id.replace(/[\u0000:\/\\]/g, "~") + '.json';
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
    var day = GetHistory.ts2day(states[states.length - 1].ts);

    var file = adapter.config.storeDir + day + '/history.' + id.replace(/[\u0000:\/\\]/g, "~") + '.json';
    var data;

    var i;
    for (i = states.length - 1; i >= 0; i--) {
        if (!states[i]) continue;
        if (GetHistory.ts2day(states[i].ts) !== day) {
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

function getOneCachedData(id, options, cache, addId) {
    addId = addId || options.addId;

    if (history[id]) {
        var res = history[id].list;
        // todo can be optimized
        if (res) {
            var iProblemCount = 0;
            var vLast = null;
            for (var i = res.length - 1; i >= 0 ; i--) {
                if (!res[i]) {
                    iProblemCount++;
                    continue;
                }
                if (options.start && res[i].ts < options.start) {
                    if (options.ack) res[i].ack = !!res[i].ack;
                    if (addId) res[i].id = id;
                    // add one before start
                    cache.unshift(res[i]);
                    break;
                } else if (res[i].ts > options.end) {
                    // add one after end
                    vLast = res[i];
                    continue;
                }
                if (options.ack) res[i].ack = !!res[i].ack;

                if (vLast) {
                    if (options.ack) vLast.ack = !!vLast.ack;
                    if (addId) res[i].id = id;
                    cache.unshift(vLast);
                    vLast = null;
                }

                if (addId) res[i].id = id;
                cache.unshift(res[i]);

                if (!options.start && cache.length >= options.count) break;
            }
            if (iProblemCount) adapter.log.warn('got null states ' + iProblemCount + ' times for ' + options.id);

            adapter.log.debug('got ' + res.length + ' datapoints for ' + options.id);
        } else {
            adapter.log.debug('datapoints for ' + options.id + ' do not yet exist');
        }
    }
}

function getCachedData(options, callback) {
    var cache = [];

    if (options.id && options.id !== '*') {
        getOneCachedData(options.id, options, cache);
    } else {
        for (var id in history) {
            getOneCachedData(id, options, cache, true);
        }
    }
    options.length = cache.length;
    callback(cache, !options.start && cache.length >= options.count);
}

function tsSort(a, b) {
    return b.ts - a.ts;
}

function getOneFileData(dayList, dayStart, dayEnd, id, options, data, addId) {
    addId = addId || options.addId;

    // get all files in directory
    for (var i = 0; i < dayList.length; i++) {
        var day = parseInt(dayList[i], 10);

        if (!isNaN(day) && day > 20100101 && day >= dayStart && day <= dayEnd) {
            var file = options.path + dayList[i].toString() + '/history.' + id + '.json';

            if (fs.existsSync(file)) {
                try {
                    var _data = JSON.parse(fs.readFileSync(file)).sort(tsSort);
                    var last = false;

                    for (var ii in _data) {
                        if (options.ack) _data[ii].ack = !!_data[ii].ack;
                        if (addId) _data[ii].id = id;
                        data.push(_data[ii]);
                        if (!options.start && data.length >= options.count) break;
                        if (last) break;
                        if (options.start && _data[ii].ts < options.start) last = true;
                    }
                } catch (e) {
                    console.log('Cannot parse file ' + file + ': ' + e.message);
                }
            }
        }
        if (!options.start && data.length >= options.count) break;
        if (day > dayEnd) break;
    }
}

function getFileData(options, callback) {
    var dayStart = options.start ? parseInt(GetHistory.ts2day(options.start), 10) : 0;
    var dayEnd   = parseInt(GetHistory.ts2day(options.end), 10);
    var fileData = [];

    // get list of directories
    var dayList = getDirectories(options.path).sort(function (a, b) {
        return b - a;
    });

    if (options.id && options.id !== '*') {
        getOneFileData(dayList, dayStart, dayEnd, options.id, options, fileData);
    } else {
        for (var id in history) {
            getOneFileData(dayList, dayStart, dayEnd, id, options, fileData, true);
        }
    }

    callback(fileData);
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
        end:        msg.message.options.end || ((new Date()).getTime() + 5000000),
        step:       parseInt(msg.message.options.step,  10) || null,
        count:      parseInt(msg.message.options.count, 10) || 500,
        from:       msg.message.options.from || false,
        ack:        msg.message.options.ack  || false,
        q:          msg.message.options.q    || false,
        ignoreNull: msg.message.options.ignoreNull,
        aggregate:  msg.message.options.aggregate || 'average', // One of: max, min, average, total
        limit:      parseInt(msg.message.options.limit || adapter.config.limit || 2000, 10),
        addId:      msg.message.options.addId || false,
        sessionId:  msg.message.options.sessionId
    };

    if (options.start > options.end) {
        var _end      = options.end;
        options.end   = options.start;
        options.start = _end;
    }

    // if less 2000.01.01 00:00:00
    if (options.start < 946681200000) {
        options.start *= 1000;
        if (options.step !== null && options.step !== undefined) options.step *= 1000;
    }

    // if less 2000.01.01 00:00:00
    if (options.end < 946681200000) options.end *= 1000;

    if ((!options.start && options.count) || options.aggregate === 'onchange' || options.aggregate === '' || options.aggregate === 'none') {
        getCachedData(options, function (cacheData, isFull) {
            adapter.log.debug('after getCachedData: length = ' + cacheData.length + ', isFull=' + isFull);
            // if all data read
            if (isFull && cacheData.length) {
                cacheData = cacheData.sort(sortByTs);
                if ((options.count) && (cacheData.length > options.count) && (options.aggregate === 'none')) {
                    cacheData = cacheData.slice(0, options.count);
                    adapter.log.debug('cut cacheData to ' + options.count + ' values');
                }
                adapter.log.debug('Send: ' + cacheData.length + ' values in: ' + (new Date().getTime() - startTime) + 'ms');
                adapter.sendTo(msg.from, msg.command, {
                    result: cacheData,
                    step:   null,
                    error:  null
                }, msg.callback);
            } else {
                var origCount = options.count;
                options.count -= cacheData.length;
                getFileData(options, function (fileData) {
                    adapter.log.debug('after getFileData: cacheData.length = ' + cacheData.length + ', fileData.length = ' + fileData.length);
                    cacheData = cacheData.concat(fileData);
                    cacheData = cacheData.sort(sortByTs);
                    options.result = cacheData;
                    options.count = origCount;
                    Aggregate.beautify(options);

                    adapter.log.debug('Send: ' + options.result.length + ' values in: ' + (new Date().getTime() - startTime) + 'ms');
                    adapter.sendTo(msg.from, msg.command, {
                        result: options.result,
                        step:   null,
                        error:  null
                    }, msg.callback);
                });
            }
        });
    } else {
        // to use parallel requests activate this.
        if (1 || typeof GetHistory === 'undefined') {
            adapter.log.debug('use parallel requests');
            var gh = cp.fork(__dirname + '/lib/getHistory.js', [JSON.stringify(options)], {silent: false});

            var ghTimeout = setTimeout(function () {
                try {
                    gh.kill('SIGINT');
                }
                catch (err) {
                    adapter.log.error(err);
                }
            }, 120000);

            gh.on('message', function (data) {
                var cmd = data[0];
                if (cmd === 'getCache') {
                    var settings = data[1];
                    getCachedData(settings, function (cacheData) {
                        gh.send(['cacheData', cacheData]);
                    });
                } else if (cmd === 'response') {
                    clearTimeout(ghTimeout);
                    ghTimeout = null;

                    var result          = data[1];
                    var overallLength   = data[2];
                    var step            = data[3];
                    if (result) {
                        adapter.log.debug('Send: ' + result.length + ' of: ' + overallLength + ' in: ' + (new Date().getTime() - startTime) + 'ms');
                        adapter.sendTo(msg.from, msg.command, {
                            result:     result,
                            step:       step,
                            error:      null
                        }, msg.callback);
                    } else {
                        adapter.log.info('No Data');
                        adapter.sendTo(msg.from, msg.command, {
                            result:     [],
                            step:       null,
                            error:      null
                        }, msg.callback);
                    }
                }
            });
        } else {
            GetHistory.initAggregate(options);
            GetHistory.getFileData(options);
            getCachedData(options, function (cachedData) {
                GetHistory.aggregation(options, cachedData);
                var data = GetHistory.response(options);

                if (data[0] === 'response') {
                    if (data[1]) {
                        adapter.log.debug('Send: ' + data[1].length + ' of: ' + data[2] + ' in: ' + (new Date().getTime() - startTime) + 'ms');
                        adapter.sendTo(msg.from, msg.command, {
                            result: data[1],
                            step:   data[3],
                            error:  null
                        }, msg.callback);
                    } else {
                        adapter.log.info('No Data');
                        adapter.sendTo(msg.from, msg.command, {
                            result: [],
                            step:   null,
                            error:  null
                        }, msg.callback);
                    }
                } else {
                    adapter.log.error('Unknown response type: ' + data[0]);
                }
            });
        }
    }
}

function getDirectories(path) {
    return fs.readdirSync(path).filter(function (file) {
        return fs.statSync(path + '/' + file).isDirectory();
    });
}

function storeState(msg) {
    if (!msg.message || !msg.message.id || !msg.message.state) {
        adapter.log.error('storeState called with invalid data');
        adapter.sendTo(msg.from, msg.command, {
            error:  'Invalid call'
        }, msg.callback);
        return;
    }

    if (Array.isArray(msg.message)) {
        adapter.log.debug('storeState: store ' + msg.message.length + ' states for multiple ids');
        for (var i = 0; i < msg.message.length; i++) {
            if (history[msg.message[i].id]) {
                history[msg.message[i].id].state = msg.message[i].state;
                pushHelper(msg.message[i].id);
            }
            else {
                adapter.log.warn('storeState: history not enabled for ' + msg.message[i].id + '. Ignoring');
            }
        }
    } else if (Array.isArray(msg.message.state)) {
        adapter.log.debug('storeState: store ' + msg.message.state.length + ' states for ' + msg.message.id);
        for (var j = 0; j < msg.message.state.length; j++) {
            if (history[msg.message.id]) {
                history[msg.message.id].state = msg.message.state[j];
                pushHelper(msg.message.id);
            }
            else {
                adapter.log.warn('storeState: history not enabled for ' + msg.message.id + '. Ignoring');
            }
        }
    } else {
        adapter.log.debug('storeState: store 1 state for ' + msg.message.id);
        if (history[msg.message.id]) {
            history[msg.message.id].state = msg.message.state;
            pushHelper(msg.message.id);
        }
        else {
            adapter.log.warn('storeState: history not enabled for ' + msg.message.id + '. Ignoring');
        }
    }

    adapter.sendTo(msg.from, msg.command, {
        success:                  true
    }, msg.callback);
}

function enableHistory(msg) {
    if (!msg.message || !msg.message.id) {
        adapter.log.error('enableHistory called with invalid data');
        adapter.sendTo(msg.from, msg.command, {
            error:  'Invalid call'
        }, msg.callback);
        return;
    }
    var obj = {};
    obj.common = {};
    obj.common.custom = {};
    if (msg.message.options) {
        obj.common.custom[adapter.namespace] = msg.message.options;
    }
    else {
        obj.common.custom[adapter.namespace] = {};
    }
    obj.common.custom[adapter.namespace].enabled = true;
    adapter.extendForeignObject(msg.message.id, obj, function (err) {
        if (err) {
            adapter.log.error('enableHistory: ' + err);
            adapter.sendTo(msg.from, msg.command, {
                error:  err
            }, msg.callback);
        } else {
            adapter.log.info(JSON.stringify(obj));
            adapter.sendTo(msg.from, msg.command, {
                success:                  true
            }, msg.callback);
        }
    });
}

function disableHistory(msg) {
    if (!msg.message || !msg.message.id) {
        adapter.log.error('disableHistory called with invalid data');
        adapter.sendTo(msg.from, msg.command, {
            error:  'Invalid call'
        }, msg.callback);
        return;
    }
    var obj = {};
    obj.common = {};
    obj.common.custom = {};
    obj.common.custom[adapter.namespace] = {};
    obj.common.custom[adapter.namespace].enabled = false;
    adapter.extendForeignObject(msg.message.id, obj, function (err) {
        if (err) {
            adapter.log.error('disableHistory: ' + err);
            adapter.sendTo(msg.from, msg.command, {
                error:  err
            }, msg.callback);
        } else {
            adapter.log.info(JSON.stringify(obj));
            adapter.sendTo(msg.from, msg.command, {
                success:                  true
            }, msg.callback);
        }
    });
}

function getEnabledDPs(msg) {
    var data = {};
    for (var id in history) {
        if (!history.hasOwnProperty(id)) continue;
        data[id] = history[id][adapter.namespace];
    }

    adapter.sendTo(msg.from, msg.command, data, msg.callback);
}
