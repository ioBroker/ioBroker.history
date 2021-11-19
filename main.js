/* jshint -W097 */// jshint strict:false
/*jslint node: true */
'use strict';
const cp          = require('child_process');
const utils       = require('@iobroker/adapter-core'); // Get common adapter utils
const path        = require('path');
const dataDir     = path.normalize(utils.controllerDir + '/' + require(utils.controllerDir + '/lib/tools').getDefaultDataDir());
const fs          = require('fs');
const GetHistory  = require('./lib/getHistory.js');
const Aggregate   = require('./lib/aggregate.js');
const adapterName = require('./package.json').name.split('.').pop();

const history    = {};
const aliasMap   = {};
let subscribeAll = false;
let bufferChecker = null;
const tasksStart = [];
let finished   = false;

function isEqual(a, b) {
    //console.log('Compare ' + JSON.stringify(a) + ' with ' +  JSON.stringify(b));
    // Create arrays of property names
    if (a === null || a === undefined || b === null || b === undefined) {
        return (a === b);
    }

    const aProps = Object.getOwnPropertyNames(a);
    const bProps = Object.getOwnPropertyNames(b);

    // If number of properties is different,
    // objects are not equivalent
    if (aProps.length !== bProps.length) {
        //console.log('num props different: ' + JSON.stringify(aProps) + ' / ' + JSON.stringify(bProps));
        return false;
    }

    for (var i = 0; i < aProps.length; i++) {
        const propName = aProps[i];

        if (typeof a[propName] !== typeof b[propName]) {
            //console.log('type props ' + propName + ' different');
            return false;
        }
        if (typeof a[propName] === 'object') {
            if (!isEqual(a[propName], b[propName])) {
                return false;
            }
        }
        else {
            // If values of same property are not equal,
            // objects are not equivalent
            if (a[propName] !== b[propName]) {
                //console.log('props ' + propName + ' different');
                return false;
            }
        }
    }

    // If we made it this far, objects
    // are considered equivalent
    return true;
}

let adapter;
function startAdapter(options) {
    options = options || {};

    Object.assign(options, {

        name: adapterName,

        objectChange: (id, obj) => {
            const formerAliasId = aliasMap[id] ? aliasMap[id] : id;

            if (obj && obj.common &&
                (obj.common.custom  && obj.common.custom[adapter.namespace] && typeof obj.common.custom[adapter.namespace] === 'object' && obj.common.custom[adapter.namespace].enabled
                )
            ) {
                const realId = id;
                let checkForRemove = true;
                if (obj.common.custom && obj.common.custom[adapter.namespace] && obj.common.custom[adapter.namespace].aliasId) {
                    if (obj.common.custom[adapter.namespace].aliasId !== id) {
                        aliasMap[id] = obj.common.custom[adapter.namespace].aliasId;
                        adapter.log.debug('Registered Alias: ' + id + ' --> ' + aliasMap[id]);
                        id = aliasMap[id];
                        checkForRemove = false;
                    }
                    else {
                        adapter.log.warn('Ignoring Alias-ID because identical to ID for ' + id);
                        obj.common.custom[adapter.namespace].aliasId = '';
                    }
                }
                if (checkForRemove && aliasMap[id]) {
                    adapter.log.debug('Removed Alias: ' + id + ' !-> ' + aliasMap[id]);
                    delete aliasMap[id];
                }

                const writeNull = !history[id];
                const state     = history[id] ? history[id].state   : null;
                const list      = history[id] ? history[id].list    : null;
                const timeout   = history[id] ? history[id].timeout : null;

                if (!history[formerAliasId] && !subscribeAll) {
                    // unsubscribe
                    for (const _id in history) {
                        if (history.hasOwnProperty(_id) && history.hasOwnProperty(history[_id].realId)) {
                            adapter.unsubscribeForeignStates(history[_id].realId);
                        }
                    }
                    subscribeAll = true;
                    adapter.subscribeForeignStates('*');
                }

                if (!obj.common.custom[adapter.namespace].maxLength && obj.common.custom[adapter.namespace].maxLength !== '0' && obj.common.custom[adapter.namespace].maxLength !== 0) {
                    obj.common.custom[adapter.namespace].maxLength = parseInt(adapter.config.maxLength, 10) || 960;
                } else {
                    obj.common.custom[adapter.namespace].maxLength = parseInt(obj.common.custom[adapter.namespace].maxLength, 10);
                }
                if (!obj.common.custom[adapter.namespace].retention && obj.common.custom[adapter.namespace].retention !== '0' && obj.common.custom[adapter.namespace].retention !== 0) {
                    obj.common.custom[adapter.namespace].retention = parseInt(adapter.config.retention, 10) || 0;
                } else {
                    obj.common.custom[adapter.namespace].retention = parseInt(obj.common.custom[adapter.namespace].retention, 10) || 0;
                }
                if (!obj.common.custom[adapter.namespace].debounce && obj.common.custom[adapter.namespace].debounce !== '0' && obj.common.custom[adapter.namespace].debounce !== 0) {
                    obj.common.custom[adapter.namespace].debounce = parseInt(adapter.config.debounce, 10) || 1000;
                } else {
                    obj.common.custom[adapter.namespace].debounce = parseInt(obj.common.custom[adapter.namespace].debounce, 10);
                }
                obj.common.custom[adapter.namespace].changesOnly = obj.common.custom[adapter.namespace].changesOnly === 'true' || obj.common.custom[adapter.namespace].changesOnly === true;
                if (obj.common.custom[adapter.namespace].changesRelogInterval !== undefined && obj.common.custom[adapter.namespace].changesRelogInterval !== null && obj.common.custom[adapter.namespace].changesRelogInterval !== '') {
                    obj.common.custom[adapter.namespace].changesRelogInterval = parseInt(obj.common.custom[adapter.namespace].changesRelogInterval, 10) || 0;
                } else {
                    obj.common.custom[adapter.namespace].changesRelogInterval = adapter.config.changesRelogInterval;
                }
                if (obj.common.custom[adapter.namespace].changesMinDelta !== undefined && obj.common.custom[adapter.namespace].changesMinDelta !== null && obj.common.custom[adapter.namespace].changesMinDelta !== '') {
                    obj.common.custom[adapter.namespace].changesMinDelta = parseFloat(obj.common.custom[adapter.namespace].changesMinDelta.toString().replace(/,/g, '.')) || 0;
                } else {
                    obj.common.custom[adapter.namespace].changesMinDelta = adapter.config.changesMinDelta;
                }

                // add one day if retention is too small
                if (obj.common.custom[adapter.namespace].retention && obj.common.custom[adapter.namespace].retention <= 604800) {
                    obj.common.custom[adapter.namespace].retention += 86400;
                }

                if (history[formerAliasId] && history[formerAliasId][adapter.namespace] && isEqual(obj.common.custom[adapter.namespace], history[formerAliasId][adapter.namespace])) {
                    adapter.log.debug('Object ' + id + ' unchanged. Ignore');
                    return;
                }

                history[id] = obj.common.custom;
                history[id].state   = state;
                history[id].list    = list || [];
                history[id].timeout = timeout;
                history[id].realId  = realId;

                if (history[formerAliasId] && history[formerAliasId].relogTimeout) clearTimeout(history[formerAliasId].relogTimeout);

                if (history[id][adapter.namespace].changesRelogInterval > 0) {
                    history[id].relogTimeout = setTimeout(reLogHelper, (history[id][adapter.namespace].changesRelogInterval * 500 * Math.random()) + history[id][adapter.namespace].changesRelogInterval * 500, id);
                }

                if (writeNull && adapter.config.writeNulls) {
                    writeNulls(id);
                }

                adapter.log.info('enabled logging of ' + id + ', Alias=' + (id !== realId));
            } else {
                if (aliasMap[id]) {
                    adapter.log.debug('Removed Alias: ' + id + ' !-> ' + aliasMap[id]);
                    delete aliasMap[id];
                }
                id = formerAliasId;
                if (history[id]) {
                    adapter.log.info('disabled logging of ' + id);
                    if (history[id].relogTimeout) clearTimeout(history[id].relogTimeout);
                    if (history[id].timeout) clearTimeout(history[id].timeout);
                    storeCached(true, id);
                    delete history[id];
                }
            }
        },

        stateChange: (id, state) => {
            id = aliasMap[id] ? aliasMap[id] : id;
            pushHistory(id, state);
        },

        unload: callback => finish(callback),

        ready: () => main(),

        message: obj => processMessage(obj)
    });
    adapter = new utils.Adapter(options);

    return adapter;
}

process.on('SIGINT', () =>
    adapter && adapter.setState && finish());

process.on('SIGTERM', () =>
    adapter && adapter.setState && finish());

function storeCached(isFinishing, onlyId) {
    const now = Date.now();

    for (const id in history) {
        if (!history.hasOwnProperty(id) || (onlyId !== undefined && onlyId !== id)) continue;

        if (isFinishing) {
            if (history[id].skipped) {
                history[id].list.push(history[id].skipped);
                history[id].skipped = null;
            }
            if (adapter.config.writeNulls) {
                const nullValue = {val: null, ts: now, lc: now, q: 0x40, from: 'system.adapter.' + adapter.namespace};
                if (history[id][adapter.namespace].changesOnly && history[id].state && history[id].state !== null) {
                    const state = Object.assign({}, history[id].state);
                    state.ts   = now;
                    state.from = 'system.adapter.' + adapter.namespace;
                    history[id].list.push(state);
                    nullValue.ts += 1;
                    nullValue.lc += 1;
                }

                // terminate values with null to indicate adapter stop.
                history[id].list.push(nullValue);
            }
        }

        if (history[id].list && history[id].list.length) {
            adapter.log.debug('Store the rest for ' + id);
            appendFile(id, history[id].list);
        }
    }
}

function finish(callback) {
    if (!subscribeAll) {
        for (const _id in history) {
            if (history.hasOwnProperty(_id)) {
                adapter.unsubscribeForeignStates(history[_id].realId);
            }
        }
    } else {
        adapter.unsubscribeForeignStates('*');
        subscribeAll = false;
    }
    if (bufferChecker) {
        clearInterval(bufferChecker);
        bufferChecker = null;
    }
    for (const id in history) {
        if (!history.hasOwnProperty(id)) continue;

        if (history[id].relogTimeout) {
            clearTimeout(history[id].relogTimeout);
            history[id].relogTimeout = null;
        }
        if (history[id].timeout) {
            clearTimeout(history[id].timeout);
            history[id].timeout = null;
        }
    }

    if (!finished) {
        finished = true;
        storeCached(true);
    }

    if (callback) {
        callback();
    }
}

function processMessage(msg) {
    if (msg.command === 'features') {
        adapter.sendTo(msg.from, msg.command, {supportedFeatures: []}, msg.callback);
    } else
    if (msg.command === 'getHistory') {
        getHistory(msg);
    } else if (msg.command === 'storeState') {
        storeState(msg);
    } else if (msg.command === 'enableHistory') {
        enableHistory(msg);
    } else if (msg.command === 'disableHistory') {
        disableHistory(msg);
    } else if (msg.command === 'getEnabledDPs') {
        getEnabledDPs(msg);
    } else if (msg.command === 'stopInstance') {
        finish(() => {
            if (msg.callback) {
                adapter.sendTo(msg.from, msg.command, 'stopped', msg.callback);
                setTimeout(() =>
                    adapter.terminate ? adapter.terminate(0): process.exit(0), 200);
            }
        });
    }
}

function fixSelector(callback) {
    // fix _design/custom object
    adapter.getForeignObject('_design/custom', (err, obj) => {
        if (!obj || !obj.views.state.map.includes('common.custom')) {
            obj = {
                _id: '_design/custom',
                language: 'javascript',
                views: {
                    state: {
                        map: 'function(doc) { doc.type === \'state\' && doc.common.custom && emit(doc._id, doc.common.custom) }'
                    }
                }
            };
            adapter.setForeignObject('_design/custom', obj, err => callback && callback(err));
        } else {
            callback && callback(err);
        }
    });
}

function processStartValues() {
    if (tasksStart && tasksStart.length) {
        const task = tasksStart.shift();
        if (history[task.id][adapter.namespace].changesOnly) {
            adapter.getForeignState(history[task.id].realId, (err, state) => {
                const now = task.now || Date.now();
                pushHistory(task.id, {
                    val:  null,
                    ts:   now,
                    ack:  true,
                    q:    0x40,
                    from: 'system.adapter.' + adapter.namespace});

                if (state) {
                    state.ts   = now;
                    state.from = 'system.adapter.' + adapter.namespace;
                    pushHistory(task.id, state);
                }
                setImmediate(processStartValues);
            });
        } else {
            pushHistory(task.id, {
                val:  null,
                ts:   task.now || Date.now(),
                ack:  true,
                q:    0x40,
                from: 'system.adapter.' + adapter.namespace});
            setImmediate(processStartValues);
        }
    }
}

function writeNulls(id, now) {
    if (!id) {
        now = Date.now();
        for (const _id in history) {
            if (history.hasOwnProperty(_id)) {
                writeNulls(_id, now);
            }
        }
    } else {
        now = now || Date.now();
        tasksStart.push({id: id, now: now});
        if (tasksStart.length === 1) {
            processStartValues();
        }
        if (history[id][adapter.namespace].changesRelogInterval > 0) {
            if (history[id].relogTimeout) clearTimeout(history[id].relogTimeout);
            history[id].relogTimeout = setTimeout(reLogHelper, (history[id][adapter.namespace].changesRelogInterval * 500 * Math.random()) + history[id][adapter.namespace].changesRelogInterval * 500, id);
        }
    }
}

function main() { //start
    // set default history if not yet set
    adapter.getForeignObject('system.config', (err, obj) => {
        if (obj && obj.common && !obj.common.defaultHistory) {
            obj.common.defaultHistory = adapter.namespace;
            adapter.setForeignObject('system.config', obj, err => {
                if (err) {
                    adapter.log.error('Cannot set default history instance: ' + err);
                } else {
                    adapter.log.info('Set default history instance to "' + adapter.namespace + '"');
                }
            });
        }
    });


    adapter.config.storeDir = adapter.config.storeDir || 'history';
    adapter.config.storeDir = adapter.config.storeDir.replace(/\\/g, '/');
    if (adapter.config.writeNulls === undefined) adapter.config.writeNulls = true;

    // remove last "/"
    if (adapter.config.storeDir[adapter.config.storeDir.length - 1] === '/') {
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

    try {
        // create directory
        if (!fs.existsSync(adapter.config.storeDir)) {
            fs.mkdirSync(adapter.config.storeDir);
        }
    } catch (err) {
        adapter.log.error('Could not create Storage directory: ' + err);
    }

    fixSelector(function () {
        adapter.getObjectView('custom', 'state', {}, (err, doc) => {
            let count = 0;
            if (doc && doc.rows) {
                for (let i = 0, l = doc.rows.length; i < l; i++) {
                    if (doc.rows[i].value) {
                        let id = doc.rows[i].id;
                        const realId = id;
                        if (doc.rows[i].value[adapter.namespace] && doc.rows[i].value[adapter.namespace].aliasId) {
                            aliasMap[id] = doc.rows[i].value[adapter.namespace].aliasId;
                            adapter.log.debug('Found Alias: ' + id + ' --> ' + aliasMap[id]);
                            id = aliasMap[id];
                        }
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
                        if (!history[id][adapter.namespace] || typeof history[id][adapter.namespace] !== 'object' || history[id][adapter.namespace].enabled === false) {
                            delete history[id];
                        } else {
                            count++;
                            adapter.log.info('enabled logging of ' + id + ' (Count=' + count + '), Alias=' + (id !== realId));
                            if (!history[id][adapter.namespace].maxLength && history[id][adapter.namespace].maxLength !== '0' && history[id][adapter.namespace].maxLength !== 0) {
                                history[id][adapter.namespace].maxLength = parseInt(adapter.config.maxLength, 10) || 960;
                            } else {
                                history[id][adapter.namespace].maxLength = parseInt(history[id][adapter.namespace].maxLength, 10);
                            }
                            if (!history[id][adapter.namespace].retention && history[id][adapter.namespace].retention !== '0' && history[id][adapter.namespace].retention !== 0) {
                                history[id][adapter.namespace].retention = parseInt(adapter.config.retention, 10) || 0;
                            } else {
                                history[id][adapter.namespace].retention = parseInt(history[id][adapter.namespace].retention, 10) || 0;
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

                            history[id].realId = realId;
                            history[id].list = history[id].list || [];
                        }
                    }
                }
            }
            if (count < 20) {
                for (const _id in history) {
                    if (history.hasOwnProperty(_id)) {
                        adapter.subscribeForeignStates(history[_id].realId);
                    }
                }
            } else {
                subscribeAll = true;
                adapter.subscribeForeignStates('*');
            }

            if (adapter.config.writeNulls) writeNulls();

            // store all buffered data every 10 minutes to not lost the data
            bufferChecker = setInterval(function () {
                storeCached();
            }, 10 * 60000);
        });
    });

    adapter.subscribeForeignObjects('*');
}

function pushHistory(id, state, timerRelog) {
    if (timerRelog === undefined) timerRelog = false;
    // Push into history
    if (history[id]) {
        const settings = history[id][adapter.namespace];

        if (!settings || !state) return;

        if (state && state.val === undefined) {
            adapter.log.warn(`state value undefined received for ${id} which is not allowed. Ignoring.`);
            return;
        }

        if (state.ts < 946681200000) state.ts *= 1000;
        if (state.lc < 946681200000) state.lc *= 1000;

        if (typeof state.val === 'string') {
            const f = parseFloat(state.val);
            if (f == state.val) {
                state.val = f;
            }
        }
        if (history[id].state && settings.changesOnly && !timerRelog) {
            if (settings.changesRelogInterval === 0) {
                if ((history[id].state.val !== null || state.val === null) && state.ts !== state.lc) {
                    history[id].skipped = state; // remember new timestamp
                    adapter.log.debug('value not changed ' + id + ', last-value=' + history[id].state.val + ', new-value=' + state.val + ', ts=' + state.ts);
                    return;
                }
            } else if (history[id].lastLogTime) {
                if ((history[id].state.val !== null || state.val === null) && (state.ts !== state.lc) && (Math.abs(history[id].lastLogTime - state.ts) < settings.changesRelogInterval * 1000)) {
                    history[id].skipped = state; // remember new timestamp
                    adapter.log.debug('value not changed ' + id + ', last-value=' + history[id].state.val + ', new-value=' + state.val + ', ts=' + state.ts);
                    return;
                }
                if (state.ts !== state.lc) {
                    adapter.log.debug('value-changed-relog ' + id + ', value=' + state.val + ', lastLogTime=' + history[id].lastLogTime + ', ts=' + state.ts);
                }
            }
            if (history[id].state.val !== null && (settings.changesMinDelta !== 0) && (typeof state.val === 'number') && (Math.abs(history[id].state.val - state.val) < settings.changesMinDelta)) {
                history[id].skipped = state; // remember new timestamp
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

        let ignoreDebonce = false;
        if (timerRelog) {
            state.ts = Date.now();
            state.from = 'system.adapter.' + adapter.namespace;
            adapter.log.debug('timed-relog ' + id + ', value=' + state.val + ', lastLogTime=' + history[id].lastLogTime + ', ts=' + state.ts);
            ignoreDebonce = true;
        } else {
            if (settings.changesOnly && history[id].skipped) {
                history[id].state = history[id].skipped;
                pushHelper(id);
            }
            if (history[id].state && ((history[id].state.val === null && state.val !== null) || (history[id].state.val !== null && state.val === null))) {
                ignoreDebonce = true;
            } else if (!history[id].state && state.val === null) {
                ignoreDebonce = true;
            }
            // only store state if really changed
            history[id].state = state;
        }
        history[id].lastLogTime = state.ts;
        history[id].skipped = null;
        if (settings.debounce && !ignoreDebonce) {
            // Discard changes in de-bounce time to store last stable value
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
    if (history[_id].skipped) {
        history[_id].state = history[_id].skipped;
        history[_id].state.from = 'system.adapter.' + adapter.namespace;
        history[_id].skipped = null;
        pushHistory(_id, history[_id].state, true);
    }
    else {
        adapter.getForeignState(history[_id].realId, function (err, state) {
            if (err) {
                adapter.log.info('init timed Relog: can not get State for ' + _id + ' : ' + err);
            }
            else if (!state) {
                adapter.log.info('init timed Relog: disable relog because state not set so far ' + _id + ': ' + JSON.stringify(state));
            }
            else if (history[_id]) {
                adapter.log.debug('init timed Relog: getState ' + _id + ':  Value=' + state.val + ', ack=' + state.ack + ', ts=' + state.ts  + ', lc=' + state.lc);
                history[_id].state = state;
                pushHistory(_id, history[_id].state, true);
            }
        });
    }
}

function pushHelper(_id) {
    if (!history[_id] || !history[_id].state) return;
    const _settings = history[_id][adapter.namespace];
    // if it was not deleted in this time
    if (_settings) {
        history[_id].timeout = null;
        history[_id].list = history[_id].list || [];

        if (typeof history[_id].state.val === 'string') {
            const f = parseFloat(history[_id].state.val);
            if (f == history[_id].state.val) {
                history[_id].state.val = f;
            } else if (history[_id].state.val === 'true') {
                history[_id].state.val = true;
            } else if (history[_id].state.val === 'false') {
                history[_id].state.val = false;
            }
        }
        if (history[_id].state.lc !== undefined) {
            delete history[_id].state.lc;
        }
        if (!adapter.config.storeAck && history[_id].state.ack !== undefined) {
            delete history[_id].state.ack;
        } else {
            history[_id].state.ack = history[_id].state.ack ? 1 : 0;
        }
        if (!adapter.config.storeFrom && history[_id].state.from !== undefined) {
            delete history[_id].state.from;
        }

        history[_id].list.push(history[_id].state);

        if (history[_id].list.length > _settings.maxLength) {
            adapter.log.debug('moving ' + history[_id].list.length + ' entries from '+ _id +' to file');
            appendFile(_id, history[_id].list);
        }
    }
}

function checkRetention(id) {
    if (history[id][adapter.namespace].retention) {
        const d = new Date();
        const dt = d.getTime();
        // check every 6 hours
        if (!history[id].lastCheck || dt - history[id].lastCheck >= 21600000/* 6 hours */) {
            history[id].lastCheck = dt;
            // get list of directories
            const dayList = getDirectories(adapter.config.storeDir).sort(function (a, b) {
                return a - b;
            });
            // calculate date
            d.setSeconds(-(history[id][adapter.namespace].retention));
            const day = GetHistory.ts2day(d.getTime());
            for (let i = 0; i < dayList.length; i++) {
                if (dayList[i] < day) {
                    const file = GetHistory.getFilenameForID(adapter.config.storeDir, dayList[i], id);
                    if (fs.existsSync(file)) {
                        adapter.log.info('Delete old history "' + file + '"');
                        try {
                            fs.unlinkSync(file);
                        } catch (ex) {
                            adapter.log.error('Cannot delete file "' + file + '": ' + ex);
                        }
                        let files;
                        try {
                            files = fs.readdirSync(adapter.config.storeDir + dayList[i]);
                        } catch (err) {
                            files = [];
                        }
                        if (!files.length) {
                            adapter.log.info('Delete old history dir "' + adapter.config.storeDir + dayList[i] + '"');
                            try {
                                fs.rmdirSync(adapter.config.storeDir + dayList[i]);
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
    const day = GetHistory.ts2day(states[states.length - 1].ts);

    const file = GetHistory.getFilenameForID(adapter.config.storeDir, day, id);
    let data;

    let i;
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

    checkRetention(id);
}

function getOneCachedData(id, options, cache, addId) {
    addId = addId || options.addId;

    if (history[id]) {
        const res = history[id].list;
        // todo can be optimized
        if (res) {
            let iProblemCount = 0;
            let vLast = null;
            for (let i = res.length - 1; i >= 0 ; i--) {
                if (!res[i]) {
                    iProblemCount++;
                    continue;
                }
                if (options.start && res[i].ts < options.start) {
                    if (options.ack) {
                        res[i].ack = !!res[i].ack;
                    }
                    if (addId) {
                        res[i].id = id;
                    }
                    // add one before start
                    cache.unshift(res[i]);
                    break;
                } else if (res[i].ts > options.end) {
                    // add one after end
                    vLast = res[i];
                    continue;
                }
                if (options.ack) {
                    res[i].ack = !!res[i].ack;
                }

                if (vLast) {
                    if (options.ack) {
                        vLast.ack = !!vLast.ack;
                    }
                    if (addId) {
                        res[i].id = id;
                    }
                    cache.unshift(vLast);
                    vLast = null;
                }

                if (addId) {
                    res[i].id = id;
                }
                cache.unshift(res[i]);

                if (!options.start && cache.length >= options.count) {
                    break;
                }
            }

            iProblemCount && adapter.log.warn('got null states ' + iProblemCount + ' times for ' + options.id);

            adapter.log.debug('got ' + res.length + ' datapoints for ' + options.id);
        } else {
            adapter.log.debug('datapoints for ' + options.id + ' do not yet exist');
        }
    }
}

function getCachedData(options, callback) {
    const cache = [];

    if (options.id && options.id !== '*') {
        getOneCachedData(options.id, options, cache);
    } else {
        for (const id in history) {
            if (history.hasOwnProperty(id)) {
                getOneCachedData(id, options, cache, true);
            }
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
    for (let i = 0; i < dayList.length; i++) {
        const day = parseInt(dayList[i], 10);

        if (!isNaN(day) && day > 20100101 && day >= dayStart && day <= dayEnd) {
            const file = GetHistory.getFilenameForID(options.path, dayList[i], id);

            if (fs.existsSync(file)) {
                try {
                    const _data = JSON.parse(fs.readFileSync(file)).sort(tsSort);
                    let last = false;

                    for (const ii in _data) {
                        if (!_data.hasOwnProperty(ii)) continue;
                        if (options.ack) {
                            _data[ii].ack = !!_data[ii].ack;
                        }
                        if (addId) {
                            _data[ii].id = id;
                        }
                        data.push(_data[ii]);
                        if (!options.start && data.length >= options.count) {
                            break;
                        }
                        if (last) {
                            break;
                        }
                        if (options.start && _data[ii].ts < options.start) {
                            last = true;
                        }
                    }
                } catch (e) {
                    console.log('Cannot parse file ' + file + ': ' + e.message);
                }
            }
        }
        if (!options.start && data.length >= options.count) {
            break;
        }
        if (day > dayEnd) {
            break;
        }
    }
}

function getFileData(options, callback) {
    const dayStart = options.start ? parseInt(GetHistory.ts2day(options.start), 10) : 0;
    const dayEnd   = parseInt(GetHistory.ts2day(options.end), 10);
    const fileData = [];

    // get list of directories
    const dayList = getDirectories(options.path).sort((a, b) => b - a);

    if (options.id && options.id !== '*') {
        getOneFileData(dayList, dayStart, dayEnd, options.id, options, fileData);
    } else {
        for (const id in history) {
            if (history.hasOwnProperty(id)) {
                getOneFileData(dayList, dayStart, dayEnd, id, options, fileData, true);
            }
        }
    }

    callback(fileData);
}

function sortByTs(a, b) {
    const aTs = a.ts;
    const bTs = b.ts;
    return ((aTs < bTs) ? -1 : ((aTs > bTs) ? 1 : 0));
}

function applyOptions(data, options, shouldCopy) {
    if (shouldCopy) {
        const _data = [];
        data.forEach(item => {
            const _item = {
                ts: item.ts,
                val: item.val,
            };
            if (options.ack) {
                _item.ack = item.ack;
            }
            if (options.from) {
                _item.from = item.from;
            }
            if (options.q) {
                _item.q = item.q;
            }
            _data.push(_item);
        });
        return _data;
    } else {
        data.forEach(item => {
            if (!options.ack && item.ack !== undefined) {
                delete item.ack;
            }
            if (!options.from && item.from !== undefined) {
                delete item.from;
            }
            if (!options.q && item.q !== undefined) {
                delete item.q;
            }
        });

        return data;
    }
}

function getHistory(msg) {
    const startTime = Date.now();

    if (!msg.message || !msg.message.options) {
        return adapter.sendTo(msg.from, msg.command, {
            error:  "Invalid call. No options for getHistory provided"
        }, msg.callback);
    }

    const options = {
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
    if (options.id && aliasMap[options.id]) {
        options.id = aliasMap[options.id];
    }
    if (options.start > options.end) {
        const _end      = options.end;
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
        getCachedData(options, (cacheData, isFull) => {
            adapter.log.debug('after getCachedData: length = ' + cacheData.length + ', isFull=' + isFull);

            cacheData = applyOptions(cacheData, options, true);

            // if all data read
            if (isFull && cacheData.length) {
                cacheData = cacheData.sort(sortByTs);
                if ((options.count) && (cacheData.length > options.count) && (options.aggregate === 'none')) {
                    cacheData = cacheData.slice(0, options.count);
                    adapter.log.debug('cut cacheData to ' + options.count + ' values');
                }
                adapter.log.debug('Send: ' + cacheData.length + ' values in: ' + (Date.now() - startTime) + 'ms');

                adapter.sendTo(msg.from, msg.command, {
                    result: cacheData,
                    step:   null,
                    error:  null
                }, msg.callback);
            } else {
                const origCount = options.count;
                options.count -= cacheData.length;
                getFileData(options, fileData => {
                    fileData = applyOptions(fileData, options, false);
                    adapter.log.debug('after getFileData: cacheData.length = ' + cacheData.length + ', fileData.length = ' + fileData.length);
                    cacheData = cacheData.concat(fileData);
                    cacheData = cacheData.sort(sortByTs);
                    options.result = cacheData;
                    options.count = origCount;
                    Aggregate.beautify(options);

                    adapter.log.debug('Send: ' + options.result.length + ' values in: ' + (Date.now() - startTime) + 'ms');

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
            try {
                const gh = cp.fork(__dirname + '/lib/getHistory.js', [JSON.stringify(options)], {silent: false});

                let ghTimeout = setTimeout(() => {
                    try {
                        gh.kill('SIGINT');
                    } catch (err) {
                        adapter.log.error(err);
                    }
                }, 120000);

                gh.on('message', data => {
                    const cmd = data[0];
                    if (cmd === 'getCache') {
                        const settings = data[1];
                        getCachedData(settings, cacheData => {
                            try {
                                gh.send(['cacheData', cacheData]);
                            } catch (err) {
                                adapter.log.info('Can not send data to forked process: ' + err.message);
                            }
                        });
                    } else if (cmd === 'response') {
                        clearTimeout(ghTimeout);
                        ghTimeout = null;

                        const result = applyOptions(data[1], options, true);
                        const overallLength = data[2];
                        const step = data[3];
                        if (result) {
                            adapter.log.debug('Send: ' + result.length + ' of: ' + overallLength + ' in: ' + (Date.now() - startTime) + 'ms');
                            adapter.sendTo(msg.from, msg.command, {
                                result: result,
                                step: step,
                                error: null
                            }, msg.callback);
                        } else {
                            adapter.log.info('No Data');
                            adapter.sendTo(msg.from, msg.command, {
                                result: [],
                                step: null,
                                error: null
                            }, msg.callback);
                        }
                    }
                });
            } catch (err) {
                adapter.log.info('Can not use parallel requests: ' + err.message);
            }
        } else {
            GetHistory.initAggregate(options);
            GetHistory.getFileData(options);
            getCachedData(options, cachedData => {
                GetHistory.aggregation(options, cachedData);
                const data = GetHistory.response(options);

                if (data[0] === 'response') {
                    if (data[1]) {
                        adapter.log.debug(`Send: ${data[1].length} of: ${data[2]} in: ${Date.now() - startTime}ms`);
                        const timeSeries = applyOptions(data[1], options, true);
                        adapter.sendTo(msg.from, msg.command, {
                            result: timeSeries,
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
    if (!fs.existsSync(path)) {
        adapter.log.warn('Data directory ' + path + ' does not exist');
        return [];
    }
    try {
        return fs.readdirSync(path).filter(file => {
            try {
                return fs.statSync(path + '/' + file).isDirectory()
            } catch(e) {
                //ignore entry
                return false;
            }
        });
    } catch(err) {
        //ignore
        adapter.log.warn('Error reading data directory ' + path + ': ' + err);
        return [];
    }
}

function storeState(msg) {
    if (msg.message && (msg.message.success || msg.message.error)) { // Seems we got a callback from running converter
        return;
    }
    if (!msg.message || !msg.message.id || !msg.message.state) {
        adapter.log.error('storeState called with invalid data');
        adapter.sendTo(msg.from, msg.command, {
            error:  'Invalid call'
        }, msg.callback);
        return;
    }
    let id;
    if (Array.isArray(msg.message)) {
        adapter.log.debug('storeState: store ' + msg.message.length + ' states for multiple ids');
        for (let i = 0; i < msg.message.length; i++) {
            id = aliasMap[msg.message[i].id] ? aliasMap[msg.message[i].id] : msg.message[i].id;
            if (history[id]) {
                history[id].state = msg.message[i].state;
                pushHelper(id);
            }
            else {
                adapter.log.warn('storeState: history not enabled for ' + msg.message[i].id + '. Ignoring');
            }
        }
    } else if (msg.message.state && Array.isArray(msg.message.state)) {
        adapter.log.debug('storeState: store ' + msg.message.state.length + ' states for ' + msg.message.id);
        for (let j = 0; j < msg.message.state.length; j++) {
            id = aliasMap[msg.message.id] ? aliasMap[msg.message.id] : msg.message.id;
            if (history[id]) {
                history[id].state = msg.message.state[j];
                pushHelper(id);
            }
            else {
                adapter.log.warn('storeState: history not enabled for ' + msg.message.id + '. Ignoring');
            }
        }
    } else {
        adapter.log.debug('storeState: store 1 state for ' + msg.message.id);
        id = aliasMap[msg.message.id] ? aliasMap[msg.message.id] : msg.message.id;
        if (history[id]) {
            history[id].state = msg.message.state;
            pushHelper(id);
        }
        else {
            adapter.log.warn('storeState: history not enabled for ' + msg.message.id + '. Ignoring');
        }
    }

    adapter.sendTo(msg.from, msg.command, {
        success: true
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
    const obj = {};
    obj.common = {};
    obj.common.custom = {};
    if (msg.message.options) {
        obj.common.custom[adapter.namespace] = msg.message.options;
    }
    else {
        obj.common.custom[adapter.namespace] = {};
    }
    obj.common.custom[adapter.namespace].enabled = true;
    adapter.extendForeignObject(msg.message.id, obj, err => {
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
    const obj = {};
    obj.common = {};
    obj.common.custom = {};
    obj.common.custom[adapter.namespace] = {};
    obj.common.custom[adapter.namespace].enabled = false;
    adapter.extendForeignObject(msg.message.id, obj, err => {
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
    const data = {};
    for (const id in history) {
        if (!history.hasOwnProperty(id)) continue;
        data[history[id].realId] = history[id][adapter.namespace];
    }

    adapter.sendTo(msg.from, msg.command, data, msg.callback);
}

// If started as allInOne/compact mode => return function to create instance
if (module && module.parent) {
    module.exports = startAdapter;
} else {
    // or start the instance directly
    startAdapter();
}
