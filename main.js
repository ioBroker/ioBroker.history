/* jshint -W097 */
/* jshint strict: false */
/* jslint node: true */
'use strict';
const cp          = require('child_process');
const utils       = require('@iobroker/adapter-core'); // Get common adapter utils
const path        = require('path');
const dataDir     = path.normalize(utils.controllerDir + '/' + require(utils.controllerDir + '/lib/tools').getDefaultDataDir());
const fs          = require('fs');
const GetHistory  = require('./lib/getHistory.js');
const Aggregate   = require('./lib/aggregate.js');
const adapterName = require('./package.json').name.split('.').pop();

const history     = {};
const aliasMap    = {};
let subscribeAll  = false;
let bufferChecker = null;
const tasksStart  = [];
let finished      = false;

function isEqual(a, b) {
    //console.log('Compare ' + JSON.stringify(a) + ' with ' +  JSON.stringify(b));
    // Create arrays of property names
    if (a === null || a === undefined || b === null || b === undefined) {
        return a === b;
    }

    const aProps = Object.getOwnPropertyNames(a);
    const bProps = Object.getOwnPropertyNames(b);

    // If number of properties is different,
    // objects are not equivalent
    if (aProps.length !== bProps.length) {
        //console.log('num props different: ' + JSON.stringify(aProps) + ' / ' + JSON.stringify(bProps));
        return false;
    }

    for (let i = 0; i < aProps.length; i++) {
        const propName = aProps[i];

        if (typeof a[propName] !== typeof b[propName]) {
            //console.log('type props ' + propName + ' different');
            return false;
        }
        if (typeof a[propName] === 'object') {
            if (!isEqual(a[propName], b[propName])) {
                return false;
            }
        } else {
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
                        adapter.log.debug(`Registered Alias: ${id} --> ${aliasMap[id]}`);
                        id = aliasMap[id];
                        checkForRemove = false;
                    }
                    else {
                        adapter.log.warn('Ignoring Alias-ID because identical to ID for ' + id);
                        obj.common.custom[adapter.namespace].aliasId = '';
                    }
                }
                if (checkForRemove && aliasMap[id]) {
                    adapter.log.debug(`Removed Alias: ${id} !-> ${aliasMap[id]}`);
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
                if (!obj.common.custom[adapter.namespace].blockTime && obj.common.custom[adapter.namespace].blockTime !== '0' && obj.common.custom[adapter.namespace].blockTime !== 0) {
                    if (!obj.common.custom[adapter.namespace].debounce && obj.common.custom[adapter.namespace].debounce !== '0' && obj.common.custom[adapter.namespace].debounce !== 0) {
                        obj.common.custom[adapter.namespace].blockTime = parseInt(adapter.config.blockTime, 10) || 0;
                    } else {
                        obj.common.custom[adapter.namespace].blockTime = parseInt(obj.common.custom[adapter.namespace].debounce, 10) || 0;
                    }
                } else {
                    obj.common.custom[adapter.namespace].blockTime = parseInt(obj.common.custom[adapter.namespace].blockTime, 10) || 0;
                }
                if (!obj.common.custom[adapter.namespace].debounceTime && obj.common.custom[adapter.namespace].debounceTime !== '0' && obj.common.custom[adapter.namespace].debounceTime !== 0) {
                    obj.common.custom[adapter.namespace].debounceTime = parseInt(adapter.config.debounceTime, 10) || 0;
                } else {
                    obj.common.custom[adapter.namespace].debounceTime = parseInt(obj.common.custom[adapter.namespace].debounceTime, 10) || 0;
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

                obj.common.custom[adapter.namespace].ignoreZero = obj.common.custom[adapter.namespace].ignoreZero === 'true' || obj.common.custom[adapter.namespace].ignoreZero === true;
                obj.common.custom[adapter.namespace].ignoreBelowZero = obj.common.custom[adapter.namespace].ignoreBelowZero === 'true' || obj.common.custom[adapter.namespace].ignoreBelowZero === true;

                if (obj.common.custom[adapter.namespace].disableSkippedValueLogging !== undefined && obj.common.custom[adapter.namespace].disableSkippedValueLogging !== null && obj.common.custom[adapter.namespace].disableSkippedValueLogging !== '') {
                    obj.common.custom[adapter.namespace].disableSkippedValueLogging = obj.common.custom[adapter.namespace].disableSkippedValueLogging === 'true' || obj.common.custom[adapter.namespace].disableSkippedValueLogging === true;
                } else {
                    obj.common.custom[adapter.namespace].disableSkippedValueLogging = adapter.config.disableSkippedValueLogging;
                }

                if (obj.common.custom[adapter.namespace].enableDebugLogs !== undefined && obj.common.custom[adapter.namespace].enableDebugLogs !== null && obj.common.custom[adapter.namespace].enableDebugLogs !== '') {
                    obj.common.custom[adapter.namespace].enableDebugLogs = obj.common.custom[adapter.namespace].enableDebugLogs === 'true' || obj.common.custom[adapter.namespace].enableDebugLogs === true;
                } else {
                    obj.common.custom[adapter.namespace].enableDebugLogs = adapter.config.enableDebugLogs;
                }

                // add one day if retention is too small
                if (obj.common.custom[adapter.namespace].retention && obj.common.custom[adapter.namespace].retention <= 604800) {
                    obj.common.custom[adapter.namespace].retention += 86400;
                }

                if (history[formerAliasId] && history[formerAliasId][adapter.namespace] && isEqual(obj.common.custom[adapter.namespace], history[formerAliasId][adapter.namespace])) {
                    return adapter.log.debug(`Object ${id} unchanged. Ignore`);
                }

                history[id] = obj.common.custom;
                history[id].state   = state;
                history[id].list    = list || [];
                history[id].timeout = timeout;
                history[id].realId  = realId;

                if (history[formerAliasId] && history[formerAliasId].relogTimeout) {
                    clearTimeout(history[formerAliasId].relogTimeout);
                    history[formerAliasId].relogTimeout = null;
                }

                if (history[id][adapter.namespace].changesRelogInterval > 0) {
                    history[id].relogTimeout = setTimeout(reLogHelper, (history[id][adapter.namespace].changesRelogInterval * 500 * Math.random()) + history[id][adapter.namespace].changesRelogInterval * 500, id);
                }

                if (writeNull && adapter.config.writeNulls) {
                    writeNulls(id);
                }

                adapter.log.info(`enabled logging of ${id}, Alias=${id !== realId}`);
            } else {
                if (aliasMap[id]) {
                    adapter.log.debug(`Removed Alias: ${id} !-> ${aliasMap[id]}`);
                    delete aliasMap[id];
                }
                id = formerAliasId;
                if (history[id]) {
                    adapter.log.info('disabled logging of ' + id);
                    if (history[id].relogTimeout) {
                        clearTimeout(history[id].relogTimeout);
                        history[id].relogTimeout = null;
                    }
                    if (history[id].timeout) {
                        clearTimeout(history[id].timeout);
                        history[id].timeout = null;
                    }
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
        if (!history.hasOwnProperty(id) || (onlyId !== undefined && onlyId !== id)) {
            continue;
        }

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
        if (!history.hasOwnProperty(id)) {
            continue;
        }

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

    callback && callback();
}

function processMessage(msg) {
    if (msg.command === 'features') {
        adapter.sendTo(msg.from, msg.command, {supportedFeatures: ['update', 'delete', 'deleteRange', 'deleteAll', 'storeState']}, msg.callback);
    } else if (msg.command === 'update') {
        updateState(msg);
    } else if (msg.command === 'delete') {
        deleteState(msg);
    } else if (msg.command === 'deleteAll') {
        deleteStateAll(msg);
    } else if (msg.command === 'deleteRange') {
        deleteState(msg);
    } else if (msg.command === 'getHistory') {
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
                    from: 'system.adapter.' + adapter.namespace
                });

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
                from: 'system.adapter.' + adapter.namespace
            });

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
        tasksStart.push({id, now});
        if (tasksStart.length === 1) {
            processStartValues();
        }
        if (history[id][adapter.namespace].changesRelogInterval > 0) {
            history[id].relogTimeout && clearTimeout(history[id].relogTimeout);
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

    if (adapter.config.blockTime !== null && adapter.config.blockTime !== undefined) {
        adapter.config.blockTime = parseInt(adapter.config.blockTime, 10) || 0;
    } else {
        if (adapter.config.debounce !== null && adapter.config.debounce !== undefined) {
            adapter.config.debounce = parseInt(adapter.config.debounce, 10) || 0;
        } else {
            adapter.config.blockTime = 0;
        }
    }

    if (adapter.config.debounceTime !== null && adapter.config.debounceTime !== undefined) {
        adapter.config.debounceTime = parseInt(adapter.config.debounceTime, 10) || 0;
    } else {
        adapter.config.debounceTime = 0;
    }



    try {
        // create directory
        if (!fs.existsSync(adapter.config.storeDir)) {
            fs.mkdirSync(adapter.config.storeDir);
        }
    } catch (err) {
        adapter.log.error('Could not create Storage directory: ' + err);
    }

    adapter.getObjectView('system', 'custom', {}, (err, doc) => {
        let count = 0;
        if (doc && doc.rows) {
            for (let i = 0, l = doc.rows.length; i < l; i++) {
                if (doc.rows[i].value) {
                    let id = doc.rows[i].id;
                    const realId = id;
                    if (doc.rows[i].value[adapter.namespace] && doc.rows[i].value[adapter.namespace].aliasId) {
                        aliasMap[id] = doc.rows[i].value[adapter.namespace].aliasId;
                        adapter.log.debug(`Found Alias: ${id} --> ${aliasMap[id]}`);
                        id = aliasMap[id];
                    }
                    history[id] = doc.rows[i].value;

                    if (!history[id][adapter.namespace] || typeof history[id][adapter.namespace] !== 'object' || history[id][adapter.namespace].enabled === false) {
                        delete history[id];
                    } else {
                        count++;
                        adapter.log.info(`enabled logging of ${id} (Count=${count}), Alias=${id !== realId}`);
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
                        if (!history[id][adapter.namespace].blockTime && history[id][adapter.namespace].blockTime !== '0' && history[id][adapter.namespace].blockTime !== 0) {
                            if (!history[id][adapter.namespace].debounce && history[id][adapter.namespace].debounce !== '0' && history[id][adapter.namespace].debounce !== 0) {
                                history[id][adapter.namespace].blockTime = parseInt(adapter.config.blockTime, 10) || 0;
                            } else {
                                history[id][adapter.namespace].blockTime = parseInt(history[id][adapter.namespace].debounce, 10) || 0;
                            }
                        } else {
                            history[id][adapter.namespace].blockTime = parseInt(history[id][adapter.namespace].blockTime, 10) || 0;
                        }
                        if (!history[id][adapter.namespace].debounceTime && history[id][adapter.namespace].debounceTime !== '0' && history[id][adapter.namespace].debounceTime !== 0) {
                            history[id][adapter.namespace].debounceTime = parseInt(adapter.config.debounceTime, 10) || 0;
                        } else {
                            history[id][adapter.namespace].debounceTime = parseInt(history[id][adapter.namespace].debounceTime, 10) || 0;
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

                        history[id][adapter.namespace].ignoreZero = history[id][adapter.namespace].ignoreZero === 'true' || history[id][adapter.namespace].ignoreZero === true;
                        history[id][adapter.namespace].ignoreBelowZero = history[id][adapter.namespace].ignoreBelowZero === 'true' || history[id][adapter.namespace].ignoreBelowZero === true;

                        if (history[id][adapter.namespace].disableSkippedValueLogging !== undefined && history[id][adapter.namespace].disableSkippedValueLogging !== null && history[id][adapter.namespace].disableSkippedValueLogging !== '') {
                            history[id][adapter.namespace].disableSkippedValueLogging = history[id][adapter.namespace].disableSkippedValueLogging === 'true' || history[id][adapter.namespace].disableSkippedValueLogging === true;
                        } else {
                            history[id][adapter.namespace].disableSkippedValueLogging = adapter.config.disableSkippedValueLogging;
                        }

                        if (history[id][adapter.namespace].enableDebugLogs !== undefined && history[id][adapter.namespace].enableDebugLogs !== null && history[id][adapter.namespace].enableDebugLogs !== '') {
                            history[id][adapter.namespace].enableDebugLogs = history[id][adapter.namespace].enableDebugLogs === 'true' || history[id][adapter.namespace].enableDebugLogs === true;
                        } else {
                            history[id][adapter.namespace].enableDebugLogs = adapter.config.enableDebugLogs;
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

        adapter.config.writeNulls && writeNulls();

        // store all buffered data every 10 minutes to not lost the data
        bufferChecker = setInterval(() => storeCached(), 10 * 60000);
    });

    adapter.subscribeForeignObjects('*');
}

function pushHistory(id, state, timerRelog) {
    if (timerRelog === undefined) timerRelog = false;
    // Push into history
    if (history[id]) {
        const settings = history[id][adapter.namespace];

        if (!settings || !state) {
            return;
        }

        if (state && state.val === undefined) {
            return adapter.log.warn(`state value undefined received for ${id} which is not allowed. Ignoring.`);
        }

        // convert from seconds to milliseconds
        if (state.ts < 946681200000) {
            state.ts *= 1000;
        }
        // convert from seconds to milliseconds
        if (state.lc < 946681200000) {
            state.lc *= 1000;
        }

        if (typeof state.val === 'string') {
            const f = parseFloat(state.val);
            if (f == state.val) {
                state.val = f;
            }
        }

        let ignoreDebonce = false;

        if (!timerRelog) {
            const valueUnstable = !!history[id].timeout;
            // When a debounce timer runs and the value is the same as the last one, ignore it
            if (history[id].timeout && state.ts !== state.lc) {
                settings.enableDebugLogs && adapter.log.debug(`value not changed debounce ${id}, value=${state.val}, ts=${state.ts}, debounce timer keeps running`);
                return;
            } else if (history[id].timeout) { // if value changed, clear timer
                settings.enableDebugLogs && adapter.log.debug(`value changed during debounce time ${id}, value=${state.val}, ts=${state.ts}, debounce timer restarted`);
                clearTimeout(history[id].timeout);
                history[id].timeout = null;
            }

            if (!valueUnstable && settings.blockTime && history[id].state && (history[id].state.ts + settings.blockTime) > state.ts) {
                settings.enableDebugLogs && adapter.log.debug(`value ignored blockTime ${id}, value=${state.val}, ts=${state.ts}, lastState.ts=${history[id].state.ts}, blockTime=${settings.blockTime}`);
                return;
            }

            if (settings.ignoreZero && (state.val === undefined || state.val === null || state.val === 0)) {
                settings.enableDebugLogs && adapter.log.debug(`value ignore because zero or null ${id}, new-value=${state.val}, ts=${state.ts}`);
                return;
            } else
            if (settings.ignoreBelowZero && typeof state.val === 'number' && state.val < 0) {
                settings.enableDebugLogs && adapter.log.debug(`value ignored because below 0 ${id}, new-value=${state.val}, ts=${state.ts}`);
                return;
            }

            if (history[id].state && settings.changesOnly && !timerRelog) {
                if (settings.changesRelogInterval === 0) {
                    if ((history[id].state.val !== null || state.val === null) && state.ts !== state.lc) {
                        // remember new timestamp
                        if (!valueUnstable && !settings.disableSkippedValueLogging) {
                            history[id].skipped = state;
                        }
                        settings.enableDebugLogs && adapter.log.debug(`value not changed ${id}, last-value=${history[id].state.val}, new-value=${state.val}, ts=${state.ts}`);
                        return;
                    }
                } else if (history[id].lastLogTime) {
                    if ((history[id].state.val !== null || state.val === null) && (state.ts !== state.lc) && (Math.abs(history[id].lastLogTime - state.ts) < settings.changesRelogInterval * 1000)) {
                        // remember new timestamp
                        if (!valueUnstable && !settings.disableSkippedValueLogging) {
                            history[id].skipped = state;
                        }
                        settings.enableDebugLogs && adapter.log.debug(`value not changed ${id}, last-value=${history[id].state.val}, new-value=${state.val}, ts=${state.ts}`);
                        return;
                    }
                    if (state.ts !== state.lc) {
                        settings.enableDebugLogs && adapter.log.debug(`value-not-changed-relog ${id}, value=${state.val}, lastLogTime=${history[id].lastLogTime}, ts=${state.ts}`);
                        ignoreDebonce = true;
                    }
                }
                if (typeof state.val === 'number') {
                    if (
                        history[id].state.val !== null &&
                        settings.changesMinDelta !== 0 &&
                        Math.abs(history[id].state.val - state.val) < settings.changesMinDelta
                    ) {
                        if (!valueUnstable && !settings.disableSkippedValueLogging) {
                            history[id].skipped = state;
                        }
                        settings.enableDebugLogs && adapter.log.debug(`Min-Delta not reached ${id}, last-value=${history[id].state.val}, new-value=${state.val}, ts=${state.ts}`);
                        return;
                    } else if (settings.changesMinDelta !== 0) {
                        settings.enableDebugLogs && adapter.log.debug(`Min-Delta reached ${id}, last-value=${history[id].state.val}, new-value=${state.val}, ts=${state.ts}`);
                    }
                } else {
                    settings.enableDebugLogs && adapter.log.debug(`Min-Delta ignored because no number ${id}, last-value=${history[id].state.val}, new-value=${state.val}, ts=${state.ts}`);
                }
            }
        }

        if (history[id].relogTimeout) {
            clearTimeout(history[id].relogTimeout);
            history[id].relogTimeout = null;
        }

        if (timerRelog) {
            state = Object.assign({}, state);
            state.ts = Date.now();
            state.from = 'system.adapter.' + adapter.namespace;
            settings.enableDebugLogs && adapter.log.debug(`timed-relog ${id}, value=${state.val}, lastLogTime=${history[id].lastLogTime}, ts=${state.ts}`);
            ignoreDebonce = true;
        } else {
            if (settings.changesOnly && history[id].skipped) {
                settings.enableDebugLogs && adapter.log.debug(`Skipped value logged ${id}, value=${history[id].skipped.val}, ts=${history[id].skipped.ts}`);
                pushHelper(id, history[id].skipped);
                history[id].skipped = null;
            }
            if (history[id].state && ((history[id].state.val === null && state.val !== null) || (history[id].state.val !== null && state.val === null))) {
                ignoreDebonce = true;
            } else if (!history[id].state && state.val === null) {
                ignoreDebonce = true;
            }
        }
        if (settings.debounceTime && !ignoreDebonce && !timerRelog) {
            // Discard changes in de-bounce time to store last stable value
            history[id].timeout && clearTimeout(history[id].timeout);
            history[id].timeout = setTimeout((id, state) => {
                history[id].timeout = null;
                history[id].state = state;
                history[id].lastLogTime = state.ts;
                settings.enableDebugLogs && adapter.log.debug(`Value logged ${id}, value=${history[id].state.val}, ts=${history[id].state.ts}`);
                pushHelper(id);
                if (settings.changesRelogInterval > 0) {
                    history[id].relogTimeout = setTimeout(reLogHelper, settings.changesRelogInterval * 1000, id);
                }
            }, settings.debounceTime, id, state);
        } else {
            if (!timerRelog) {
                history[id].state = state;
            }
            history[id].lastLogTime = state.ts;

            settings.enableDebugLogs && adapter.log.debug(`Value logged ${id}, value=${history[id].state.val}, ts=${history[id].state.ts}`);
            pushHelper(id, state);
            if (settings.changesRelogInterval > 0) {
                history[id].relogTimeout = setTimeout(reLogHelper, settings.changesRelogInterval * 1000, id);
            }
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
        pushHistory(_id, history[_id].skipped, true);
    } else if (history[_id].state) {
        pushHistory(_id, history[_id].state, true);
    }
    else {
        adapter.getForeignState(history[_id].realId, (err, state) => {
            if (err) {
                adapter.log.info(`init timed Relog: can not get State for ${_id} : ${err}`);
            }
            else if (!state) {
                adapter.log.info(`init timed Relog: disable relog because state not set so far ${_id}: ${JSON.stringify(state)}`);
            }
            else if (history[_id]) {
                adapter.log.debug(`init timed Relog: getState ${_id}:  Value=${state.val}, ack=${state.ack}, ts=${state.ts}, lc=${state.lc}`);
                history[_id].state = state;
                pushHistory(_id, history[_id].state, true);
            }
        });
    }
}

function pushHelper(_id, state) {
    if (!history[_id] || (!history[_id].state && !state)) return;
    if (!state) {
        state = history[_id].state;
    }

    // if it was not deleted in this time
    history[_id].list = history[_id].list || [];

    if (typeof state.val === 'string') {
        const f = parseFloat(state.val);
        if (f == state.val) {
            state.val = f;
        } else if (state.val === 'true') {
            state.val = true;
        } else if (state.val === 'false') {
            state.val = false;
        }
    }
    if (state.lc !== undefined) {
        delete state.lc;
    }
    if (!adapter.config.storeAck && state.ack !== undefined) {
        delete state.ack;
    } else {
        state.ack = state.ack ? 1 : 0;
    }
    if (!adapter.config.storeFrom && state.from !== undefined) {
        delete state.from;
    }

    history[_id].list.push(state);

    const _settings = history[_id][adapter.namespace];
    if (_settings && history[_id].list.length > _settings.maxLength) {
        _settings.enableDebugLogs && adapter.log.debug(`moving ${history[_id].list.length} entries from ${_id} to file`);
        appendFile(_id, history[_id].list);
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
            const dayList = getDirectories(adapter.config.storeDir).sort((a, b) => a - b);
            // calculate date
            d.setSeconds(-(history[id][adapter.namespace].retention));

            const day = GetHistory.ts2day(d.getTime());

            for (let i = 0; i < dayList.length; i++) {
                if (dayList[i] < day) {
                    const file = GetHistory.getFilenameForID(adapter.config.storeDir, dayList[i], id);
                    if (fs.existsSync(file)) {
                        adapter.log.info(`Delete old history "${file}"`);
                        try {
                            fs.unlinkSync(file);
                        } catch (ex) {
                            adapter.log.error(`Cannot delete file "${file}": ${ex}`);
                        }
                        let files;
                        try {
                            files = fs.readdirSync(adapter.config.storeDir + dayList[i]);
                        } catch (err) {
                            files = [];
                        }
                        if (!files.length) {
                            adapter.log.info(`Delete old history dir "${adapter.config.storeDir}${dayList[i]}"`);
                            try {
                                fs.rmdirSync(adapter.config.storeDir + dayList[i]);
                            } catch (ex) {
                                adapter.log.error(`Cannot delete directory "${adapter.config.storeDir}${dayList[i]}": ${ex}`);
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
        if (!states[i]) {
            continue;
        }
        if (GetHistory.ts2day(states[i].ts) !== day) {
            break;
        }
    }

    data = states.splice(i - states.length + 1);

    if (fs.existsSync(file)) {
        try {
            data = JSON.parse(fs.readFileSync(file)).concat(data);
        } catch (err) {
            adapter.log.error(`Cannot read file ${file}: ${err}`);
        }
    }

    try {
        // create directory
        if (!fs.existsSync(adapter.config.storeDir + day)) {
            fs.mkdirSync(adapter.config.storeDir + day);
        }
        fs.writeFileSync(file, JSON.stringify(data, null, 2));
    } catch (ex) {
        adapter.log.error(`Cannot store file ${file}: ${ex}`);
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

            iProblemCount && adapter.log.warn(`got null states ${iProblemCount} times for ${options.id}`);

            adapter.log.debug(`got ${res.length} datapoints for ${options.id}`);
        } else {
            adapter.log.debug(`datapoints for ${options.id} do not yet exist`);
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
    callback(cache, !options.start && options.count && cache.length >= options.count);
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
                        if (!_data.hasOwnProperty(ii)) {
                            continue;
                        }
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
                    console.log(`Cannot parse file ${file}: ${e.message}`);
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
    return (aTs < bTs) ? -1 : ((aTs > bTs) ? 1 : 0);
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
            error:  'Invalid call. No options for getHistory provided'
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
        limit:      parseInt(msg.message.options.limit, 10) || parseInt(msg.message.options.count, 10) || adapter.config.limit || 2000,
        addId:      msg.message.options.addId || false,
        sessionId:  msg.message.options.sessionId,
        returnNewestEntries: msg.message.options.returnNewestEntries || false
    };

    if (!options.start && options.count) {
        options.returnNewestEntries = true;
    }

    adapter.log.debug(`getHistory call: ${JSON.stringify(options)}`);

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
        if (options.step !== null && options.step !== undefined) {
            options.step *= 1000;
        }
    }

    // if less 2000.01.01 00:00:00
    if (options.end < 946681200000) {
        options.end *= 1000;
    }

    if ((!options.start && options.count) || options.aggregate === 'onchange' || options.aggregate === '' || options.aggregate === 'none') {
        getCachedData(options, (cacheData, isFull) => {
            adapter.log.debug(`after getCachedData: length = ${cacheData.length}, isFull=${isFull}`);

            cacheData = applyOptions(cacheData, options, true);

            // if all data read
            if (isFull && cacheData.length) {
                cacheData = cacheData.sort(sortByTs);
                if (options.count && cacheData.length > options.count && options.aggregate === 'none') {
                    cacheData = cacheData.slice(-options.count);
                    adapter.log.debug(`cut cacheData to ${options.count} values`);
                }
                adapter.log.debug(`Send: ${cacheData.length} values in: ${Date.now() - startTime}ms`);

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
                    adapter.log.debug(`after getFileData: cacheData.length = ${cacheData.length}, fileData.length = ${fileData.length}`);
                    cacheData = cacheData.concat(fileData);
                    cacheData = cacheData.sort(sortByTs);
                    options.count = origCount;
                    options.result = cacheData;
                    if (options.count && options.result.length > options.count && options.aggregate === 'none' && !options.returnNewestEntries) {
                        if (options.start) {
                            for (let i = 0; i < options.result.length; i++) {
                                if (options.result[i].ts < options.start) {
                                    options.result.splice(i, 1);
                                    i--;
                                } else {
                                    break;
                                }
                            }
                        }
                        options.result = options.result.slice(0, options.count);
                        adapter.log.debug(`pre-cut data to ${options.count} oldest values`);
                    }
                    Aggregate.beautify(options);

                    adapter.log.debug(`after beautify: options.result.length = ${options.result.length}`);

                    adapter.log.debug(`Send: ${options.result.length} values in: ${Date.now() - startTime}ms`);

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
            adapter.log.debug('use parallel requests for getHistory');
            try {
                const gh = cp.fork(__dirname + '/lib/getHistory.js', [JSON.stringify(options)], {silent: false});

                let ghTimeout = setTimeout(() => {
                    try {
                        gh.kill('SIGINT');
                    } catch (err) {
                        adapter.log.error(err.message);
                    }
                }, 120000);

                gh.on('error', err => {
                    adapter.log.info('Error communicating to forked process: ' + err.message);
                    adapter.sendTo(msg.from, msg.command, {
                        result: [],
                        step: null,
                        error: null
                    }, msg.callback);
                });

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

                        options.result = applyOptions(data[1], options, true);
                        const overallLength = data[2];
                        const step = data[3];
                        if (options.result) {
                            Aggregate.beautify(options);
                            adapter.log.debug(`Send: ${options.result.length} of: ${overallLength} in: ${Date.now() - startTime}ms`);
                            adapter.sendTo(msg.from, msg.command, {
                                result: options.result,
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
                adapter.log.info(`Can not use parallel requests: ${err.message}`);
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
                        options.result = applyOptions(data[1], options, true);
                        Aggregate.beautify(options);
                        adapter.sendTo(msg.from, msg.command, {
                            result: options.result,
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
        adapter.log.warn(`Data directory ${path} does not exist`);
        return [];
    }
    try {
        return fs.readdirSync(path).filter(file => {
            try {
                return fs.statSync(path + '/' + file).isDirectory()
            } catch (e) {
                // ignore entry
                return false;
            }
        });
    } catch (err) {
        // ignore
        adapter.log.warn(`Error reading data directory ${path}: ${err}`);
        return [];
    }
}

function update(id, state) {
    // first try to find the value in not yet saved data
    let found = false;
    if (history[id]) {
        const res = history[id].list;
        if (res) {
            for (let i = res.length - 1; i >= 0; i--) {
                if (res[i].ts === state.ts) {
                    if (state.val !== undefined) {
                        res[i].val = state.val;
                    }
                    if (state.q !== undefined && res[i].q !== undefined) {
                        res[i].q = state.q;
                    }
                    if (state.from !== undefined && res[i].from !== undefined) {
                        res[i].from = state.from;
                    }
                    if (state.ack !== undefined) {
                        res[i].ack = state.ack;
                    }
                    found = true;
                    break;
                }
            }
        }
    }

    if (!found) {
        const day = GetHistory.ts2day(state.ts);
        if (!isNaN(day) && day > 20100101) {
            const file = GetHistory.getFilenameForID(adapter.config.storeDir, day, id);

            if (fs.existsSync(file)) {
                try {
                    const res = JSON.parse(fs.readFileSync(file)).sort(tsSort);

                    for (let i = 0; i < res.length; i++) {
                        if (res[i].ts === state.ts) {
                            if (state.val !== undefined) {
                                res[i].val = state.val;
                            }
                            if (state.q !== undefined && res[i].q !== undefined) {
                                res[i].q = state.q;
                            }
                            if (state.from !== undefined && res[i].from !== undefined) {
                                res[i].from = state.from;
                            }
                            if (state.ack !== undefined) {
                                res[i].ack = state.ack;
                            }
                            found = true;
                            break;
                        }
                    }
                    if (found) {
                        // save file
                        fs.writeFileSync(file, JSON.stringify(res, null, 2));
                    }
                } catch (error) {
                    adapter.log.error(`Cannot process file "${file}": ${error}`);
                }
            }
        }
    }

    return found;
}

function _delete(id, state) {
    // first try to find the value in not yet saved data
    let found = false;
    if (history[id]) {
        const res = history[id].list;
        if (res) {
            if (!state.ts && !state.start && !state.end) {
                history[id].list = [];
            } else {
                for (let i = res.length - 1; i >= 0; i--) {
                    if (state.start && state.end) {
                        if (res[i].ts >= state.start && res[i].ts <= state.end) {
                            res.splice(i, 1);
                        }
                    } else if (state.start) {
                        if (res[i].ts >= state.start) {
                            res.splice(i, 1);
                        }
                    } else if (state.end) {
                        if (res[i].ts <= state.end) {
                            res.splice(i, 1);
                        }
                    } else
                    if (res[i].ts === state.ts) {
                        res.splice(i, 1);
                        found = true;
                        break;
                    }
                }
            }
        }
    }

    if (!found) {
        const files = [];
        if (state.ts) {
            const day = GetHistory.ts2day(state.ts);
            if (!isNaN(day) && day > 20100101) {
                const file = GetHistory.getFilenameForID(adapter.config.storeDir, day, id);

                if (fs.existsSync(file)) {
                    files.push(file);
                }
            }
        } else {
            let dayStart;
            let dayEnd;
            if (state.start && state.end) {
                dayStart = parseInt(GetHistory.ts2day(state.start), 10);
                dayEnd = parseInt(GetHistory.ts2day(state.end), 10);
            } else if (state.start) {
                dayStart = parseInt(GetHistory.ts2day(state.start), 10);
                dayEnd   = parseInt(GetHistory.ts2day(Date.now()), 10);
            } else if (state.end) {
                dayStart = 0;
                dayEnd   = parseInt(GetHistory.ts2day(state.end), 10);
            } else {
                dayStart = 0;
                dayEnd   = parseInt(GetHistory.ts2day(Date.now()), 10);
            }

            const dayList = getDirectories(adapter.config.storeDir).sort((a, b) => b - a);

            for (let i = 0; i < dayList.length; i++) {
                const day = parseInt(dayList[i], 10);

                if (!isNaN(day) && day > 20100101 && day >= dayStart && day <= dayEnd) {
                    const file = GetHistory.getFilenameForID(adapter.config.storeDir, dayList[i], id);
                    if (fs.existsSync(file)) {
                        files.push(file);
                    }
                }
            }
        }

        files.forEach(file => {
            try {
                let res = JSON.parse(fs.readFileSync(file)).sort(tsSort);

                if (!state.ts && !state.start && !state.end) {
                    res = [];
                    found = true;
                } else {
                    for (let i = res.length - 1; i => 0; i--) {
                        if (state.start && state.end) {
                            if (res[i].ts >= state.start && res[i].ts <= state.end) {
                                res.splice(i, 1);
                                found = true;
                            }
                        } else if (state.start) {
                            if (res[i].ts >= state.start) {
                                res.splice(i, 1);
                                found = true;
                            }
                        } else if (state.end) {
                            if (res[i].ts <= state.end) {
                                res.splice(i, 1);
                                found = true;
                            }
                        } else if (res[i].ts === state.ts) {
                            res.splice(i, 1);
                            found = true;
                            break;
                        }
                    }
                }

                if (found) {
                    // save file
                    if (res.length) {
                        fs.writeFileSync(file, JSON.stringify(res, null, 2));
                    } else {
                        // delete file if no data
                        fs.unlinkSync(file);
                    }
                }
            } catch (error) {
                adapter.log.error(`Cannot process file "${file}": ${error}`);
            }
        });
    }

    return found;
}

function updateState(msg) {
    if (!msg.message) {
        adapter.log.error('updateState called with invalid data');
        return adapter.sendTo(msg.from, msg.command, {error: `Invalid call: ${JSON.stringify(msg)}`}, msg.callback);
    }

    let id;
    let success = true;
    if (Array.isArray(msg.message)) {
        adapter.log.debug(`updateState ${msg.message.length} items`);
        for (let i = 0; i < msg.message.length; i++) {
            id = aliasMap[msg.message[i].id] ? aliasMap[msg.message[i].id] : msg.message[i].id;

            if (msg.message[i].state && typeof msg.message[i].state === 'object') {
                update(id, msg.message[i].state);
            } else {
                adapter.log.warn(`Invalid state for ${JSON.stringify(msg.message[i])}`);
            }
        }
    } else if (msg.message.state && Array.isArray(msg.message.state)) {
        adapter.log.debug(`updateState ${msg.message.state.length} items`);
        id = aliasMap[msg.message.id] ? aliasMap[msg.message.id] : msg.message.id;
        for (let j = 0; j < msg.message.state.length; j++) {
            if (msg.message.state[j] && typeof msg.message.state[j] === 'object') {
                update(id, msg.message.state[j]);
            } else {
                adapter.log.warn(`Invalid state for ${JSON.stringify(msg.message.state[j])}`);
            }
        }
    } else if (msg.message.id && msg.message.state && typeof msg.message.state === 'object') {
        adapter.log.debug('updateState 1 item');
        id = aliasMap[msg.message.id] ? aliasMap[msg.message.id] : msg.message.id;
        success = update(id, msg.message.state);
    } else {
        adapter.log.error('updateState called with invalid data');
        return adapter.sendTo(msg.from, msg.command, {error: `Invalid call: ${JSON.stringify(msg)}`}, msg.callback);
    }

    adapter.sendTo(msg.from, msg.command, {success}, msg.callback);
}

function deleteState(msg) {
    if (!msg.message) {
        adapter.log.error('deleteState called with invalid data');
        return adapter.sendTo(msg.from, msg.command, {error: `Invalid call: ${JSON.stringify(msg)}`}, msg.callback);
    }

    let id;
    let success = true;
    if (Array.isArray(msg.message)) {
        adapter.log.debug(`deleteState ${msg.message.length} items`);
        for (let i = 0; i < msg.message.length; i++) {
            id = aliasMap[msg.message[i].id] ? aliasMap[msg.message[i].id] : msg.message[i].id;

            // {id: 'blabla', ts: 892}
            if (msg.message[i].ts) {
                _delete(id, {ts: msg.message[i].ts});
            } else
            if (msg.message[i].start) {
                if (typeof msg.message[i].start === 'string') {
                    msg.message[i].start = new Date(msg.message[i].start).getTime();
                }
                if (typeof msg.message[i].end === 'string') {
                    msg.message[i].end = new Date(msg.message[i].end).getTime();
                }
                _delete(id, {start: msg.message[i].start, end: msg.message[i].end || Date.now()});
            } else
            if (typeof msg.message[i].state === 'object' && msg.message[i].state && msg.message[i].state.ts) {
                _delete(id, {ts: msg.message[i].state.ts});
            } else
            if (typeof msg.message[i].state === 'object' && msg.message[i].state && msg.message[i].state.start) {
                if (typeof msg.message[i].state.start === 'string') {
                    msg.message[i].state.start = new Date(msg.message[i].state.start).getTime();
                }
                if (typeof msg.message[i].state.end === 'string') {
                    msg.message[i].state.end = new Date(msg.message[i].state.end).getTime();
                }
                _delete(id, {start: msg.message[i].state.start, end: msg.message[i].state.end || Date.now()});
            } else {
                adapter.log.warn(`Invalid state for ${JSON.stringify(msg.message[i])}`);
            }
        }
    } else if (msg.message.state && Array.isArray(msg.message.state)) {
        adapter.log.debug(`deleteState ${msg.message.state.length} items`);
        id = aliasMap[msg.message.id] ? aliasMap[msg.message.id] : msg.message.id;

        for (let j = 0; j < msg.message.state.length; j++) {
            if (msg.message.state[j] && typeof msg.message.state[j] === 'object') {
                if (msg.message.state[j].ts) {
                    _delete(id, {ts: msg.message.state[j].ts});
                } else if (msg.message.state[j].start) {
                    if (typeof msg.message.state[j].start === 'string') {
                        msg.message.state[j].start = new Date(msg.message.state[j].start).getTime();
                    }
                    if (typeof msg.message.state[j].end === 'string') {
                        msg.message.state[j].end = new Date(msg.message.state[j].end).getTime();
                    }
                    _delete(id, {start: msg.message.state[j].start, end: msg.message.state[j].end || Date.now()});
                }
            } else if (msg.message.state[j] && typeof msg.message.state[j] === 'number') {
                _delete(id, {ts: msg.message.state[j]});
            } else {
                adapter.log.warn(`Invalid state for ${JSON.stringify(msg.message.state[j])}`);
            }
        }
    } else if (msg.message.ts && Array.isArray(msg.message.ts)) {
        adapter.log.debug(`deleteState ${msg.message.ts.length} items`);
        id = aliasMap[msg.message.id] ? aliasMap[msg.message.id] : msg.message.id;
        for (let j = 0; j < msg.message.ts.length; j++) {
            if (msg.message.ts[j] && typeof msg.message.ts[j] === 'number') {
                _delete(id, {ts: msg.message.ts[j]});
            } else {
                adapter.log.warn(`Invalid state for ${JSON.stringify(msg.message.ts[j])}`);
            }
        }
    } else if (msg.message.id && msg.message.state && typeof msg.message.state === 'object') {
        adapter.log.debug('deleteState 1 item');
        id = aliasMap[msg.message.id] ? aliasMap[msg.message.id] : msg.message.id;
        success = _delete(id, {ts: msg.message.state.ts});
    } else if (msg.message.id && msg.message.ts && typeof msg.message.ts === 'number') {
        adapter.log.debug('deleteState 1 item');
        id = aliasMap[msg.message.id] ? aliasMap[msg.message.id] : msg.message.id;
        success = _delete(id, {ts: msg.message.ts});
    } else {
        adapter.log.error('deleteState called with invalid data');
        return adapter.sendTo(msg.from, msg.command, {error: `Invalid call: ${JSON.stringify(msg)}`}, msg.callback);
    }

    adapter.sendTo(msg.from, msg.command, {success}, msg.callback);
}

function deleteStateAll(msg) {
    if (!msg.message) {
        adapter.log.error('deleteState called with invalid data');
        return adapter.sendTo(msg.from, msg.command, {error: `Invalid call: ${JSON.stringify(msg)}`}, msg.callback);
    }

    let id;
    if (Array.isArray(msg.message)) {
        adapter.log.debug(`deleteStateAll ${msg.message.length} items`);
        for (let i = 0; i < msg.message.length; i++) {
            id = aliasMap[msg.message[i].id] ? aliasMap[msg.message[i].id] : msg.message[i].id;
            _delete(id, {});
        }
    } else if (msg.message.id) {
        adapter.log.debug('deleteStateAll 1 item');
        id = aliasMap[msg.message.id] ? aliasMap[msg.message.id] : msg.message.id;
        _delete(id, {});
    } else {
        adapter.log.error('deleteStateAll called with invalid data');
        return adapter.sendTo(msg.from, msg.command, {error: `Invalid call: ${JSON.stringify(msg)}`}, msg.callback);
    }

    adapter.sendTo(msg.from, msg.command, {success: true}, msg.callback);
}

function storeState(msg) {
    if (msg.message && (msg.message.success || msg.message.error)) { // Seems we got a callback from running converter
        return;
    }
    if (!msg.message || !msg.message.id || !msg.message.state) {
        adapter.log.error('storeState called with invalid data');
        return adapter.sendTo(msg.from, msg.command, {
            error:  'Invalid call'
        }, msg.callback);
    }
    let id;
    if (Array.isArray(msg.message)) {
        adapter.log.debug(`storeState: store ${msg.message.length} states for multiple ids`);
        for (let i = 0; i < msg.message.length; i++) {
            id = aliasMap[msg.message[i].id] ? aliasMap[msg.message[i].id] : msg.message[i].id;
            if (history[id]) {
                pushHelper(id, msg.message[i].state);
            } else {
                adapter.log.warn(`storeState: history not enabled for ${msg.message[i].id}. Ignoring`);
            }
        }
    } else if (msg.message.state && Array.isArray(msg.message.state)) {
        adapter.log.debug(`storeState: store ${msg.message.state.length} states for ${msg.message.id}`);
        for (let j = 0; j < msg.message.state.length; j++) {
            id = aliasMap[msg.message.id] ? aliasMap[msg.message.id] : msg.message.id;
            if (history[id]) {
                pushHelper(id, msg.message.state[j]);
            } else {
                adapter.log.warn(`storeState: history not enabled for ${msg.message.id}. Ignoring`);
            }
        }
    } else {
        adapter.log.debug(`storeState: store 1 state for ${msg.message.id}`);
        id = aliasMap[msg.message.id] ? aliasMap[msg.message.id] : msg.message.id;
        if (history[id]) {
            pushHelper(id, msg.message.state);
        } else {
            adapter.log.warn(`storeState: history not enabled for ${msg.message.id}. Ignoring`);
        }
    }

    adapter.sendTo(msg.from, msg.command, {success: true}, msg.callback);
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
                error: err
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
            error: 'Invalid call'
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
                error: err
            }, msg.callback);
        } else {
            adapter.log.info(JSON.stringify(obj));
            adapter.sendTo(msg.from, msg.command, {
                success: true
            }, msg.callback);
        }
    });
}

function getEnabledDPs(msg) {
    const data = {};
    for (const id in history) {
        if (!history.hasOwnProperty(id)) {
            continue;
        }
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
