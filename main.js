const cp = require('node:child_process');
const { Adapter, getAbsoluteDefaultDataDir } = require('@iobroker/adapter-core'); // Get common adapter utils
const dataDir = getAbsoluteDefaultDataDir();
const fs = require('node:fs');
const GetHistory = require('./lib/getHistory');
const Aggregate = require('./lib/aggregate');

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

function sortByTs(a, b) {
    const aTs = a.ts;
    const bTs = b.ts;
    return aTs < bTs ? -1 : aTs > bTs ? 1 : 0;
}

function tsSort(a, b) {
    return b.ts - a.ts;
}

class HistoryAdapter extends Adapter {
    history = {};
    aliasMap = {};
    subscribeAll = false;
    bufferChecker = null;
    tasksStart = [];
    finished = false;

    constructor(options) {
        super({
            ...options,
            name: 'history',
            objectChange: (id, obj) => {
                const formerAliasId = this.aliasMap[id] ? this.aliasMap[id] : id;

                const customConfig = obj?.common?.custom?.[this.namespace];

                if (customConfig && typeof customConfig === 'object' && customConfig.enabled) {
                    const realId = id;
                    let checkForRemove = true;
                    if (customConfig.aliasId) {
                        if (customConfig.aliasId !== id) {
                            this.aliasMap[id] = customConfig.aliasId;
                            this.log.debug(`Registered Alias: ${id} --> ${this.aliasMap[id]}`);
                            id = this.aliasMap[id];
                            checkForRemove = false;
                        } else {
                            this.log.warn(`Ignoring Alias-ID because identical to ID for ${id}`);
                            customConfig.aliasId = '';
                        }
                    }
                    if (checkForRemove && this.aliasMap[id]) {
                        this.log.debug(`Removed Alias: ${id} !-> ${this.aliasMap[id]}`);
                        delete this.aliasMap[id];
                    }

                    const doWriteNull = !this.history[id]?.config;
                    const state = this.history[id] ? this.history[id].state : null;
                    const list = this.history[id] ? this.history[id].list : null;
                    const timeout = this.history[id] ? this.history[id].timeout : null;

                    if (!this.history[formerAliasId]?.config && !this.subscribeAll) {
                        // unsubscribe
                        for (const _id in this.history) {
                            if (
                                Object.prototype.hasOwnProperty.call(this.history, _id) &&
                                Object.prototype.hasOwnProperty.call(this.history, this.history[_id].realId)
                            ) {
                                this.unsubscribeForeignStates(this.history[_id].realId);
                            }
                        }
                        this.subscribeAll = true;
                        this.subscribeForeignStates('*');
                    }

                    this.parseConfig(customConfig);

                    if (
                        this.history[formerAliasId]?.config &&
                        isEqual(customConfig, this.history[formerAliasId].config)
                    ) {
                        return this.log.debug(`Object ${id} unchanged. Ignore`);
                    }

                    if (this.history[formerAliasId]?.relogTimeout) {
                        clearTimeout(this.history[formerAliasId].relogTimeout);
                        this.history[formerAliasId].relogTimeout = null;
                    }

                    this.history[id] = { config: customConfig };
                    this.history[id].state = state;
                    this.history[id].list = list || [];
                    this.history[id].timeout = timeout;
                    this.history[id].realId = realId;

                    if (this.history[id].config?.changesOnly && this.history[id].config.changesRelogInterval > 0) {
                        this.history[id].relogTimeout = setTimeout(
                            _id => this.reLogHelper(_id),
                            this.history[id].config.changesRelogInterval * 500 * Math.random() +
                                this.history[id].config.changesRelogInterval * 500,
                            id,
                        );
                    }

                    if (doWriteNull && this.config.writeNulls) {
                        this.writeNulls(id);
                    }

                    this.log.info(`enabled logging of ${id}, Alias=${id !== realId}, WriteNulls=${doWriteNull}`);
                } else {
                    if (this.aliasMap[id]) {
                        this.log.debug(`Removed Alias: ${id} !-> ${this.aliasMap[id]}`);
                        delete this.aliasMap[id];
                    }
                    id = formerAliasId;
                    if (this.history[id]) {
                        this.log.info(`disabled logging of ${id}`);
                        if (this.history[id].relogTimeout) {
                            clearTimeout(this.history[id].relogTimeout);
                            this.history[id].relogTimeout = null;
                        }
                        if (this.history[id].timeout) {
                            clearTimeout(this.history[id].timeout);
                            this.history[id].timeout = null;
                        }
                        this.storeCached(true, id);
                        delete this.history[id];
                    }
                }
            },

            stateChange: (id, state) => {
                id = this.aliasMap[id] ? this.aliasMap[id] : id;
                this.pushHistory(id, state);
            },

            unload: callback => this.finish(callback),

            ready: () => this.main(),

            message: obj => this.processMessage(obj),
        });
    }

    parseConfig(customConfig) {
        if (!customConfig.maxLength && customConfig.maxLength !== '0' && customConfig.maxLength !== 0) {
            customConfig.maxLength = parseInt(this.config.maxLength, 10) || 960;
        } else {
            customConfig.maxLength = parseInt(customConfig.maxLength, 10);
        }

        if (!customConfig.retention && customConfig.retention !== '0' && customConfig.retention !== 0) {
            customConfig.retention = parseInt(this.config.retention, 10) || 0;
        } else {
            customConfig.retention = parseInt(customConfig.retention, 10) || 0;
        }
        if (customConfig.retention === -1) {
            // customRetentionDuration
            if (
                customConfig.customRetentionDuration !== undefined &&
                customConfig.customRetentionDuration !== null &&
                customConfig.customRetentionDuration !== ''
            ) {
                customConfig.customRetentionDuration = parseInt(customConfig.customRetentionDuration, 10) || 0;
            } else {
                customConfig.customRetentionDuration = this.config.customRetentionDuration;
            }
            customConfig.retention = customConfig.customRetentionDuration * 24 * 60 * 60;
        }

        if (!customConfig.blockTime && customConfig.blockTime !== '0' && customConfig.blockTime !== 0) {
            if (!customConfig.debounce && customConfig.debounce !== '0' && customConfig.debounce !== 0) {
                customConfig.blockTime = parseInt(this.config.blockTime, 10) || 0;
            } else {
                customConfig.blockTime = parseInt(customConfig.debounce, 10) || 0;
            }
        } else {
            customConfig.blockTime = parseInt(customConfig.blockTime, 10) || 0;
        }
        if (!customConfig.debounceTime && customConfig.debounceTime !== '0' && customConfig.debounceTime !== 0) {
            customConfig.debounceTime = parseInt(this.config.debounceTime, 10) || 0;
        } else {
            customConfig.debounceTime = parseInt(customConfig.debounceTime, 10) || 0;
        }
        customConfig.changesOnly = customConfig.changesOnly === 'true' || customConfig.changesOnly === true;
        if (
            customConfig.changesRelogInterval !== undefined &&
            customConfig.changesRelogInterval !== null &&
            customConfig.changesRelogInterval !== ''
        ) {
            customConfig.changesRelogInterval = parseInt(customConfig.changesRelogInterval, 10) || 0;
        } else {
            customConfig.changesRelogInterval = this.config.changesRelogInterval;
        }
        if (
            customConfig.changesMinDelta !== undefined &&
            customConfig.changesMinDelta !== null &&
            customConfig.changesMinDelta !== ''
        ) {
            customConfig.changesMinDelta = parseFloat(customConfig.changesMinDelta.toString().replace(/,/g, '.')) || 0;
        } else {
            customConfig.changesMinDelta = this.config.changesMinDelta;
        }

        customConfig.ignoreZero = customConfig.ignoreZero === 'true' || customConfig.ignoreZero === true;

        if (
            customConfig.ignoreAboveNumber !== undefined &&
            customConfig.ignoreAboveNumber !== null &&
            customConfig.ignoreAboveNumber !== ''
        ) {
            customConfig.ignoreAboveNumber = parseFloat(customConfig.ignoreAboveNumber) || null;
        }
        if (
            customConfig.ignoreBelowNumber !== undefined &&
            customConfig.ignoreBelowNumber !== null &&
            customConfig.ignoreBelowNumber !== ''
        ) {
            customConfig.ignoreBelowNumber = parseFloat(customConfig.ignoreBelowNumber) || null;
        } else if (customConfig.ignoreBelowZero === 'true' || customConfig.ignoreBelowZero === true) {
            customConfig.ignoreBelowNumber = 0;
        }

        if (
            customConfig.disableSkippedValueLogging !== undefined &&
            customConfig.disableSkippedValueLogging !== null &&
            customConfig.disableSkippedValueLogging !== ''
        ) {
            customConfig.disableSkippedValueLogging =
                customConfig.disableSkippedValueLogging === 'true' || customConfig.disableSkippedValueLogging === true;
        } else {
            customConfig.disableSkippedValueLogging = this.config.disableSkippedValueLogging;
        }

        // round
        if (customConfig.round !== null && customConfig.round !== undefined && customConfig !== '') {
            customConfig.round = parseInt(customConfig, 10);
            if (!isFinite(customConfig.round) || customConfig.round < 0) {
                customConfig.round = this.config.round;
            } else {
                customConfig.round = Math.pow(10, parseInt(customConfig.round, 10));
            }
        } else {
            customConfig.round = this.config.round;
        }

        if (
            customConfig.enableDebugLogs !== undefined &&
            customConfig.enableDebugLogs !== null &&
            customConfig.enableDebugLogs !== ''
        ) {
            customConfig.enableDebugLogs =
                customConfig.enableDebugLogs === 'true' || customConfig.enableDebugLogs === true;
        } else {
            customConfig.enableDebugLogs = this.config.enableDebugLogs;
        }

        // add one day if retention is too small
        if (customConfig.retention && customConfig.retention <= 604800) {
            customConfig.retention += 86400;
        }
    }

    storeCached(isFinishing, onlyId) {
        const now = Date.now();

        for (const id in this.history) {
            if (!Object.prototype.hasOwnProperty.call(this.history, id) || (onlyId !== undefined && onlyId !== id)) {
                continue;
            }

            this.history[id].list ||= [];
            if (isFinishing) {
                if (this.history[id].skipped && !this.history[id].config?.disableSkippedValueLogging) {
                    this.history[id].list.push(this.history[id].skipped);
                    this.history[id].skipped = null;
                }
                if (this.config.writeNulls) {
                    const nullValue = {
                        val: null,
                        ts: now,
                        lc: now,
                        q: 0x40,
                        from: `system.adapter.${this.namespace}`,
                    };
                    if (this.history[id].config?.changesOnly && this.history[id].state) {
                        const state = Object.assign({}, this.history[id].state);
                        state.ts = now;
                        state.from = `system.adapter.${this.namespace}`;
                        this.history[id].list.push(state);
                        nullValue.ts += 1;
                        nullValue.lc += 1;
                    }

                    // terminate values with null to indicate adapter stop.
                    this.history[id].list.push(nullValue);
                }
            }

            if (this.history[id].list && this.history[id].list.length) {
                this.log.debug(`Store the rest for ${id}`);
                this.appendFile(id, this.history[id].list);
            }
        }
    }

    finish(callback) {
        if (!this.subscribeAll) {
            for (const _id in this.history) {
                if (Object.prototype.hasOwnProperty.call(this.history, _id)) {
                    this.unsubscribeForeignStates(this.history[_id].realId);
                }
            }
        } else {
            this.unsubscribeForeignStates('*');
            this.subscribeAll = false;
        }
        if (this.bufferChecker) {
            clearInterval(this.bufferChecker);
            this.bufferChecker = null;
        }
        for (const id in this.history) {
            if (!Object.prototype.hasOwnProperty.call(this.history, id)) {
                continue;
            }

            if (this.history[id].relogTimeout) {
                clearTimeout(this.history[id].relogTimeout);
                this.history[id].relogTimeout = null;
            }
            if (this.history[id].timeout) {
                clearTimeout(this.history[id].timeout);
                this.history[id].timeout = null;
            }
        }

        if (!this.finished) {
            this.finished = true;
            this.storeCached(true);
        }

        callback?.();
    }

    processMessage(msg) {
        if (msg.command === 'features') {
            this.sendTo(
                msg.from,
                msg.command,
                { supportedFeatures: ['update', 'delete', 'deleteRange', 'deleteAll', 'storeState'] },
                msg.callback,
            );
        } else if (msg.command === 'update') {
            this.updateState(msg);
        } else if (msg.command === 'delete') {
            this.deleteState(msg);
        } else if (msg.command === 'deleteAll') {
            this.deleteStateAll(msg);
        } else if (msg.command === 'deleteRange') {
            this.deleteState(msg);
        } else if (msg.command === 'getHistory') {
            this.getHistory(msg);
        } else if (msg.command === 'storeState') {
            this.storeState(msg);
        } else if (msg.command === 'enableHistory') {
            this.enableHistory(msg);
        } else if (msg.command === 'disableHistory') {
            this.disableHistory(msg);
        } else if (msg.command === 'getEnabledDPs') {
            this.getEnabledDPs(msg);
        } else if (msg.command === 'stopInstance') {
            this.finish(() => {
                if (msg.callback) {
                    this.sendTo(msg.from, msg.command, 'stopped', msg.callback);
                    setTimeout(() => (this.terminate ? this.terminate(0) : process.exit(0)), 200);
                }
            });
        }
    }

    processStartValues() {
        if (this.tasksStart && this.tasksStart.length) {
            const task = this.tasksStart.shift();
            if (this.history[task.id]?.config?.changesOnly) {
                this.getForeignState(this.history[task.id].realId, (err, state) => {
                    const now = task.now || Date.now();
                    this.pushHistory(task.id, {
                        val: null,
                        ts: now,
                        ack: true,
                        q: 0x40,
                        from: `system.adapter.${this.namespace}`,
                    });

                    if (state) {
                        state.ts = now;
                        state.from = `system.adapter.${this.namespace}`;
                        this.pushHistory(task.id, state);
                    }
                    setImmediate(() => this.processStartValues());
                });
            } else {
                this.pushHistory(task.id, {
                    val: null,
                    ts: task.now || Date.now(),
                    ack: true,
                    q: 0x40,
                    from: `system.adapter.${this.namespace}`,
                });

                setImmediate(() => this.processStartValues());
            }
        }
    }

    writeNulls(id, now) {
        if (!id) {
            now = Date.now();
            for (const _id in this.history) {
                if (Object.prototype.hasOwnProperty.call(this.history, _id)) {
                    this.writeNulls(_id, now);
                }
            }
        } else {
            now ||= Date.now();
            this.tasksStart.push({ id, now });
            if (this.tasksStart.length === 1) {
                this.processStartValues();
            }
            if (this.history[id].config?.changesOnly && this.history[id].config.changesRelogInterval > 0) {
                if (this.history[id].relogTimeout) {
                    clearTimeout(this.history[id].relogTimeout);
                }
                this.history[id].relogTimeout = setTimeout(
                    _id => this.reLogHelper(_id),
                    this.history[id].config.changesRelogInterval * 500 * Math.random() +
                        this.history[id].config.changesRelogInterval * 500,
                    id,
                );
            }
        }
    }

    main() {
        //start
        // set default history if not yet set
        this.getForeignObject('system.config', (err, obj) => {
            if (obj?.common && !obj.common.defaultHistory) {
                obj.common.defaultHistory = this.namespace;
                this.setForeignObject('system.config', obj, err => {
                    if (err) {
                        this.log.error(`Cannot set default history instance: ${err}`);
                    } else {
                        this.log.info(`Set default history instance to "${this.namespace}"`);
                    }
                });
            }
        });

        this.config.storeDir ||= 'history';
        this.config.storeDir = this.config.storeDir.replace(/\\/g, '/');
        if (this.config.writeNulls === undefined) {
            this.config.writeNulls = true;
        }

        // remove last "/"
        if (this.config.storeDir[this.config.storeDir.length - 1] === '/') {
            this.config.storeDir = this.config.storeDir.substring(0, this.config.storeDir.length - 1);
        }

        if (this.config.storeDir[0] !== '/' && !this.config.storeDir.match(/^\w:\//)) {
            this.config.storeDir = dataDir + this.config.storeDir;
        }
        this.config.storeDir += '/';

        this.config.retention = parseInt(this.config.retention, 10) || 0;
        if (this.config.retention === -1) {
            // Custom timeframe
            this.config.retention = (parseInt(this.config.customRetentionDuration, 10) || 0) * 24 * 60 * 60;
        }

        if (this.config.changesRelogInterval !== null && this.config.changesRelogInterval !== undefined) {
            this.config.changesRelogInterval = parseInt(this.config.changesRelogInterval, 10);
        } else {
            this.config.changesRelogInterval = 0;
        }

        if (this.config.changesMinDelta !== null && this.config.changesMinDelta !== undefined) {
            this.config.changesMinDelta = parseFloat(this.config.changesMinDelta.toString().replace(/,/g, '.'));
        } else {
            this.config.changesMinDelta = 0;
        }

        if (this.config.blockTime !== null && this.config.blockTime !== undefined) {
            this.config.blockTime = parseInt(this.config.blockTime, 10) || 0;
        } else {
            if (this.config.debounce !== null && this.config.debounce !== undefined) {
                this.config.debounce = parseInt(this.config.debounce, 10) || 0;
            } else {
                this.config.blockTime = 0;
            }
        }

        if (this.config.debounceTime !== null && this.config.debounceTime !== undefined) {
            this.config.debounceTime = parseInt(this.config.debounceTime, 10) || 0;
        } else {
            this.config.debounceTime = 0;
        }

        if (this.config.round !== null && this.config.round !== undefined && this.config.round !== '') {
            this.config.round = parseInt(this.config.round, 10);
            if (!isFinite(this.config.round) || this.config.round < 0) {
                this.config.round = null;
                this.log.info(`Invalid round value: ${this.config.round} - ignore, do not round values`);
            } else {
                this.config.round = Math.pow(10, parseInt(this.config.round, 10));
            }
        } else {
            this.config.round = null;
        }

        try {
            // create directory
            if (!fs.existsSync(this.config.storeDir)) {
                fs.mkdirSync(this.config.storeDir);
            }
        } catch (err) {
            this.log.error(`Could not create Storage directory: ${err}`);
        }

        this.getObjectView('system', 'custom', {}, (err, doc) => {
            let count = 0;
            if (doc?.rows) {
                for (let i = 0, l = doc.rows.length; i < l; i++) {
                    if (doc.rows[i].value) {
                        let id = doc.rows[i].id;
                        const realId = id;
                        const customConfig = doc.rows[i].value[this.namespace];
                        if (customConfig?.aliasId) {
                            this.aliasMap[id] = doc.rows[i].value[this.namespace].aliasId;
                            this.log.debug(`Found Alias: ${id} --> ${this.aliasMap[id]}`);
                            id = this.aliasMap[id];
                        }

                        if (customConfig && typeof customConfig === 'object' && customConfig.enabled) {
                            count++;
                            this.log.info(`enabled logging of ${id} (Count=${count}), Alias=${id !== realId}`);
                            this.parseConfig(customConfig);

                            this.history[id] = { config: customConfig };
                            this.history[id].realId = realId;
                            this.history[id].list ||= [];
                        }
                    }
                }
            }
            if (count < 20) {
                for (const _id in this.history) {
                    if (Object.prototype.hasOwnProperty.call(this.history, _id)) {
                        this.subscribeForeignStates(this.history[_id].realId);
                    }
                }
            } else {
                this.subscribeAll = true;
                this.subscribeForeignStates('*');
            }

            if (this.config.writeNulls) {
                this.writeNulls();
            }

            // store all buffered data every 10 minutes to not lose the data
            this.bufferChecker = setInterval(() => this.storeCached(), 10 * 60000);
        });

        this.subscribeForeignObjects('*');
    }

    pushHistory(id, state, timerRelog) {
        if (timerRelog === undefined) {
            timerRelog = false;
        }
        // Push into history
        if (this.history[id]) {
            const settings = this.history[id].config;

            if (!settings || !state) {
                return;
            }

            if (state && state.val === undefined) {
                return this.log.warn(`state value undefined received for ${id} which is not allowed. Ignoring.`);
            }

            if (typeof state.val === 'string') {
                if (isFinite(state.val)) {
                    state.val = parseFloat(state.val);
                }
            }

            settings.enableDebugLogs &&
                this.log.debug(
                    `new value received for ${id}, new-value=${state.val}, ts=${state.ts}, relog=${timerRelog}`,
                );

            let ignoreDebounce = false;

            if (!timerRelog) {
                const valueUnstable = !!this.history[id].timeout;
                // When a debounce timer runs and the value is the same as the last one, ignore it
                if (this.history[id].timeout && state.ts !== state.lc) {
                    settings.enableDebugLogs &&
                        this.log.debug(
                            `value not changed debounce ${id}, value=${state.val}, ts=${state.ts}, debounce timer keeps running`,
                        );
                    return;
                } else if (this.history[id].timeout) {
                    // if value changed, clear timer
                    settings.enableDebugLogs &&
                        this.log.debug(
                            `value changed during debounce time ${id}, value=${state.val}, ts=${state.ts}, debounce timer restarted`,
                        );
                    clearTimeout(this.history[id].timeout);
                    this.history[id].timeout = null;
                }

                if (
                    !valueUnstable &&
                    settings.blockTime &&
                    this.history[id].state &&
                    this.history[id].state.ts + settings.blockTime > state.ts
                ) {
                    settings.enableDebugLogs &&
                        this.log.debug(
                            `value ignored blockTime ${id}, value=${state.val}, ts=${state.ts}, lastState.ts=${this.history[id].state.ts}, blockTime=${settings.blockTime}`,
                        );
                    return;
                }

                if (settings.ignoreZero && (state.val === undefined || state.val === null || state.val === 0)) {
                    if (settings.enableDebugLogs) {
                        this.log.debug(
                            `value ignore because zero or null ${id}, new-value=${state.val}, ts=${state.ts}`,
                        );
                    }
                    return;
                } else if (
                    typeof settings.ignoreBelowNumber === 'number' &&
                    typeof state.val === 'number' &&
                    state.val < settings.ignoreBelowNumber
                ) {
                    settings.enableDebugLogs &&
                        this.log.debug(
                            `value ignored because below ${settings.ignoreBelowNumber} for ${id}, new-value=${state.val}, ts=${state.ts}`,
                        );
                    return;
                }
                if (
                    typeof settings.ignoreAboveNumber === 'number' &&
                    typeof state.val === 'number' &&
                    state.val > settings.ignoreAboveNumber
                ) {
                    settings.enableDebugLogs &&
                        this.log.debug(
                            `value ignored because above ${settings.ignoreAboveNumber} for ${id}, new-value=${state.val}, ts=${state.ts}`,
                        );
                    return;
                }

                if (this.history[id].state && settings.changesOnly) {
                    if (settings.changesRelogInterval === 0) {
                        if ((this.history[id].state.val !== null || state.val === null) && state.ts !== state.lc) {
                            // remember new timestamp
                            if (!valueUnstable && !settings.disableSkippedValueLogging) {
                                this.history[id].skipped = state;
                            }
                            settings.enableDebugLogs &&
                                this.log.debug(
                                    `value not changed ${id}, last-value=${this.history[id].state.val}, new-value=${state.val}, ts=${state.ts}`,
                                );
                            return;
                        }
                    } else if (this.history[id].lastLogTime) {
                        if (
                            (this.history[id].state.val !== null || state.val === null) &&
                            state.ts !== state.lc &&
                            Math.abs(this.history[id].lastLogTime - state.ts) < settings.changesRelogInterval * 1000
                        ) {
                            // remember new timestamp
                            if (!valueUnstable && !settings.disableSkippedValueLogging) {
                                this.history[id].skipped = state;
                            }
                            settings.enableDebugLogs &&
                                this.log.debug(
                                    `value not changed ${id}, last-value=${this.history[id].state.val}, new-value=${state.val}, ts=${state.ts}`,
                                );
                            return;
                        }
                        if (state.ts !== state.lc) {
                            settings.enableDebugLogs &&
                                this.log.debug(
                                    `value-not-changed-relog ${id}, value=${state.val}, lastLogTime=${this.history[id].lastLogTime}, ts=${state.ts}`,
                                );
                            ignoreDebounce = true;
                        }
                    }
                    if (typeof state.val === 'number') {
                        if (
                            this.history[id].state.val !== null &&
                            settings.changesMinDelta !== 0 &&
                            Math.abs(this.history[id].state.val - state.val) < settings.changesMinDelta
                        ) {
                            if (!valueUnstable && !settings.disableSkippedValueLogging) {
                                this.history[id].skipped = state;
                            }
                            settings.enableDebugLogs &&
                                this.log.debug(
                                    `Min-Delta not reached ${id}, last-value=${this.history[id].state.val}, new-value=${state.val}, ts=${state.ts}`,
                                );
                            return;
                        } else if (settings.changesMinDelta !== 0) {
                            settings.enableDebugLogs &&
                                this.log.debug(
                                    `Min-Delta reached ${id}, last-value=${this.history[id].state.val}, new-value=${state.val}, ts=${state.ts}`,
                                );
                        }
                    } else {
                        settings.enableDebugLogs &&
                            this.log.debug(
                                `Min-Delta ignored because no number ${id}, last-value=${this.history[id].state.val}, new-value=${state.val}, ts=${state.ts}`,
                            );
                    }
                }
            }

            if (this.history[id].relogTimeout) {
                clearTimeout(this.history[id].relogTimeout);
                this.history[id].relogTimeout = null;
            }

            if (timerRelog) {
                state = Object.assign({}, state);
                state.ts = Date.now();
                state.from = `system.adapter.${this.namespace}`;
                settings.enableDebugLogs &&
                    this.log.debug(
                        `timed-relog ${id}, value=${state.val}, lastLogTime=${this.history[id].lastLogTime}, ts=${state.ts}`,
                    );
                ignoreDebounce = true;
            } else {
                if (settings.changesOnly && this.history[id].skipped) {
                    settings.enableDebugLogs &&
                        this.log.debug(
                            `Skipped value logged ${id}, value=${this.history[id].skipped.val}, ts=${this.history[id].skipped.ts}`,
                        );
                    this.pushHelper(id, this.history[id].skipped);
                    this.history[id].skipped = null;
                }
                if (
                    this.history[id].state &&
                    ((this.history[id].state.val === null && state.val !== null) ||
                        (this.history[id].state.val !== null && state.val === null))
                ) {
                    ignoreDebounce = true;
                } else if (!this.history[id].state && state.val === null) {
                    ignoreDebounce = true;
                }
            }
            if (settings.debounceTime && !ignoreDebounce && !timerRelog) {
                // Discard changes in de-bounce time to store last stable value
                this.history[id].timeout && clearTimeout(this.history[id].timeout);
                this.history[id].timeout = setTimeout(
                    (id, state) => {
                        this.history[id].timeout = null;
                        this.history[id].state = state;
                        this.history[id].lastLogTime = state.ts;
                        settings.enableDebugLogs &&
                            this.log.debug(
                                `Value logged ${id}, value=${this.history[id].state.val}, ts=${this.history[id].state.ts}`,
                            );
                        this.pushHelper(id);
                        if (settings.changesOnly && settings.changesRelogInterval > 0) {
                            this.history[id].relogTimeout = setTimeout(
                                _id => this.reLogHelper(_id),
                                settings.changesRelogInterval * 1000,
                                id,
                            );
                        }
                    },
                    settings.debounceTime,
                    id,
                    state,
                );
            } else {
                if (!timerRelog) {
                    this.history[id].state = state;
                }
                this.history[id].lastLogTime = state.ts;

                settings.enableDebugLogs &&
                    this.log.debug(
                        `Value logged ${id}, value=${this.history[id].state.val}, ts=${this.history[id].state.ts}`,
                    );
                this.pushHelper(id, state);
                if (settings.changesOnly && settings.changesRelogInterval > 0) {
                    this.history[id].relogTimeout = setTimeout(
                        _id => this.reLogHelper(_id),
                        settings.changesRelogInterval * 1000,
                        id,
                    );
                }
            }
        }
    }

    reLogHelper(_id) {
        if (!this.history[_id]) {
            this.log.info(`non-existing id ${_id}`);
            return;
        }

        this.history[_id].relogTimeout = null;

        if (this.history[_id].skipped) {
            this.pushHistory(_id, this.history[_id].skipped, true);
        } else if (this.history[_id].state) {
            this.pushHistory(_id, this.history[_id].state, true);
        } else {
            this.getForeignState(this.history[_id].realId, (err, state) => {
                if (err) {
                    this.log.info(`init timed Relog: can not get State for ${_id} : ${err}`);
                } else if (!state) {
                    this.log.info(
                        `init timed Relog: disable relog because state not set so far ${_id}: ${JSON.stringify(state)}`,
                    );
                } else if (this.history[_id]) {
                    this.log.debug(
                        `init timed Relog: getState ${_id}:  Value=${state.val}, ack=${state.ack}, ts=${state.ts}, lc=${state.lc}`,
                    );
                    this.history[_id].state = state;
                    this.pushHistory(_id, this.history[_id].state, true);
                }
            });
        }
    }

    pushHelper(_id, state) {
        if (!this.history[_id] || (!this.history[_id].state && !state)) {
            return;
        }
        if (!state) {
            state = this.history[_id].state;
        }

        // if it was not deleted in this time
        this.history[_id].list ||= [];

        if (typeof state.val === 'string') {
            if (isFinite(state.val)) {
                state.val = parseFloat(state.val);
            } else if (state.val === 'true') {
                state.val = true;
            } else if (state.val === 'false') {
                state.val = false;
            }
        }
        if (state.lc !== undefined) {
            delete state.lc;
        }
        if (!this.config.storeAck && state.ack !== undefined) {
            delete state.ack;
        } else {
            state.ack = state.ack ? 1 : 0;
        }
        if (!this.config.storeFrom && state.from !== undefined) {
            delete state.from;
        }

        this.history[_id].list.push(state);

        const _settings = (this.history[_id] && this.history[_id].config) || {};
        const maxLength =
            _settings.maxLength !== undefined ? _settings.maxLength : parseInt(this.config.maxLength, 10) || 960;
        if (_settings && this.history[_id].list.length > maxLength) {
            _settings.enableDebugLogs &&
                this.log.debug(`moving ${this.history[_id].list.length} entries from ${_id} to file`);
            this.appendFile(_id, this.history[_id].list);
        }
    }

    checkRetention(id) {
        if (this.history[id]?.config?.retention) {
            const d = new Date();
            const dt = d.getTime();
            // check every 6 hours
            if (!this.history[id].lastCheck || dt - this.history[id].lastCheck >= 21600000 /* 6 hours */) {
                this.history[id].lastCheck = dt;
                // get list of directories
                const dayList = this.getDirectories(this.config.storeDir).sort((a, b) => a - b);
                // calculate date
                d.setSeconds(-this.history[id].config.retention);

                const day = GetHistory.ts2day(d.getTime());

                for (let i = 0; i < dayList.length; i++) {
                    if (dayList[i] < day) {
                        const file = GetHistory.getFilenameForID(this.config.storeDir, dayList[i], id);
                        if (fs.existsSync(file)) {
                            this.log.info(`Delete old history "${file}"`);
                            try {
                                fs.unlinkSync(file);
                            } catch (ex) {
                                this.log.error(`Cannot delete file "${file}": ${ex}`);
                            }
                            let files;
                            try {
                                files = fs.readdirSync(this.config.storeDir + dayList[i]);
                            } catch {
                                files = [];
                            }
                            if (!files.length) {
                                this.log.info(`Delete old history dir "${this.config.storeDir}${dayList[i]}"`);
                                try {
                                    fs.rmdirSync(this.config.storeDir + dayList[i]);
                                } catch (ex) {
                                    this.log.error(
                                        `Cannot delete directory "${this.config.storeDir}${dayList[i]}": ${ex}`,
                                    );
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

    appendFile(id, states) {
        const day = GetHistory.ts2day(states[states.length - 1].ts);

        const file = GetHistory.getFilenameForID(this.config.storeDir, day, id);
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
                data = JSON.parse(fs.readFileSync(file, 'utf8')).concat(data);
            } catch (err) {
                this.log.error(`Cannot read file ${file}: ${err}`);
            }
        }

        try {
            // create directory
            if (!fs.existsSync(this.config.storeDir + day)) {
                fs.mkdirSync(this.config.storeDir + day);
            }
            fs.writeFileSync(file, JSON.stringify(data, null, 2), 'utf8');
        } catch (ex) {
            this.log.error(`Cannot store file ${file}: ${ex}`);
        }

        if (states.length) {
            this.appendFile(id, states);
        }

        this.checkRetention(id);
    }

    getOneCachedData(id, options, cache, addId) {
        addId ||= options.addId;

        if (this.history[id]) {
            const res = this.history[id].list;
            // todo can be optimized
            if (res) {
                let iProblemCount = 0;
                let vLast = null;
                for (let i = res.length - 1; i >= 0; i--) {
                    if (!res[i]) {
                        iProblemCount++;
                        continue;
                    }
                    const resEntry = Object.assign({}, res[i]);
                    if (typeof resEntry.val === 'number' && isFinite(resEntry.val) && options.round) {
                        resEntry.val = Math.round(resEntry.val * options.round) / options.round;
                    }
                    if (options.ack) {
                        resEntry.ack = !!resEntry.ack;
                    }
                    if (addId) {
                        resEntry.id = id;
                    }
                    if (options.start && resEntry.ts < options.start) {
                        // add one before start
                        cache.unshift(resEntry);
                        break;
                    } else if (resEntry.ts > options.end) {
                        // add one after end
                        vLast = resEntry;
                        continue;
                    }

                    if (vLast) {
                        cache.unshift(vLast);
                        vLast = null;
                    }

                    cache.unshift(resEntry);

                    if (
                        options.returnNewestEntries &&
                        cache.length >= options.count &&
                        (options.aggregate === 'onchange' || options.aggregate === '' || options.aggregate === 'none')
                    ) {
                        break;
                    }
                }

                iProblemCount &&
                    this.log.warn(`getOneCachedData: got null states ${iProblemCount} times for ${options.id}`);

                this.log.debug(`getOneCachedData: got ${res.length} datapoints for ${options.id}`);
            } else {
                this.log.debug(`getOneCachedData: datapoints for ${options.id} do not yet exist`);
            }
        }
    }

    getCachedData(options, callback) {
        const cache = [];

        if (options.id && options.id !== '*') {
            this.getOneCachedData(options.id, options, cache);
        } else {
            for (const id in this.history) {
                if (Object.prototype.hasOwnProperty.call(this.history, id)) {
                    this.getOneCachedData(id, options, cache, true);
                }
            }
        }

        options.length = cache.length;
        callback(cache, options.returnNewestEntries && cache.length >= options.count);
    }

    getOneFileData(dayList, dayStart, dayEnd, id, options, data, addId) {
        addId ||= options.addId;

        if (options.debugLog) {
            this.log.debug(`getOneFileData: ${dayStart} -> ${dayEnd} for ${id}`);
        }

        // get all files in directory
        for (let i = 0; i < dayList.length; i++) {
            const day = parseInt(dayList[i], 10);
            if (!isNaN(day) && day >= dayStart && day <= dayEnd) {
                const file = GetHistory.getFilenameForID(options.path, day, id);
                const tsCheck = new Date(Math.floor(day / 10000), 0, 1).getTime();

                options.debugLog && this.log.debug(`handleFileData: ${day} -> ${file}`);
                try {
                    if (fs.existsSync(file)) {
                        try {
                            let _data = JSON.parse(fs.readFileSync(file, 'utf-8')).sort(tsSort);
                            // adapter.log.debug(`_data = ${JSON.stringify(_data)}`);
                            let last = false;

                            for (const ii in _data) {
                                if (!Object.prototype.hasOwnProperty.call(_data, ii)) {
                                    continue;
                                }

                                // if a ts in seconds is in then convert on the fly
                                if (_data[ii].ts && _data[ii].ts < tsCheck) {
                                    _data[ii].ts *= 1000;
                                }

                                if (typeof _data[ii].val === 'number' && isFinite(_data[ii].val) && options.round) {
                                    _data[ii].val = Math.round(_data[ii].val * options.round) / options.round;
                                }
                                if (options.ack) {
                                    _data[ii].ack = !!_data[ii].ack;
                                }
                                if (addId) {
                                    _data[ii].id = id;
                                }
                                data.push(_data[ii]);
                                if (
                                    (options.returnNewestEntries ||
                                        options.aggregate === 'onchange' ||
                                        options.aggregate === '' ||
                                        options.aggregate === 'none') &&
                                    data.length >= options.count
                                ) {
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
                } catch (e) {
                    console.log(`Cannot read file ${file}: ${e.message}`);
                }
            }

            if (data.length >= options.count) {
                break;
            }
        }
    }

    getFileData(options, callback) {
        const dayStart = options.start ? parseInt(GetHistory.ts2day(options.start), 10) : 0;
        const dayEnd = parseInt(GetHistory.ts2day(options.end), 10);
        const fileData = [];

        // get list of directories
        let dayList = this.getDirectories(options.path);
        if (options.returnNewestEntries) {
            dayList = dayList.sort((a, b) => b - a);
        } else {
            dayList = dayList.sort((a, b) => a - b);
        }

        if (options.id && options.id !== '*') {
            this.getOneFileData(dayList, dayStart, dayEnd, options.id, options, fileData);
        } else {
            for (const id in this.history) {
                if (Object.prototype.hasOwnProperty.call(this.history, id)) {
                    this.getOneFileData(dayList, dayStart, dayEnd, id, options, fileData, true);
                }
            }
        }

        callback(fileData);
    }

    applyOptions(data, options) {
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
            if (!options.addId && item.id !== undefined) {
                delete item.id;
            }
            if (!options.user && item.user !== undefined) {
                delete item.user;
            }
            if (!options.comment && item.c !== undefined) {
                delete item.c;
            }
        });

        return data;
    }

    getHistory(msg) {
        const startTime = Date.now();

        if (!msg.message?.options) {
            return this.sendTo(
                msg.from,
                msg.command,
                {
                    error: 'Invalid call. No options for getHistory provided',
                },
                msg.callback,
            );
        }

        const options = {
            id: msg.message.id ? msg.message.id : null,
            path: this.config.storeDir,
            start: msg.message.options.start,
            end: msg.message.options.end || new Date().getTime() + 5000000,
            step: parseInt(msg.message.options.step, 10) || null,
            count: parseInt(msg.message.options.count, 10),
            from: msg.message.options.from || false,
            ack: msg.message.options.ack || false,
            q: msg.message.options.q || false,
            ignoreNull: msg.message.options.ignoreNull,
            aggregate: msg.message.options.aggregate || 'average', // One of: max, min, average, total
            limit:
                parseInt(msg.message.options.limit, 10) ||
                parseInt(msg.message.options.count, 10) ||
                this.config.limit ||
                2000,
            addId: msg.message.options.addId || false,
            sessionId: msg.message.options.sessionId,
            returnNewestEntries: msg.message.options.returnNewestEntries || false,
            percentile:
                msg.message.options.aggregate === 'percentile'
                    ? parseInt(msg.message.options.percentile, 10) || 50
                    : null,
            quantile:
                msg.message.options.aggregate === 'quantile' ? parseFloat(msg.message.options.quantile) || 0.5 : null,
            integralUnit:
                msg.message.options.aggregate === 'integral'
                    ? parseInt(msg.message.options.integralUnit, 10) || 60
                    : null,
            integralInterpolation:
                msg.message.options.aggregate === 'integral'
                    ? msg.message.options.integralInterpolation || 'none'
                    : null,
            removeBorderValues: msg.message.options.removeBorderValues || false,
            logId: `${msg.message.id ? msg.message.id : 'all'}${Date.now()}${Math.random()}`,
        };

        this.log.debug(`${options.logId} getHistory message: ${JSON.stringify(msg.message)}`);

        if (!options.count || isNaN(options.count)) {
            if (options.aggregate === 'none' || options.aggregate === 'onchange') {
                options.count = options.limit;
            } else {
                options.count = 500;
            }
        }

        if (
            msg.message.options.round !== null &&
            msg.message.options.round !== undefined &&
            msg.message.options.round !== ''
        ) {
            msg.message.options.round = parseInt(msg.message.options.round, 10);
            if (!isFinite(msg.message.options.round) || msg.message.options.round < 0) {
                options.round = this.config.round;
            } else {
                options.round = Math.pow(10, parseInt(msg.message.options.round, 10));
            }
        } else {
            options.round = this.config.round;
        }

        try {
            if (options.start && typeof options.start !== 'number') {
                options.start = new Date(options.start).getTime();
            }
        } catch {
            return this.sendTo(
                msg.from,
                msg.command,
                {
                    error: `Invalid call. Start date ${JSON.stringify(options.start)} is not a valid date`,
                },
                msg.callback,
            );
        }

        try {
            if (options.end && typeof options.end !== 'number') {
                options.end = new Date(options.end).getTime();
            }
        } catch {
            return this.sendTo(
                msg.from,
                msg.command,
                {
                    error: `Invalid call. End date ${JSON.stringify(options.end)} is not a valid date`,
                },
                msg.callback,
            );
        }

        if (!options.start && options.count) {
            options.returnNewestEntries = true;
        }

        if (options.id && this.aliasMap[options.id]) {
            options.id = this.aliasMap[options.id];
        }

        if (options.start > options.end) {
            const _end = options.end;
            options.end = options.start;
            options.start = _end;
        }

        if (!options.start && !options.count) {
            options.start = Date.now() - 86400000; // - 1 day
        }

        if ((options.aggregate === 'percentile' && options.percentile < 0) || options.percentile > 100) {
            this.log.error(`Invalid percentile value: ${options.percentile}, use 50 as default`);
            options.percentile = 50;
        }

        if ((options.aggregate === 'quantile' && options.quantile < 0) || options.quantile > 1) {
            this.log.error(`Invalid quantile value: ${options.quantile}, use 0.5 as default`);
            options.quantile = 0.5;
        }

        if (
            options.aggregate === 'integral' &&
            (typeof options.integralUnit !== 'number' || options.integralUnit <= 0)
        ) {
            this.log.error(`Invalid integralUnit value: ${options.integralUnit}, use 60s as default`);
            options.integralUnit = 60;
        }

        this.history[options.id] ||= {};
        const debugLog = (options.debugLog = !!(
            this.history[options.id]?.config?.enableDebugLogs || this.config.enableDebugLogs
        ));

        // include nulls and replace them with last value
        if (options.ignoreNull === 'true') {
            options.ignoreNull = true;
        }
        // include nulls
        if (options.ignoreNull === 'false') {
            options.ignoreNull = false;
        }
        // include nulls and replace them with 0
        if (options.ignoreNull === '0') {
            options.ignoreNull = 0;
        }
        if (options.ignoreNull !== true && options.ignoreNull !== false && options.ignoreNull !== 0) {
            options.ignoreNull = false;
        }

        if (debugLog) {
            this.log.debug(`${options.logId} getHistory options final: ${JSON.stringify(options)}`);
        }

        if (
            (!options.start && options.count) ||
            options.aggregate === 'onchange' ||
            options.aggregate === '' ||
            options.aggregate === 'none'
        ) {
            this.getCachedData(options, (cacheData, isFull) => {
                if (debugLog) {
                    this.log.debug(
                        `${options.logId} after getCachedData: length = ${cacheData.length}, isFull=${isFull}`,
                    );
                }

                cacheData = this.applyOptions(cacheData, options);

                // if all data read
                if (isFull && cacheData.length) {
                    cacheData = cacheData.sort(sortByTs);
                    if (options.count && cacheData.length > options.count && options.aggregate === 'none') {
                        cacheData.splice(0, cacheData.length - options.count);
                        debugLog && this.log.debug(`${options.logId} cut cacheData to ${options.count} values`);
                    }
                    this.log.debug(`${options.logId} Send: ${cacheData.length} values in: ${Date.now() - startTime}ms`);

                    this.sendTo(
                        msg.from,
                        msg.command,
                        {
                            result: cacheData,
                            step: null,
                            error: null,
                        },
                        msg.callback,
                    );
                } else {
                    const origCount = options.count;
                    if (options.returnNewestEntries) {
                        options.count -= cacheData.length;
                    }
                    this.getFileData(options, fileData => {
                        if (debugLog) {
                            this.log.debug(
                                `${options.logId} after getFileData: cacheData.length = ${cacheData.length}, fileData.length = ${fileData.length}`,
                            );
                        }
                        options.count = origCount;
                        fileData = this.applyOptions(fileData, options);
                        cacheData = cacheData.concat(fileData);
                        cacheData = cacheData.sort(sortByTs);
                        options.result = cacheData;
                        if (
                            options.count &&
                            options.result.length > options.count &&
                            options.aggregate === 'none' &&
                            !options.returnNewestEntries
                        ) {
                            let cutPoint = 0;
                            if (options.start) {
                                for (let i = 0; i < options.result.length; i++) {
                                    if (options.result[i].ts >= options.start) {
                                        cutPoint = i;
                                        break;
                                    }
                                }
                            }
                            cutPoint > 0 && options.result.splice(0, cutPoint);
                            options.result.length = options.count;
                            if (debugLog) {
                                this.log.debug(`${options.logId} pre-cut data to ${options.count} oldest values`);
                            }
                        }
                        if (options.debugLog) {
                            options.log = this.log.debug;
                        }
                        Aggregate.beautify(options);

                        if (debugLog) {
                            this.log.debug(
                                `${options.logId} after beautify: options.result.length = ${options.result.length}`,
                            );
                        }

                        this.log.debug(
                            `${options.logId} Send: ${options.result.length} values in: ${Date.now() - startTime}ms`,
                        );

                        this.sendTo(
                            msg.from,
                            msg.command,
                            {
                                result: options.result,
                                step: null,
                                error: null,
                            },
                            msg.callback,
                        );
                    });
                }
            });
        } else {
            // to use parallel requests, activate this.
            let responseSent = false;
            this.log.debug(`${options.logId} use parallel requests for getHistory`);
            try {
                let gh = cp.fork(`${__dirname}/lib/getHistory.js`, [JSON.stringify(options)], { silent: false });

                let ghTimeout = setTimeout(() => {
                    try {
                        gh.kill('SIGINT');
                    } catch (err) {
                        this.log.error(err.message);
                    }
                    gh = null;
                }, 120000);

                gh.on('error', err => {
                    gh = null;
                    if (!responseSent) {
                        this.log.info(`${options.logId} Error communicating to forked process: ${err.message}`);
                        this.sendTo(
                            msg.from,
                            msg.command,
                            {
                                result: [],
                                step: null,
                                error: null,
                            },
                            msg.callback,
                        );
                    }
                    responseSent = true;
                });

                gh.on('message', data => {
                    const cmd = data[0];
                    if (cmd === 'getCache') {
                        const settings = data[1];
                        this.getCachedData(settings, cacheData => {
                            try {
                                gh.send(['cacheData', cacheData]);
                            } catch (err) {
                                this.log.info(`${options.logId} Can not send data to forked process: ${err.message}`);
                            }
                        });
                    } else if (cmd === 'response') {
                        clearTimeout(ghTimeout);
                        ghTimeout = null;

                        try {
                            gh.send(['exit']);
                        } catch (err) {
                            this.log.info(`${options.logId} Can not exit forked process: ${err.message}`);
                        }
                        gh = null;

                        options.result = this.applyOptions(data[1], options);
                        const overallLength = data[2];
                        const step = data[3];
                        if (options.result) {
                            !responseSent &&
                                this.log.debug(
                                    `${options.logId} Send: ${options.result.length} of: ${overallLength} in: ${Date.now() - startTime}ms`,
                                );
                            !responseSent &&
                                this.sendTo(
                                    msg.from,
                                    msg.command,
                                    {
                                        result: options.result,
                                        step: step,
                                        error: null,
                                    },
                                    msg.callback,
                                );
                            responseSent = true;
                            options.result = null;
                        } else {
                            !responseSent && this.log.info(`${options.logId} No Data`);
                            !responseSent &&
                                this.sendTo(
                                    msg.from,
                                    msg.command,
                                    {
                                        result: [],
                                        step: null,
                                        error: null,
                                    },
                                    msg.callback,
                                );
                            responseSent = true;
                        }
                    } else if (cmd === 'debug') {
                        let line = data.slice(1).join(', ');
                        if (line.includes(options.logId)) {
                            line = line.replace(`${options.logId} `, '');
                        }
                        this.log.debug(`${options.logId} GetHistory fork: ${line}`);
                    }
                });
            } catch (err) {
                this.log.info(`${options.logId} Can not use parallel requests: ${err.message}`);
            }
        }
    }

    getDirectories(path) {
        if (!fs.existsSync(path)) {
            this.log.warn(`Data directory ${path} does not exist`);
            return [];
        }
        try {
            return fs.readdirSync(path).filter(file => {
                try {
                    return !file.startsWith('.') && fs.statSync(`${path}/${file}`).isDirectory();
                } catch {
                    // ignore entry
                    return false;
                }
            });
        } catch (err) {
            // ignore
            this.log.warn(`Error reading data directory ${path}: ${err}`);
            return [];
        }
    }

    update(id, state) {
        // first try to find the value in not yet saved data
        let found = false;
        if (this.history[id]) {
            const res = this.history[id].list;
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
                        if (state.user !== undefined) {
                            res[i].user = state.user;
                        }
                        if (state.c !== undefined) {
                            res[i].c = state.c;
                        }
                        found = true;
                        break;
                    }
                }
            }
        }

        if (!found) {
            const day = GetHistory.ts2day(state.ts);
            if (!isNaN(day)) {
                const file = GetHistory.getFilenameForID(this.config.storeDir, day, id);
                const tsCheck = new Date(Math.floor(day / 10000), 0, 1).getTime();

                if (fs.existsSync(file)) {
                    try {
                        const res = JSON.parse(fs.readFileSync(file, 'utf8'));

                        for (let i = 0; i < res.length; i++) {
                            // if a ts in seconds is in then convert on the fly
                            if (res[i].ts && res[i].ts < tsCheck) {
                                res[i].ts *= 1000;
                            }
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
                                if (state.user !== undefined) {
                                    res[i].user = state.user;
                                }
                                if (state.c !== undefined) {
                                    res[i].c = state.c;
                                }
                                found = true;
                                break;
                            }
                        }
                        if (found) {
                            // save file
                            fs.writeFileSync(file, JSON.stringify(res, null, 2), 'utf8');
                        }
                    } catch (error) {
                        this.log.error(`Cannot process file "${file}": ${error}`);
                    }
                }
            }
        }

        return found;
    }

    _delete(id, state) {
        // first try to find the value in not yet saved data
        let found = false;
        if (this.history[id]) {
            const res = this.history[id].list;
            if (res) {
                if (!state.ts && !state.start && !state.end) {
                    this.history[id].list = [];
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
                        } else if (res[i].ts === state.ts) {
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
                if (!isNaN(day)) {
                    const file = GetHistory.getFilenameForID(this.config.storeDir, day, id);

                    if (fs.existsSync(file)) {
                        files.push({ file, day });
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
                    dayEnd = parseInt(GetHistory.ts2day(Date.now()), 10);
                } else if (state.end) {
                    dayStart = 0;
                    dayEnd = parseInt(GetHistory.ts2day(state.end), 10);
                } else {
                    dayStart = 0;
                    dayEnd = parseInt(GetHistory.ts2day(Date.now()), 10);
                }

                const dayList = this.getDirectories(this.config.storeDir).sort((a, b) => b - a);

                for (let i = 0; i < dayList.length; i++) {
                    const day = parseInt(dayList[i], 10);

                    if (!isNaN(day) && day >= dayStart && day <= dayEnd) {
                        const file = GetHistory.getFilenameForID(this.config.storeDir, dayList[i], id);
                        if (fs.existsSync(file)) {
                            files.push({ file, day });
                        }
                    }
                }
            }

            files.forEach(entry => {
                try {
                    const tsCheck = new Date(Math.floor(entry.day / 10000), 0, 1).getTime();
                    let res = JSON.parse(fs.readFileSync(entry.file, 'utf8')).sort(tsSort);

                    if (!state.ts && !state.start && !state.end) {
                        res = [];
                        found = true;
                    } else {
                        for (let i = res.length - 1; i >= 0; i--) {
                            // if a ts in seconds is in then convert on the fly
                            if (res[i].ts && res[i].ts < tsCheck) {
                                res[i].ts *= 1000;
                            }
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
                            fs.writeFileSync(entry.file, JSON.stringify(res, null, 2), 'utf8');
                        } else {
                            // delete file if no data
                            fs.unlinkSync(entry.file);
                        }
                    }
                } catch (error) {
                    this.log.error(`Cannot process file "${entry.file}": ${error}`);
                }
            });
        }

        return found;
    }

    updateState(msg) {
        if (!msg.message) {
            this.log.error('updateState called with invalid data');
            return this.sendTo(msg.from, msg.command, { error: `Invalid call: ${JSON.stringify(msg)}` }, msg.callback);
        }

        let id;
        let success = true;
        if (Array.isArray(msg.message)) {
            this.log.debug(`updateState ${msg.message.length} items`);
            for (let i = 0; i < msg.message.length; i++) {
                id = this.aliasMap[msg.message[i].id] || msg.message[i].id;

                if (msg.message[i].state && typeof msg.message[i].state === 'object') {
                    this.update(id, msg.message[i].state);
                } else {
                    this.log.warn(`Invalid state for ${JSON.stringify(msg.message[i])}`);
                }
            }
        } else if (Array.isArray(msg.message.state)) {
            this.log.debug(`updateState ${msg.message.state.length} items`);
            id = this.aliasMap[msg.message.id] || msg.message.id;
            for (let j = 0; j < msg.message.state.length; j++) {
                if (msg.message.state[j] && typeof msg.message.state[j] === 'object') {
                    this.update(id, msg.message.state[j]);
                } else {
                    this.log.warn(`Invalid state for ${JSON.stringify(msg.message.state[j])}`);
                }
            }
        } else if (msg.message.id && msg.message.state && typeof msg.message.state === 'object') {
            this.log.debug('updateState 1 item');
            id = this.aliasMap[msg.message.id] || msg.message.id;
            success = this.update(id, msg.message.state);
        } else {
            this.log.error('updateState called with invalid data');
            return this.sendTo(msg.from, msg.command, { error: `Invalid call: ${JSON.stringify(msg)}` }, msg.callback);
        }

        this.sendTo(msg.from, msg.command, { success }, msg.callback);
    }

    deleteState(msg) {
        if (!msg.message) {
            this.log.error('deleteState called with invalid data');
            return this.sendTo(msg.from, msg.command, { error: `Invalid call: ${JSON.stringify(msg)}` }, msg.callback);
        }

        let id;
        let success = true;
        if (Array.isArray(msg.message)) {
            this.log.debug(`deleteState ${msg.message.length} items`);
            for (let i = 0; i < msg.message.length; i++) {
                id = this.aliasMap[msg.message[i].id] || msg.message[i].id;

                // {id: 'blabla', ts: 892}
                if (msg.message[i].ts) {
                    this._delete(id, { ts: msg.message[i].ts });
                } else if (msg.message[i].start) {
                    if (typeof msg.message[i].start === 'string') {
                        msg.message[i].start = new Date(msg.message[i].start).getTime();
                    }
                    if (typeof msg.message[i].end === 'string') {
                        msg.message[i].end = new Date(msg.message[i].end).getTime();
                    }
                    this._delete(id, { start: msg.message[i].start, end: msg.message[i].end || Date.now() });
                } else if (typeof msg.message[i].state === 'object' && msg.message[i].state?.ts) {
                    this._delete(id, { ts: msg.message[i].state.ts });
                } else if (typeof msg.message[i].state === 'object' && msg.message[i].state?.start) {
                    if (typeof msg.message[i].state.start === 'string') {
                        msg.message[i].state.start = new Date(msg.message[i].state.start).getTime();
                    }
                    if (typeof msg.message[i].state.end === 'string') {
                        msg.message[i].state.end = new Date(msg.message[i].state.end).getTime();
                    }
                    this._delete(id, {
                        start: msg.message[i].state.start,
                        end: msg.message[i].state.end || Date.now(),
                    });
                } else {
                    this.log.warn(`Invalid state for ${JSON.stringify(msg.message[i])}`);
                }
            }
        } else if (Array.isArray(msg.message.state)) {
            this.log.debug(`deleteState ${msg.message.state.length} items`);
            id = this.aliasMap[msg.message.id] || msg.message.id;

            for (let j = 0; j < msg.message.state.length; j++) {
                if (msg.message.state[j] && typeof msg.message.state[j] === 'object') {
                    if (msg.message.state[j].ts) {
                        this._delete(id, { ts: msg.message.state[j].ts });
                    } else if (msg.message.state[j].start) {
                        if (typeof msg.message.state[j].start === 'string') {
                            msg.message.state[j].start = new Date(msg.message.state[j].start).getTime();
                        }
                        if (typeof msg.message.state[j].end === 'string') {
                            msg.message.state[j].end = new Date(msg.message.state[j].end).getTime();
                        }
                        this._delete(id, {
                            start: msg.message.state[j].start,
                            end: msg.message.state[j].end || Date.now(),
                        });
                    }
                } else if (msg.message.state[j] && typeof msg.message.state[j] === 'number') {
                    this._delete(id, { ts: msg.message.state[j] });
                } else {
                    this.log.warn(`Invalid state for ${JSON.stringify(msg.message.state[j])}`);
                }
            }
        } else if (msg.message.ts && Array.isArray(msg.message.ts)) {
            this.log.debug(`deleteState ${msg.message.ts.length} items`);
            id = this.aliasMap[msg.message.id] || msg.message.id;
            for (let j = 0; j < msg.message.ts.length; j++) {
                if (msg.message.ts[j] && typeof msg.message.ts[j] === 'number') {
                    this._delete(id, { ts: msg.message.ts[j] });
                } else {
                    this.log.warn(`Invalid state for ${JSON.stringify(msg.message.ts[j])}`);
                }
            }
        } else if (msg.message.id && msg.message.state && typeof msg.message.state === 'object') {
            this.log.debug('deleteState 1 item');
            id = this.aliasMap[msg.message.id] || msg.message.id;
            success = this._delete(id, { ts: msg.message.state.ts });
        } else if (msg.message.id && msg.message.ts && typeof msg.message.ts === 'number') {
            this.log.debug('deleteState 1 item');
            id = this.aliasMap[msg.message.id] || msg.message.id;
            success = this._delete(id, { ts: msg.message.ts });
        } else {
            this.log.error('deleteState called with invalid data');
            return this.sendTo(msg.from, msg.command, { error: `Invalid call: ${JSON.stringify(msg)}` }, msg.callback);
        }

        this.sendTo(msg.from, msg.command, { success }, msg.callback);
    }

    deleteStateAll(msg) {
        if (!msg.message) {
            this.log.error('deleteState called with invalid data');
            return this.sendTo(msg.from, msg.command, { error: `Invalid call: ${JSON.stringify(msg)}` }, msg.callback);
        }

        let id;
        if (Array.isArray(msg.message)) {
            this.log.debug(`deleteStateAll ${msg.message.length} items`);
            for (let i = 0; i < msg.message.length; i++) {
                id = this.aliasMap[msg.message[i].id] || msg.message[i].id;
                this._delete(id, {});
            }
        } else if (msg.message.id) {
            this.log.debug('deleteStateAll 1 item');
            id = this.aliasMap[msg.message.id] || msg.message.id;
            this._delete(id, {});
        } else {
            this.log.error('deleteStateAll called with invalid data');
            return this.sendTo(msg.from, msg.command, { error: `Invalid call: ${JSON.stringify(msg)}` }, msg.callback);
        }

        this.sendTo(msg.from, msg.command, { success: true }, msg.callback);
    }

    storeStatePushData(id, state, applyRules) {
        if (!state || typeof state !== 'object') {
            throw new Error(`State ${JSON.stringify(state)} for ${id} is not valid`);
        }

        if (!this.history[id]?.config) {
            if (applyRules) {
                throw new Error(`history not enabled for ${id}, so can not apply the rules as requested`);
            }
            this.history[id] ||= {};
            this.history[id].realId = id;
        }
        if (applyRules) {
            this.pushHistory(id, state);
        } else {
            this.pushHelper(id, state);
        }
    }

    async storeState(msg) {
        if (msg.message && (msg.message.success || msg.message.error)) {
            // Seems we got a callback from running converter
            return;
        }
        if (!msg.message || !msg.message.id || !msg.message.state) {
            this.log.error('storeState called with invalid data');
            return this.sendTo(
                msg.from,
                msg.command,
                {
                    error: 'Invalid call',
                },
                msg.callback,
            );
        }

        let errors = [];
        let successCount = 0;
        if (Array.isArray(msg.message)) {
            this.log.debug(`storeState: store ${msg.message.length} states for multiple ids`);
            for (let i = 0; i < msg.message.length; i++) {
                const id = this.aliasMap[msg.message[i].id] || msg.message[i].id;
                try {
                    this.storeStatePushData(id, msg.message[i].state, msg.message.rules);
                    successCount++;
                } catch (err) {
                    errors.push(err.message);
                }
            }
        } else if (msg.message.id && Array.isArray(msg.message.state)) {
            this.log.debug(`storeState: store ${msg.message.state.length} states for ${msg.message.id}`);
            const id = this.aliasMap[msg.message.id] || msg.message.id;
            for (let j = 0; j < msg.message.state.length; j++) {
                try {
                    this.storeStatePushData(id, msg.message.state[j], msg.message.rules);
                    successCount++;
                } catch (err) {
                    errors.push(err.message);
                }
            }
        } else if (msg.message.id && msg.message.state) {
            this.log.debug(`storeState: store 1 state for ${msg.message.id}`);
            const id = this.aliasMap[msg.message.id] || msg.message.id;
            try {
                this.storeStatePushData(id, msg.message.state, msg.message.rules);
                successCount++;
            } catch (err) {
                errors.push(err.message);
            }
        } else {
            this.log.error('storeState called with invalid data');
            return this.sendTo(
                msg.from,
                msg.command,
                {
                    error: `Invalid call: ${JSON.stringify(msg)}`,
                },
                msg.callback,
            );
        }
        if (errors.length) {
            this.log.warn(`storeState executed with ${errors.length} errors: ${errors.join(', ')}`);
            return this.sendTo(
                msg.from,
                msg.command,
                {
                    error: `${errors.length} errors happened while storing data`,
                    errors: errors,
                    successCount,
                },
                msg.callback,
            );
        }

        this.log.debug(`storeState executed with ${successCount} states successfully`);
        this.sendTo(msg.from, msg.command, { success: true, successCount }, msg.callback);
    }

    enableHistory(msg) {
        if (!msg.message?.id) {
            this.log.error('enableHistory called with invalid data');
            this.sendTo(
                msg.from,
                msg.command,
                {
                    error: 'Invalid call',
                },
                msg.callback,
            );
            return;
        }
        const obj = {};
        obj.common = {};
        obj.common.custom = {};
        if (msg.message.options) {
            obj.common.custom[this.namespace] = msg.message.options;
        } else {
            obj.common.custom[this.namespace] = {};
        }
        obj.common.custom[this.namespace].enabled = true;
        this.extendForeignObject(msg.message.id, obj, err => {
            if (err) {
                this.log.error(`enableHistory: ${err}`);
                this.sendTo(
                    msg.from,
                    msg.command,
                    {
                        error: err,
                    },
                    msg.callback,
                );
            } else {
                this.log.info(JSON.stringify(obj));
                this.sendTo(
                    msg.from,
                    msg.command,
                    {
                        success: true,
                    },
                    msg.callback,
                );
            }
        });
    }

    disableHistory(msg) {
        if (!msg.message?.id) {
            this.log.error('disableHistory called with invalid data');
            this.sendTo(
                msg.from,
                msg.command,
                {
                    error: 'Invalid call',
                },
                msg.callback,
            );
            return;
        }
        const obj = {};
        obj.common = {};
        obj.common.custom = {};
        obj.common.custom[this.namespace] = {};
        obj.common.custom[this.namespace].enabled = false;
        this.extendForeignObject(msg.message.id, obj, err => {
            if (err) {
                this.log.error(`disableHistory: ${err}`);
                this.sendTo(
                    msg.from,
                    msg.command,
                    {
                        error: err,
                    },
                    msg.callback,
                );
            } else {
                this.log.info(JSON.stringify(obj));
                this.sendTo(
                    msg.from,
                    msg.command,
                    {
                        success: true,
                    },
                    msg.callback,
                );
            }
        });
    }

    getEnabledDPs(msg) {
        const data = {};
        for (const id in this.history) {
            if (Object.prototype.hasOwnProperty.call(this.history, id) && this.history[id]?.config?.enabled) {
                data[this.history[id].realId] = this.history[id].config;
            }
        }

        this.sendTo(msg.from, msg.command, data, msg.callback);
    }
}

// If started as allInOne/compact mode => return function to create instance
if (module && module.parent) {
    module.exports = options => new HistoryAdapter(options);
} else {
    // or start the instance directly
    (() => new HistoryAdapter())();
}
