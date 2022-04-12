/* jshint -W097 */// jshint strict:false
/*jslint node: true */
/*jshint expr: true*/
var expect = require('chai').expect;
var setup  = require(__dirname + '/lib/setup');

var objects = null;
var states  = null;
var onStateChanged = null;
//var onObjectChanged = null;
var sendToID = 1;

var adapterShortName = setup.adapterName.substring(setup.adapterName.indexOf('.')+1);

var now;

function checkConnectionOfAdapter(cb, counter) {
    counter = counter || 0;
    console.log('Try check #' + counter);
    if (counter > 30) {
        if (cb) cb('Cannot check connection');
        return;
    }

    console.log('Checking alive key for key : ' + adapterShortName);
    states.getState('system.adapter.' + adapterShortName + '.0.alive', function (err, state) {
        if (err) console.error(err);
        if (state && state.val) {
            if (cb) cb();
        } else {
            setTimeout(function () {
                checkConnectionOfAdapter(cb, counter + 1);
            }, 1000);
        }
    });
}
/*
function checkValueOfState(id, value, cb, counter) {
    counter = counter || 0;
    if (counter > 20) {
        if (cb) cb('Cannot check value Of State ' + id);
        return;
    }

    states.getState(id, function (err, state) {
        if (err) console.error(err);
        if (value === null && !state) {
            if (cb) cb();
        } else
        if (state && (value === undefined || state.val === value)) {
            if (cb) cb();
        } else {
            setTimeout(function () {
                checkValueOfState(id, value, cb, counter + 1);
            }, 500);
        }
    });
}
*/
function sendTo(target, command, message, callback) {
    onStateChanged = function (id, state) {
        if (id === 'messagebox.system.adapter.test.0') {
            callback(state.message);
        }
    };

    states.pushMessage('system.adapter.' + target, {
        command:    command,
        message:    message,
        from:       'system.adapter.test.0',
        callback: {
            message: message,
            id:      sendToID++,
            ack:     false,
            time:    (new Date()).getTime()
        }
    });
}

describe('Test ' + adapterShortName + ' adapter', function() {
    before('Test ' + adapterShortName + ' adapter: Start js-controller', function (_done) {
        this.timeout(600000); // because of first install from npm

        setup.setupController(async function () {
            var config = await setup.getAdapterConfig();
            // enable adapter
            config.common.enabled  = true;
            config.common.loglevel = 'debug';

            config.native.enableDebugLogs = true;
            //config.native.dbtype   = 'sqlite';

            await setup.setAdapterConfig(config.common, config.native);

            setup.startController(true, function(id, obj) {}, function (id, state) {
                    if (onStateChanged) onStateChanged(id, state);
                },
                async (_objects, _states) => {
                    objects = _objects;
                    states  = _states;
                    await objects.setObjectAsync('history.0.testValue', {
                        common: {
                            type: 'number',
                            role: 'state',
                            custom: {
                                "history.0": {
                                    enabled: true,
                                    changesOnly:  true,
                                    debounce:     0,
                                    retention:    31536000,
                                    maxLength:    3,
                                    changesMinDelta: 0.5
                                }
                            }
                        },
                        type: 'state'
                    });
                    await objects.setObjectAsync('history.0.testValueDebounce', {
                        common: {
                            type: 'number',
                            role: 'state',
                            custom: {
                                "history.0": {
                                    enabled: true,
                                    changesOnly:  true,
                                    changesRelogInterval: 10,
                                    debounceTime:     500,
                                    retention:    31536000,
                                    maxLength:    3,
                                    changesMinDelta: 0.5
                                }
                            }
                        },
                        type: 'state'
                    });
                    await objects.setObjectAsync('history.0.testValueDebounceRaw', {
                        common: {
                            type: 'number',
                            role: 'state',
                            custom: {
                                "history.0": {
                                    enabled: true,
                                    changesOnly:  true,
                                    changesRelogInterval: 10,
                                    debounceTime:     500,
                                    retention:    31536000,
                                    maxLength:    3,
                                    changesMinDelta: 0.5,
                                    disableSkippedValueLogging: true
                                }
                            }
                        },
                        type: 'state'
                    });
                    await objects.setObjectAsync('history.0.testValueBlocked', {
                        common: {
                            type: 'number',
                            role: 'state',
                            custom: {
                                "history.0": {
                                    enabled: true,
                                    changesOnly:  true,
                                    changesRelogInterval: 10,
                                    debounceTime:     0,
                                    blockTime:        1000,
                                    retention:        31536000,
                                    maxLength:        3,
                                    changesMinDelta:  0.5
                                }
                            }
                        },
                        type: 'state'
                    });
                    _done();
                });
        });
    });

    it('Test ' + adapterShortName + ' adapter: Check if adapter started', function (done) {
        this.timeout(60000);
        checkConnectionOfAdapter(function (res) {
            if (res) console.log(res);
            expect(res).not.to.be.equal('Cannot check connection');
            objects.setObject('system.adapter.test.0', {
                common: {

                },
                type: 'instance'
            },
            function () {
                states.subscribeMessage('system.adapter.test.0');
                objects.setObject('history.0.testValue2', {
                    common: {
                        type: 'number',
                        role: 'state'
                    },
                    type: 'state'
                },
                function () {
                    sendTo('history.0', 'enableHistory', {
                        id: 'history.0.testValue2',
                        options: {
                            changesOnly:  true,
                            debounce:     0,
                            retention:    31536000,
                            maxLength:    3,
                            changesMinDelta: 0.5,
                            aliasId: 'history.0.testValue2-alias'
                        }
                    }, function (result) {
                        expect(result.error).to.be.undefined;
                        expect(result.success).to.be.true;
                        // wait till adapter receives the new settings
                        setTimeout(function () {
                            done();
                        }, 2000);
                    });
                });
            });
        });
    });
    it('Test ' + adapterShortName + ': Check Enabled Points after Enable', function (done) {
        this.timeout(5000);

        sendTo('history.0', 'getEnabledDPs', {}, function (result) {
            console.log(JSON.stringify(result));
            expect(Object.keys(result).length).to.be.equal(5);
            expect(result['history.0.testValue'].enabled).to.be.true;
            done();
        });
    });
    it('Test ' + adapterShortName + ': Write values into DB', function (done) {
        this.timeout(25000);
        now = Date.now();

        states.setState('history.0.testValue', {val: 1, ts: now + 1000}, function (err) {
            if (err) {
                console.log(err);
            }
            setTimeout(function () {
                states.setState('history.0.testValue', {val: 2, ts: now + 10000}, function (err) {
                    if (err) {
                        console.log(err);
                    }
                    setTimeout(function () {
                        states.setState('history.0.testValue', {val: 2, ts: now + 13000}, function (err) {
                            if (err) {
                                console.log(err);
                            }
                            setTimeout(function () {
                                states.setState('history.0.testValue', {val: 2, ts: now + 15000}, function (err) {
                                    if (err) {
                                        console.log(err);
                                    }
                                    setTimeout(function () {
                                        states.setState('history.0.testValue', {val: 2.2, ts: now + 16000}, function (err) {
                                            if (err) {
                                                console.log(err);
                                            }
                                            setTimeout(function () {
                                                states.setState('history.0.testValue', {val: 2.5, ts: now + 17000}, function (err) {
                                                    if (err) {
                                                        console.log(err);
                                                    }
                                                    setTimeout(function () {
                                                        states.setState('history.0.testValue', {val: 3, ts: now + 19000}, function (err) {
                                                            if (err) {
                                                                console.log(err);
                                                            }
                                                            setTimeout(function () {
                                                                states.setState('history.0.testValue2', {val: 1, ts: now + 12000}, function (err) {
                                                                    if (err) {
                                                                        console.log(err);
                                                                    }
                                                                    setTimeout(function () {
                                                                        states.setState('history.0.testValue2', {val: 3, ts: now + 19000}, function (err) {
                                                                            if (err) {
                                                                                console.log(err);
                                                                            }
                                                                            done();
                                                                        });
                                                                    }, 100);
                                                                });
                                                            }, 100);
                                                        });
                                                    }, 100);
                                                });
                                            }, 100);
                                        });
                                    }, 100);
                                });
                            }, 100);
                        });
                    }, 100);
                });
            }, 100);
        });
    });

    it('Test ' + adapterShortName + ': Read values from DB using GetHistory', function (done) {
        this.timeout(25000);

        sendTo('history.0', 'getHistory', {
            id: 'history.0.testValue',
            options: {
                start:     now,
                end:       now + 30000,
                count:     50,
                aggregate: 'none'
            }
        }, function (result) {
            console.log(JSON.stringify(result.result, null, 2));
            expect(result.result.length).to.be.at.least(4);
            var found = 0;
            for (var i = 0; i < result.result.length; i++) {
                if (result.result[i].val >= 1 && result.result[i].val <= 3) found ++;
            }
            expect(found).to.be.equal(5); // additionally null value by start of adapter.

            sendTo('history.0', 'getHistory', {
                id: 'history.0.testValue',
                options: {
                    start:     now + 15000,
                    end:       now + 30000,
                    count:     2,
                    aggregate: 'none'
                }
            }, function (result) {
                console.log(JSON.stringify(result.result, null, 2));
                expect(result.result.length).to.be.equal(2);
                var found = 0;
                for (var i = 0; i < result.result.length; i++) {
                    if (result.result[i].val >= 1 && result.result[i].val <= 3) found ++;
                }
                expect(found).to.be.equal(2);
                expect(result.result[0].id).to.be.undefined;

                const latestTs = result.result[result.result.length - 1].ts;

                sendTo('history.0', 'getHistory', {
                    id: 'history.0.testValue',
                    options: {
                        start:     now + 15000,
                        end:       now + 30000,
                        count:     2,
                        aggregate: 'none',
                        addId: true,
                        returnNewestEntries: true
                    }
                }, function (result) {
                    console.log(JSON.stringify(result.result, null, 2));
                    expect(result.result.length).to.be.equal(2);
                    var found = 0;
                    for (var i = 0; i < result.result.length; i++) {
                        if (result.result[i].val >= 2.5 && result.result[i].val <= 3) found ++;
                    }
                    expect(found).to.be.equal(2);
                    expect(result.result[0].ts >= latestTs).to.be.true;
                    expect(result.result[0].id).to.be.equal('history.0.testValue');
                    done();
                });
            });
        });
    });

    it('Test ' + adapterShortName + ': Read average from DB using GetHistory', function (done) {
        this.timeout(25000);

        sendTo('history.0', 'getHistory', {
            id: 'history.0.testValue',
            options: {
                start:     now + 100,
                end:       now + 30001,
                count:     2,
                aggregate: 'average',
                ignoreNull: true,
                addId: true
            }
        }, function (result) {
            console.log(JSON.stringify(result.result, null, 2));
            expect(result.result.length).to.be.equal(4);
            expect(result.result[1].val).to.be.equal(1.5);
            expect(result.result[2].val).to.be.equal(2.57);
            expect(result.result[3].val).to.be.equal(2.57);
            expect(result.result[0].id).to.be.equal('history.0.testValue');
            done();
        });
    });

    it(`Test ${adapterShortName}: Read minmax values from DB using GetHistory`, function (done) {
        this.timeout(10000);

        sendTo('history.0', 'getHistory', {
            id: 'history.0.testValue',
            options: {
                start:     now - 30000,
                end:       now + 30000,
                count:     4,
                aggregate: 'minmax',
                addId: true
            }
        }, result => {
            console.log(JSON.stringify(result.result, null, 2));
            expect(result.result.length).to.be.at.least(4);
            expect(result.result[0].id).to.be.equal('history.0.testValue');
            done();
        });
    });

    it('Test ' + adapterShortName + ': Read values from DB using GetHistory for aliased testValue2', function (done) {
        this.timeout(25000);

        sendTo('history.0', 'getHistory', {
            id: 'history.0.testValue2',
            options: {
                start:     now,
                end:       now + 30000,
                count:     50,
                aggregate: 'none'
            }
        }, function (result) {
            console.log(JSON.stringify(result.result, null, 2));
            expect(result.result.length).to.be.equal(2);

            sendTo('history.0', 'getHistory', {
                id: 'history.0.testValue2-alias',
                options: {
                    start:     now,
                    end:       now + 30000,
                    count:     50,
                    aggregate: 'none'
                }
            }, function (result2) {
                console.log(JSON.stringify(result2.result, null, 2));
                expect(result2.result.length).to.be.equal(2);
                for (var i = 0; i < result2.result.length; i++) {
                    expect(result2.result[i].val).to.be.equal(result.result[i].val);
                }

                done();
            });
        });
    });

    function delay(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    async function logSampleData(stateId) {
        await states.setStateAsync(stateId, {val: 1}); // expect logged
        await delay(600);
        await states.setStateAsync(stateId, {val: 2}); // Expect not logged debounce
        await delay(20);
        await states.setStateAsync(stateId, {val: 2.1}); // Expect not logged debounce
        await delay(20);
        await states.setStateAsync(stateId, {val: 1.5}); // Expect not logged debounce
        await delay(20);
        await states.setStateAsync(stateId, {val: 2.3}); // Expect not logged debounce
        await delay(20);
        await states.setStateAsync(stateId, {val: 2.5}); // Expect not logged debounce
        await delay(600);
        await states.setStateAsync(stateId, {val: 2.9}); // Expect logged skipped
        await delay(600);
        await states.setStateAsync(stateId, {val: 3.0}); // Expect logged
        await delay(600);
        await states.setStateAsync(stateId, {val: 4}); // Expect logged
        await delay(600);
        await states.setStateAsync(stateId, {val: 4.4}); // expect logged skipped
        await delay(600);
        await states.setStateAsync(stateId, {val: 5});  // expect logged
        await delay(20);
        await states.setStateAsync(stateId, {val: 5});  // expect not logged debounce
        await delay(600);
        await states.setStateAsync(stateId, {val: 5});  // expect logged skipped
        await delay(600);
        await states.setStateAsync(stateId, {val: 6});  // expect logged
        await delay(10100);
        for (let i = 1; i < 10; i++) {
            await states.setStateAsync(stateId, {val: 6 + i * 0.05});  // expect logged skipped
            await delay(70);
        }
        await states.setStateAsync(stateId, {val: 7});  // expect logged
        await delay(13000);
    }

    it('Test ' + adapterShortName + ': Write debounced Raw values into DB', async function () {
        this.timeout(45000);
        now = Date.now();

        try {
            await logSampleData('history.0.testValueDebounceRaw');
        } catch (err) {
            console.log(err);
            expect(err).to.be.not.ok;
        }

        return new Promise(resolve => {
            sendTo('history.0', 'getHistory', {
                id: 'history.0.testValueDebounceRaw',
                options: {
                    start:     now,
                    end:       Date.now(),
                    count:     50,
                    aggregate: 'none'
                }
            }, function (result) {
                console.log(JSON.stringify(result.result, null, 2));
                expect(result.result.length).to.be.at.least(9);
                expect(result.result[0].val).to.be.equal(1);
                expect(result.result[1].val).to.be.equal(2.5);
                expect(result.result[2].val).to.be.equal(3.0);
                expect(result.result[3].val).to.be.equal(4);
                expect(result.result[4].val).to.be.equal(5);
                expect(result.result[5].val).to.be.equal(6);
                expect(result.result[6].val).to.be.equal(6);
                expect(result.result[7].val).to.be.equal(7);
                expect(result.result[8].val).to.be.equal(7);

                setTimeout(resolve, 2000);
            });
        });
    });

    it('Test ' + adapterShortName + ': Write debounced values into DB', async function () {
        this.timeout(45000);
        now = Date.now();

        try {
            await logSampleData('history.0.testValueDebounce');
        } catch (err) {
            console.log(err);
            expect(err).to.be.not.ok;
        }

        return new Promise(resolve => {

            sendTo('history.0', 'getHistory', {
                id: 'history.0.testValueDebounce',
                options: {
                    start:     now,
                    end:       Date.now(),
                    count:     50,
                    aggregate: 'none'
                }
            }, function (result) {
                console.log(JSON.stringify(result.result, null, 2));
                expect(result.result.length).to.be.at.least(13);
                expect(result.result[0].val).to.be.equal(1);
                expect(result.result[1].val).to.be.equal(2.5);
                expect(result.result[2].val).to.be.below(3);
                expect(result.result[3].val).to.be.equal(3);
                expect(result.result[4].val).to.be.equal(4);
                expect(result.result[5].val).to.be.below(5);
                expect(result.result[6].val).to.be.equal(5);
                expect(result.result[7].val).to.be.equal(5);
                expect(result.result[8].val).to.be.equal(6);
                expect(result.result[9].val).to.be.below(7);
                expect(result.result[10].val).to.be.below(7);
                expect(result.result[11].val).to.be.equal(7);
                expect(result.result[12].val).to.be.equal(7);

                resolve();
            });
        });
    });

    it(`Test ${adapterShortName}: Read percentile 50+95 values from DB using GetHistory`, function (done) {
        this.timeout(10000);

        sendTo('history.0', 'getHistory', {
            id: 'history.0.testValueDebounce',
            options: {
                start:     now,
                end:       Date.now(),
                count:     1,
                aggregate: 'percentile',
                percentile: 50,
                addId: true
            }
        }, result => {
            console.log(JSON.stringify(result.result, null, 2));
            expect(result.result.length).to.be.equal(1);
            expect(result.result[0].id).to.be.equal('history.0.testValueDebounce');
            const percentile50 = result.result[0].val;

            sendTo('history.0', 'getHistory', {
                id: 'history.0.testValueDebounce',
                options: {
                    start:     now,
                    end:       Date.now(),
                    count:     1,
                    aggregate: 'percentile',
                    percentile: 95,
                    addId: true
                }
            }, result => {
                console.log(JSON.stringify(result.result, null, 2));
                expect(result.result.length).to.be.equal(1);
                expect(result.result[0].id).to.be.equal('history.0.testValueDebounce');
                expect(result.result[0].val).to.be.greaterThan(percentile50);
                done();
            });
        });
    });

    it('Test ' + adapterShortName + ': Read integral from DB using GetHistory', function (done) {
        this.timeout(25000);

        sendTo('history.0', 'getHistory', {
            id: 'history.0.testValueDebounce',
            options: {
                start:     now,
                end:       Date.now(),
                count:     5,
                aggregate: 'integral',
                integralUnit: 5,
                addId: true
            }
        }, function (result) {
            console.log(JSON.stringify(result.result, null, 2));
            expect(result.result.length).to.be.equal(7);
            expect(result.result[0].id).to.be.equal('history.0.testValueDebounce');
            done();
        });
    });

    it('Test ' + adapterShortName + ': Read linear integral from DB using GetHistory', function (done) {
        this.timeout(25000);

        sendTo('history.0', 'getHistory', {
            id: 'history.0.testValueDebounce',
            options: {
                start:     now,
                end:       Date.now(),
                count:     5,
                aggregate: 'integral',
                integralUnit: 5,
                integralInterpolation: 'linear',
                addId: true
            }
        }, function (result) {
            console.log(JSON.stringify(result.result, null, 2));
            expect(result.result.length).to.be.equal(7);
            expect(result.result[0].id).to.be.equal('history.0.testValueDebounce');
            done();
        });
    });

    it('Test ' + adapterShortName + ': Write with 1s block values into DB', async function () {
        this.timeout(45000);
        now = Date.now();

        try {
            await logSampleData('history.0.testValueBlocked');
        } catch (err) {
            console.log(err);
            expect(err).to.be.not.ok;
        }

        return new Promise(resolve => {

            sendTo('history.0', 'getHistory', {
                id: 'history.0.testValueBlocked',
                options: {
                    start:     now,
                    end:       Date.now(),
                    count:     50,
                    aggregate: 'none'
                }
            }, function (result) {
                console.log(JSON.stringify(result.result, null, 2));
                expect(result.result.length).to.be.at.least(9);
                expect(result.result[0].val).to.be.equal(1);
                expect(result.result[1].val).to.be.at.least(2.9);
                expect(result.result[2].val).to.be.equal(4);
                expect(result.result[3].val).to.be.equal(5);
                expect(result.result[4].val).to.be.equal(6);
                expect(result.result[5].val).to.be.equal(6);
                expect(result.result[6].val).to.be.equal(6.45);
                expect(result.result[7].val).to.be.equal(7);
                expect(result.result[8].val).to.be.equal(7);

                resolve();
            });
        });
    });

    it('Test ' + adapterShortName + ': Write example-integral values into DB', async function () {
        this.timeout(45000);
        const nowSampleI1 = Date.now() - 24 * 60 * 60 * 1000;
        const nowSampleI21 = Date.now() - 23 * 60 * 60 * 1000;
        const nowSampleI22 = Date.now() - 22 * 60 * 60 * 1000;
        const nowSampleI23 = Date.now() - 21 * 60 * 60 * 1000;
        const nowSampleI24 = Date.now() - 20 * 60 * 60 * 1000;

        return new Promise(resolve => {

            sendTo('history.0', 'storeState', {
                id: 'history.0.testValue',
                state: [
                    {val: 2.064, ack: true, ts: nowSampleI1}, //
                    {val: 2.116, ack: true, ts: nowSampleI1 + 6 * 60 * 1000},
                    {val: 2.028, ack: true, ts: nowSampleI1 + 12 * 60 * 1000},
                    {val: 2.126, ack: true, ts: nowSampleI1 + 18 * 60 * 1000},
                    {val: 2.041, ack: true, ts: nowSampleI1 + 24 * 60 * 1000},
                    {val: 2.051, ack: true, ts: nowSampleI1 + 30 * 60 * 1000},

                    {val: -2, ack: true, ts: nowSampleI21}, // 10s none = 50.0
                    {val: 10, ack: true, ts: nowSampleI21 + 10 * 1000},
                    {val: 7, ack: true, ts: nowSampleI21 + 20 * 1000},
                    {val: 17, ack: true, ts: nowSampleI21 + 30 * 1000},
                    {val: 15, ack: true, ts: nowSampleI21 + 40 * 1000},
                    {val: 4, ack: true, ts: nowSampleI21 + 50 * 1000},

                    {val: 19, ack: true, ts: nowSampleI22}, // 10s none = 43
                    {val: 4, ack: true, ts: nowSampleI22 + 10 * 1000},
                    {val: -3, ack: true, ts: nowSampleI22 + 20 * 1000},
                    {val: 19, ack: true, ts: nowSampleI22 + 30 * 1000},
                    {val: 13, ack: true, ts: nowSampleI22 + 40 * 1000},
                    {val: 1, ack: true, ts: nowSampleI22 + 50 * 1000},

                    {val: -2, ack: true, ts: nowSampleI23}, // 10s linear = 25
                    {val: 7, ack: true, ts: nowSampleI23 + 20 * 1000},
                    {val: 4, ack: true, ts: nowSampleI23 + 50 * 1000},

                    {val: 4, ack: true, ts: nowSampleI24 + 10 * 1000}, // 10s linear = 32.5
                    {val: -3, ack: true, ts: nowSampleI24 + 20 * 1000},
                    {val: 19, ack: true, ts: nowSampleI24 + 30 * 1000},
                    {val: 1, ack: true, ts: nowSampleI24 + 50 * 1000},
                ]
            }, function (result) {
                expect(result.success).to.be.true;

                sendTo('history.0', 'getHistory', {
                    id: 'history.0.testValue',
                    options: {
                        start:     nowSampleI1,
                        end:       nowSampleI1 + 30 * 60 * 1000,
                        count:     1,
                        aggregate: 'integral',
                        integralUnit: 1,
                        integralInterpolation: 'none'
                    }
                }, function (result) {
                    console.log('Sample I1-1: ' + JSON.stringify(result.result, null, 2));
                    expect(result.result.length).to.be.equal(1);
                    // Result Influxdb1 Doku = 3732.66

                    sendTo('history.0', 'getHistory', {
                        id: 'history.0.testValue',
                        options: {
                            start:     nowSampleI1,
                            end:       nowSampleI1 + 30 * 60 * 1000,
                            count:     1,
                            aggregate: 'integral',
                            integralUnit: 60,
                            integralInterpolation: 'none'
                        }
                    }, function (result) {
                        console.log('Sample I1-60: ' + JSON.stringify(result.result, null, 2));
                        expect(result.result.length).to.be.equal(1);
                        // Result Influxdb1 Doku = 62.211

                        sendTo('history.0', 'getHistory', {
                            id: 'history.0.testValue',
                            options: {
                                start:     nowSampleI21,
                                end:       nowSampleI21 + 60 * 1000,
                                count:     1,
                                aggregate: 'integral',
                                integralUnit: 10,
                                integralInterpolation: 'none'
                            }
                        }, function (result) {
                            console.log('Sample I21: ' + JSON.stringify(result.result, null, 2));
                            expect(result.result.length).to.be.equal(1);
                            // Result Influxdb21 Doku = 50.0

                            sendTo('history.0', 'getHistory', {
                                id: 'history.0.testValue',
                                options: {
                                    start:     nowSampleI22,
                                    end:       nowSampleI22 + 60 * 1000,
                                    count:     1,
                                    aggregate: 'integral',
                                    integralUnit: 10,
                                    integralInterpolation: 'none'
                                }
                            }, function (result) {
                                console.log('Sample I22: ' + JSON.stringify(result.result, null, 2));
                                expect(result.result.length).to.be.equal(1);
                                // Result Influxdb22 Doku = 43

                                sendTo('history.0', 'getHistory', {
                                    id: 'history.0.testValue',
                                    options: {
                                        start:     nowSampleI23,
                                        end:       nowSampleI23 + 60 * 1000,
                                        count:     1,
                                        aggregate: 'integral',
                                        integralUnit: 10,
                                        integralInterpolation: 'linear'
                                    }
                                }, function (result) {
                                    console.log('Sample I23: ' + JSON.stringify(result.result, null, 2));
                                    expect(result.result.length).to.be.equal(1);
                                    // Result Influxdb23 Doku = 25.0

                                    sendTo('history.0', 'getHistory', {
                                        id: 'history.0.testValue',
                                        options: {
                                            start:     nowSampleI24,
                                            end:       nowSampleI24 + 60 * 1000,
                                            count:     1,
                                            aggregate: 'integral',
                                            integralUnit: 10,
                                            integralInterpolation: 'linear'
                                        }
                                    }, function (result) {
                                        console.log('Sample I24: ' + JSON.stringify(result.result, null, 2));
                                        expect(result.result.length).to.be.equal(1);
                                        // Result Influxdb24 Doku = 32.5

                                        resolve();
                                    });
                                });
                            });
                        });
                    });
                });
            });
        });
    });

    it('Test ' + adapterShortName + ': Remove Alias-ID', function (done) {
        this.timeout(5000);

        sendTo('history.0', 'enableHistory', {
            id: 'history.0.testValue2',
            options: {
                aliasId: ''
            }
        }, function (result) {
            expect(result.error).to.be.undefined;
            expect(result.success).to.be.true;
            // wait till adapter receives the new settings
            setTimeout(function () {
                done();
            }, 2000);
        });
    });
    it('Test ' + adapterShortName + ': Add Alias-ID again', function (done) {
        this.timeout(5000);

        sendTo('history.0', 'enableHistory', {
            id: 'history.0.testValue2',
            options: {
                aliasId: 'this.is.a.test-value'
            }
        }, function (result) {
            expect(result.error).to.be.undefined;
            expect(result.success).to.be.true;
            // wait till adapter receives the new settings
            setTimeout(function () {
                done();
            }, 2000);
        });
    });
    it('Test ' + adapterShortName + ': Change Alias-ID', function (done) {
        this.timeout(5000);

        sendTo('history.0', 'enableHistory', {
            id: 'history.0.testValue2',
            options: {
                aliasId: 'this.is.another.test-value'
            }
        }, function (result) {
            expect(result.error).to.be.undefined;
            expect(result.success).to.be.true;
            // wait till adapter receives the new settings
            setTimeout(function () {
                done();
            }, 2000);
        });
    });

    it('Test ' + adapterShortName + ': Disable Datapoint again', function (done) {
        this.timeout(5000);

        sendTo('history.0', 'disableHistory', {
            id: 'history.0.testValue'
        }, function (result) {
            expect(result.error).to.be.undefined;
            expect(result.success).to.be.true;
            done();
        });
    });
    it('Test ' + adapterShortName + ': Check Enabled Points after Disable', function (done) {
        this.timeout(5000);

        sendTo('history.0', 'getEnabledDPs', {}, function (result) {
            console.log(JSON.stringify(result));
            expect(Object.keys(result).length).to.be.equal(4);
            done();
        });
    });

    after('Test ' + adapterShortName + ' adapter: Stop js-controller', function (done) {
        this.timeout(10000);

        setup.stopController(function (normalTerminated) {
            console.log('Adapter normal terminated: ' + normalTerminated);
            done();
        });
    });
});
