/* jshint -W097 */// jshint strict:false
/*jslint node: true */
"use strict";

var utils = require(__dirname + '/lib/utils'); // Get common adapter utils
var controllerDir = utils.controllerDir;
var dataDir  = require(controllerDir + '/lib/tools').getDefaultDataDir();
var fs = require('fs');
var adapter = utils.adapter({

    name: 'history',

    objectChange: function (id, obj) {
        if (obj && obj.common && obj.common.history && obj.common.history.enabled) {
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
        callback();
    },

    ready: function () {
        main();
    },

    message: function (obj) {
        processMessage(obj);
    }

});
var history = {};



function processMessage(msg) {
    if (msg.command == "getHistory") {
        getHistory(msg)
    }
}

function main() {

    adapter.objects.getObjectView('history', 'state', {}, function (err, doc) {
        if (doc && doc.rows) {
            for (var i = 0, l = doc.rows.length; i < l; i++) {
                if (doc.rows[i].value) {
                    adapter.log.info('enabled logging of ' + doc.rows[i].id);
                    history[doc.rows[i].id] = doc.rows[i].value;
                }
            }
        }
    });

    adapter.subscribeForeignStates('*');
    adapter.subscribeForeignObjects('*');
}

function pushHistory(id, state) {

    // Push into redis
    if (history[id] && history[id].enabled) {
        if (history[id].state && history[id].changesOnly && (state.ts !== state.lc)) return;

        history[id].state = state;
        // Do not store values ofter than 1 second
        if (!history[id].timeout) {

            history[id].timeout = setTimeout(function (_id) {
                // if it was not deleted in this time
                if (history[_id]) {
                    history[_id].timeout = null;
                    adapter.states.pushFifo(_id, history[_id].state);

                    adapter.states.trimFifo(_id, history[id].minLength || adapter.config.minLength, history[id].maxLength || adapter.config.maxLength, function (err, obj) {
                        if (!err && obj.length) {
                            adapter.log.info('moving ' + obj.length + ' entries to couchdb');
                            appendCouch(_id, obj);
                        }
                    });
                }
            }, history[id].debounce || 1000, id);
        }
    }
}

function appendCouch(id, states) {

    var day = ts2day(states[states.length - 1].ts);
    var cid = 'history.' + id + '.' + day;

    adapter.getForeignObject(cid, function (err, res) {
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
            adapter.log.info('moved ' + states.length + ' history datapoints from Redis history.' + id + ' to CouchDB ' + cid);
        });

        if (i >= 0) {
            adapter.log.info((i + 1) + ' remaining datapoints of history.' + id);
            appendCouch(id, states.slice(0, (i + 1)));
        }
    });

}

function get_cacheData(id, option, callback) {
    adapter.getFifo(id, function (err, res) {
        var cache = []
        if (!err && res) {
            var iProblemCount = 0;
            for (var i = 0; i < res.length; i++) {
                if (!res[i]) {
                    iProblemCount++;
                    continue;
                }
                if (res[i].ts < option.start) {
                    continue;
                } else if (res[i].ts > option.end) {
                    break;
                }
                cache.push(res[i]);
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
        callback(cache)
    })
}

function get_fileData(id, options, callback) {

    var day_start = ts2day(options.start);
    var day_end = ts2day(options.end);
    var data = [];
    var path = controllerDir + "/" + dataDir + "history/"

    // erstellt Ordner Liste


    var day_list = getDirectories(path).sort(function (a, b) {
        return a - b
    });

    // List Datei aus Ordner
    for (var i = 0; i < day_list.length; i++) {
        var day = parseInt(day_list[i]);
        if (day >= day_start && day <= day_end) {
            var file = path + day_list[i].toString() + "/history." + id + ".json";
            if (fs.existsSync(file)) {
                try {
                    data = data.concat(JSON.parse(fs.readFileSync(file)))
                } catch (e) {
                    log.error('Cannot parse file ' + file + ': ' + e.message);
                }
            }
        } else if (day >= day_end) {
            break;
        }
    }

    callback(data)
}

function aggregate(data, options) {
    if (data && data.length) {
        var start = new Date(options.start * 1000);
        var end = new Date(options.end * 1000);

        var step = 1; // 1 Step is 1 second
        if (options.step) {
            step = options.step;
        } else{
            step = Math.round((options.end-options.start)/ options.count) ;
        }

        // Limit 2000
        if( (options.end-options.start) / step > options.limit){
            step = Math.round((options.end-options.start)/ options.limit);
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
            //if (stepEnd < start) {
            //    // Summer time
            //    stepEnd.setHours(start.getHours() + 2);
            //}

            // find all entries in this time period
            var value = null;
            var count = 0;

            var timeStamp = new Date(data[i].ts);
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
                } else if (options.aggregate == 'average10') {
                    if (data[i].ts > timeStamp) {
                        count++;
                        timeStamp = data[i].ts;
                    }
                    if (value === null) value = 0;
                    value += data[i].val;
                } else if (options.aggregate == 'total') {
                    // Find sum
                    if (value === null) value = 0;
                    value += parseFloat(data[i].val);
                }
                i++;
            }

            if (options.aggregate == 'average' || options.aggregate == 'average10') {
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

        return [result,step,data.length];
    } else {
        return [];
    }
}

function getHistory(msg, callback) {
    var startTime = new Date().getTime();
    var id = msg.message.id;
    var options =
    {
        start: msg.message.options.start || Math.round((new Date()).getTime() / 1000) - 5030, // - 1 year
        end: msg.message.options.end || Math.round((new Date()).getTime() / 1000) + 5000,
        step: parseInt(msg.message.options.step) || null,
        count: parseInt(msg.message.options.count) || 500,
        getNull: msg.message.options.getNull,
        aggregate: msg.message.options.aggregate || "average", // One of: max, min, average, total
        limit: msg.message.options.limit || adapter.config.limit || 2000
    };

    if (options.start > options.end){
        var _end = options.end;
        options.end = options.start;
        options.start =_end;
    }



    get_cacheData(id, options, function (cacheData) {
        get_fileData(id, options, function (fileData) {

            var data = cacheData.concat(fileData);

            function sortByTs(a, b) {
                var aTs = a.ts;
                var bTs = b.ts;
                return ((aTs < bTs) ? -1 : ((aTs > bTs) ? 1 : 0));
            }

            data = data.sort(sortByTs);


                var aggregateData = aggregate(data, options);

                adapter.log.info("Sende: " + aggregateData[0].length +" von: " + aggregateData[2] +" in: " + (new Date().getTime()- startTime) +"ms" );
                adapter.sendTo(msg.from, msg.command, {result: aggregateData[0],'step':aggregateData[1], error: null}, msg.callback);


        })
    })
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

