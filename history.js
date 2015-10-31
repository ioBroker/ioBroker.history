/* jshint -W097 */// jshint strict:false
/*jslint node: true */
"use strict";
var utils =    require(__dirname + '/lib/utils'); // Get common adapter utils
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

    message: function(obj){
        processMessage(obj);
    }

});

var dataDir = __dirname +"/../../iobroker-data/history"; // todo get DataDir

function processMessage(msg) {
    if(msg.command == "getHistory"){
        getHistory(msg)
    }
}

var history = {};

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
                    day:    day,
                    data:   []
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

function getHistory(msg,callback){
    var id = msg.message.id;
    var _options =
    {
        start : msg.message.start || Math.round((new Date()).getTime() / 1000) - 5030, // - 1 year
        end  : msg.message.end || Math.round((new Date()).getTime() / 1000) + 5000,
        step: msg.message.step ,
        count:msg.message.count,
        aggregate: msg.message.aggregate || "max" // One of: max, min, average, total
    };


    // get Data
    function get_cacheData(id, start, end, callback) {
        adapter.getFifo(id, function (err, res) {
            var cache = []
            if (!err && res) {
                var iProblemCount = 0;
                for (var i = 0; i < res.length; i++) {
                    if (!res[i]) {
                        iProblemCount++;
                        continue;
                    }
                    if (res[i].ts < start) {
                        continue;
                    } else if (res[i].ts > end) {
                        break;
                    }
                    cache.push(res[i]);
                }
                if (iProblemCount) that.log.warn('got null states ' + iProblemCount + ' times for ' + id);

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

    function get_fileData(id, start, end, callback) {


        var historyName = dataDir + 'history/';
        var day_start = ts2day(_options.start);
        var day_end = ts2day(_options.end);
        var data = []

        // erstellt Ordner Liste


        var day_list = getDirectories("c:/io/iobroker-data/history/").sort(function (a, b) {
            return a - b
        });

        // List Datei aus Ordner
        for (var i = 0; i < day_list.length; i++) {
            var day = parseInt(day_list[i]);
            if (day >= day_start && day <= day_end) {
                var file = "c:/io/iobroker-data/history/" + day_list[i].toString() + "/history." + id + ".json";
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


    // This function aggreage values from array by given method
    // data is array with  {ts: timestamp as number, val: as number}
    // possible options:
    //   start (required)
    //   end   (required)
    //   step  is step in seconds, if no step is there so count is used, if no count, step is 1 second
    //   aggregate is method. One of: max, min, average, total
    function aggregate(data, options) {

        var step;
        if (!options.count && options.step) {
            step = options.step;
        } else if (options.count) {
            step = Math.round(data.length / options.count);
        }else{
            step = Math.round(data.length / 1000);
        }

        if (!step) step = 1;

        // 1 Step is 1 second
        var start = new Date(options.start * 1000);
        var end = new Date(options.end * 1000);
        var timeStamp = start;
        var stepEnd;
        var i = 0;
        var result = [];
        var iStep = 0;
        options.aggregate = options.aggregate || 'max';

        while (start < end) {
            stepEnd = new Date(start);
            stepEnd.setSeconds(stepEnd.getSeconds() + step);
            if (stepEnd < start) {
                // Summer time
                stepEnd.setHours(start.getHours() + 2);
            }

            // find all entries in this time period
            var value = null;
            var count = 0;
            var timeStamp = new Date(data[i].ts);
            while (i < data.length && new Date(data[i].ts * 1000) < stepEnd) {
                var y = new Date(data[i].ts * 1000)
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
            if (value) {
                result[iStep] = {ts: stepEnd.getTime() / 1000};
                result[iStep].val = value;
                iStep++;
            }


            start = stepEnd;
        }

        return result;
    }

    get_cacheData(id,_options.start, _options.end,function(cacheData){
        get_fileData(id,_options.start, _options.end,function(fileData){

            var data = cacheData.concat(fileData)

            function SortByName(a, b) {
                var aName = a.ts;
                var bName = b.ts;
                return ((aName < bName) ? -1 : ((aName > bName) ? 1 : 0));
            }
            data = data.sort(SortByName)

            var aggre_data = aggregate(data,_options)
            //callback(null, aggre_data);
            //callback(null, []);
            //adapter.sendTo(msg.from, msg.command, {result: [], error: null}, msg.callback);
            adapter.sendTo(msg.from, msg.command, {result: aggre_data, error: null}, msg.callback);
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

