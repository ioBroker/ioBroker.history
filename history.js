/* jshint -W097 */// jshint strict:false
/*jslint node: true */
"use strict";
var utils =    require(__dirname + '/lib/utils'); // Get common adapter utils

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
    }

});

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
