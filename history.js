
/* jshint -W097 */// jshint strict:false
/*jslint node: true */
"use strict";

var adapter = require(__dirname + '/../../lib/adapter.js')({

    name:           'history',

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
        if (doc.rows) {
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
        if (history[id].changesOnly && state.ts !== state.lc) return;
        setTimeout(function (_id, _state) {
            adapter.states.pushFifo(_id, _state);

            adapter.states.lenFifo(_id, function (err, len) {
                adapter.log.info('fifo ' + _id + ' len=' + len + ' maxLength=' + adapter.config.maxLength);
                if (len > adapter.config.maxLength) {
                    adapter.states.trimFifo(_id, adapter.config.minLength || 0, function (err, obj) {
                        adapter.log.info('moving ' + obj.length + ' entries to couchdb');
                        appendCouch(_id, obj);

                    });
                }
            });
        }, 1000, id, state);
    }

}


function appendCouch(id, states) {
    //var id = 'history.' +adapter.instance + '.' + id;
    id = 'history.' + id;
    adapter.getForeignObject(id, function (err, res) {
        var obj;
        if (err || !res) {
            obj = {
                type: 'history',
                common: {},
                native: {},
                data: []
            }
        } else {
            obj = res;

        }
        for (var i = states.length - 1; i >= 0; i--) {
            obj.data.unshift(states[i]);
        }
        adapter.setForeignObject(id, obj, function () {
            adapter.log.info('moved ' + states.length + ' history datapoints of ' + id + ' to CouchDB');
        });
    });

}

