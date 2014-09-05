
/* jshint -W097 */// jshint strict:false
/*jslint node: true */
"use strict";

var adapter = require(__dirname + '/../../lib/adapter.js')({

    name:           'history',

    objectChange: function (id, obj) {
        if (obj.history) {
            history[id] = obj.history;
            console.log(id, obj.history);
        } else {
            delete history[id];
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
                    adapter.log.info('history push ' + doc.rows[i].id);
                    history[doc.rows[i].id] = doc.rows[i].value;
                }
            }
        }
    });

    adapter.subscribeForeignStates('*');
}

function pushHistory(id, state) {

    // Push to fifo
    if (history[id] && history[id].enabled) {
        if (history[id].changesOnly && state.ts !== state.lc) return;
        setTimeout(function (_id, _state) {
            adapter.states.pushFifo(_id, _state);

            adapter.states.lenFifo(_id, function (err, len) {
                if (len > adapter.config.maxLength) {
                    // Todo sent to other Log targets
                    adapter.states.trimFifo(_id, adapter.config.minLength || 0, function (err, obj) {
                        appendCouch(_id, obj);
                    });
                }
            });
        }, 1000, id, state);
    }

}


function appendCouch(id, states) {
    //var id = 'history.' +adapter.instance + '.' + id;
    var id = 'history.' + id;
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
        for (var i = states.length - 1; i >= 0 ; i--) {
            obj.data.push(states[i]);
        }
        adapter.setForeignObject(id, obj, function () {
            adapter.log.info('moved ' + states.length + ' history datapoints of ' + _id + ' to CouchDB');
        });
    });

}

