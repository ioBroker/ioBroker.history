"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
exports.finishAggregation = exports.aggregation = exports.initAggregate = void 0;
exports.getFileData = getFileData;
exports.ts2day = ts2day;
exports.response = response;
exports.getFilenameForID = getFilenameForID;
// todo     add cache data
// todo     error tests
// todo     clean up
const fs = __importStar(require("node:fs"));
const aggregate_1 = require("./aggregate");
Object.defineProperty(exports, "initAggregate", { enumerable: true, get: function () { return aggregate_1.initAggregate; } });
Object.defineProperty(exports, "aggregation", { enumerable: true, get: function () { return aggregate_1.aggregation; } });
Object.defineProperty(exports, "finishAggregation", { enumerable: true, get: function () { return aggregate_1.finishAggregation; } });
// ─── Module-level state ──────────────────────────────────────────────────────
let gOptions;
let finished = false;
let initialized = false;
let cacheReceived = false;
let cacheData = null;
// ─── Script entry point ───────────────────────────────────────────────────────
if (require.main === module) {
    gOptions = JSON.parse(process.argv[2]);
    if (gOptions?.logDebug) {
        gOptions.log = (...args) => {
            process.send?.(['debug', ...args]);
        };
    }
    else {
        gOptions.log = () => { };
    }
    process.on('message', (message) => {
        if (message[0] === 'cacheData') {
            cacheReceived = true;
            gOptions.log(`${Date.now()}: cacheData received (cnt=${message[1] ? message[1].length : 'none'}, initialized=${initialized})`);
            if (initialized) {
                gOptions.log(`${Date.now()}: aggregate cacheData`);
                if (message[1]) {
                    (0, aggregate_1.aggregation)(gOptions, message[1]);
                }
                gOptions.log(`${Date.now()}: aggregate cacheData Done`);
                if (finished) {
                    response(gOptions);
                }
                else {
                    finished = true;
                }
            }
            else {
                cacheData = message[1];
            }
        }
        else if (message[0] === 'exit') {
            process.exit();
        }
    });
    gOptions.log(`${Date.now()}: Initialize getHistory`);
    if (process.send) {
        gOptions.log(`${Date.now()}: Request cacheData`);
        process.send(['getCache', gOptions], () => processData(gOptions));
    }
    else {
        finished = true;
        processData(gOptions);
    }
}
// ─── Helpers ─────────────────────────────────────────────────────────────────
function getDirectories(path) {
    return fs.readdirSync(path).filter(file => !file.startsWith('.') && fs.statSync(`${path}/${file}`).isDirectory());
}
function tsSort(a, b) {
    return b.ts - a.ts;
}
function getFilenameForID(path, date, id) {
    // eslint-disable-next-line no-control-regex
    const safeId = id.toString().replace(/[\u0000|*,;"'<>?:/\\]/g, '~');
    return `${path}${date.toString()}/history.${safeId}.json`;
}
function ts2day(ts) {
    const dateObj = new Date(ts);
    let text = dateObj.getFullYear().toString();
    let v = dateObj.getMonth() + 1;
    if (v < 10) {
        text += '0';
    }
    text += v.toString();
    v = dateObj.getDate();
    if (v < 10) {
        text += '0';
    }
    text += v.toString();
    return text;
}
// ─── Core logic ───────────────────────────────────────────────────────────────
function getFileData(options) {
    const addId = options.addId;
    const dayStart = parseInt(ts2day(options.start), 10);
    const dayEnd = parseInt(ts2day(options.end), 10);
    options.log(`${Date.now()}: getFileData start ${dayStart} end ${dayEnd} in ${options.path}`);
    // get a list of directories
    let dayList = getDirectories(options.path);
    if (options.returnNewestEntries) {
        dayList = dayList.sort((a, b) => parseInt(b) - parseInt(a));
    }
    else {
        dayList = dayList.sort((a, b) => parseInt(a) - parseInt(b));
    }
    for (let i = 0; i < dayList.length; i++) {
        const day = parseInt(dayList[i], 10);
        if (!isNaN(day) && day >= dayStart && day <= dayEnd) {
            options.log(`${Date.now()}: getFileData process ${day}`);
            const file = getFilenameForID(options.path, dayList[i], options.id);
            const tsCheck = new Date(Math.floor(day / 10000), 0, 1).getTime();
            try {
                if (fs.existsSync(file)) {
                    let fileData;
                    try {
                        fileData = JSON.parse(fs.readFileSync(file, 'utf-8')).sort(tsSort);
                    }
                    catch {
                        fileData = null;
                    }
                    if (fileData) {
                        let last = false;
                        for (let ii = 0; ii < fileData.length; ii++) {
                            // if a ts in seconds is stored, convert on the fly
                            if (fileData[ii].ts && fileData[ii].ts < tsCheck) {
                                fileData[ii].ts *= 1000;
                            }
                            if (typeof fileData[ii].val === 'number' &&
                                isFinite(fileData[ii].val) &&
                                options.round) {
                                fileData[ii].val =
                                    Math.round(fileData[ii].val * options.round) / options.round;
                            }
                            if (options.ack) {
                                fileData[ii].ack = !!fileData[ii].ack;
                            }
                            if (!options.q && fileData[ii].q !== undefined) {
                                delete fileData[ii].q;
                            }
                            if (!options.user && fileData[ii].user !== undefined) {
                                delete fileData[ii].user;
                            }
                            if (!options.comment && fileData[ii].c !== undefined) {
                                delete fileData[ii].c;
                            }
                            if (addId) {
                                fileData[ii].id = options.id;
                            }
                            if ((options.returnNewestEntries ||
                                options.aggregate === 'onchange' ||
                                options.aggregate === '' ||
                                options.aggregate === 'none') &&
                                ii >= (options.count ?? Infinity)) {
                                fileData.length = ii;
                                break;
                            }
                            if (last) {
                                fileData.length = ii;
                                break;
                            }
                            if (options.start && fileData[ii].ts < options.start) {
                                last = true;
                            }
                        }
                    }
                    options.log(`${Date.now()}: getFileData aggregate ${day} (length=${fileData ? fileData.length : 0})`);
                    if (fileData) {
                        (0, aggregate_1.aggregation)(options, fileData);
                    }
                }
            }
            catch (err) {
                options.log(`${Date.now()}: getFileData error ${day} ${err.message}`);
                options.log(`${Date.now()}: ${err.stack}`);
            }
        }
        if (day > dayEnd) {
            break;
        }
    }
    options.log(`${Date.now()}: getFileData Done`);
}
function response(options) {
    options.log(`${Date.now()}: Finish Aggregation`);
    (0, aggregate_1.finishAggregation)(options);
    options.log(`${Date.now()}: Send Response`);
    if (require.main === module) {
        if (process.send) {
            process.send(['response', options.result, options.overallLength, options.step], () => setTimeout(() => process.exit(), 200));
        }
        else {
            setTimeout(() => process.exit(), 500);
        }
    }
    else {
        return ['response', options.result, options.overallLength, options.step];
    }
}
function processData(options) {
    options.log(`${Date.now()}: Initialize structures: ${JSON.stringify(options)}`);
    // initAggregate mutates options in-place and returns the same object
    (0, aggregate_1.initAggregate)(options);
    initialized = true;
    if (cacheReceived) {
        options.log(`${Date.now()}: Aggregate cacheData`);
        if (cacheData) {
            (0, aggregate_1.aggregation)(options, cacheData);
        }
        options.log(`${Date.now()}: Aggregate cacheData Done`);
        cacheData = null;
        finished = true;
    }
    getFileData(options);
    if (finished) {
        response(options);
    }
    else {
        finished = true;
    }
}
// how to use:
// const options = initAggregate(options);
// getFileData(options);
// const result = response(options);
//# sourceMappingURL=getHistory.js.map