'use strict';

//usage: nodejs analyzeinflux.js <InfluxDB-Instance> [<Loglevel>]
//usage: nodejs analyzeinflux.js influxdb.0 info

const utils = require('@iobroker/adapter-core'); // Get common adapter utils

const fs = require('fs');

const earliestDBValue = {};
const earliesValCachefile = `${__dirname}/earliestDBValues.json`;
const existingData = {};
const existingDataCachefile = `${__dirname}/existingDBValues.json`;
const existingTypes = {};
const existingTypesCachefile = `${__dirname}/existingDBTypes.json`;

let deepAnalyze = false;
let influxInstance = 'influxdb.0';
let influxDbVersion = 1;
let bucket = 'iobroker';

if (process.argv.indexOf('--deepAnalyze') !== -1) deepAnalyze = true;
if (process.argv[2] && process.argv[2].indexOf('influxdb') === 0) {
    influxInstance = process.argv[2];
}
process.argv[2] = '--install';

if (deepAnalyze) {
    console.log('Do deep analysis to find holes in data');
}

let breakIt = false;
const adapter = new utils.Adapter({
    name: 'history',
    ready: main,
});

const stdin = process.stdin;
// without this, we would only get streams once enter is pressed
stdin.setRawMode(true);
// resume stdin in the parent process (node app won't quit all by itself
// unless an error or process.exit() happens)
stdin.resume();
// i don't want binary, do you?
stdin.setEncoding('utf8');
// on any data into stdin
stdin.on('data', key => {
    // write the key to stdout all normal like
    console.log(`Received Keypress: ${key.toString()}`);

    // ctrl-c ( end of text )
    if (key === 'x' || key === '\u0003') {
        console.log('Trying to stop...');
        breakIt = true;
    }
});

let counter = 0;
let dpList = [];

async function main() {
    try {
        const instanceConfig = await adapter.getForeignObjectAsync(`system.adapter.${influxInstance}`);

        if (instanceConfig) {
            console.log(`InfluxDB-Configuration: ${JSON.stringify(instanceConfig.native)}`);

            if (instanceConfig.native.dbversion && String(instanceConfig.native.dbversion).startsWith('2.')) {
                influxDbVersion = 2;
            }

            if (instanceConfig.native.dbname) {
                bucket = instanceConfig.native.dbname;
            }

            console.log(`Query Data from instance "${influxInstance}" (Version ${influxDbVersion}, Bucket ${bucket})`);

            let query = 'SHOW MEASUREMENTS';
            if (influxDbVersion == 2) {
                query = `import "influxdata/influxdb/schema" schema.measurements(bucket: "${bucket}")`;
            }

            adapter.sendTo(influxInstance, 'query', query, result => {
                if (result) {
                    dpList = result.result[0];

                    console.log(`Datapoints found: ${dpList.length}`);

                    analyze();
                }
            });
        } else {
            console.log(`Unable to get instance config of: ${influxInstance}`);
        }
    } catch (err) {
        console.log(`Error: ${err}`);
        process.exit();
    }
}

function analyze() {
    if (!breakIt && dpList.length > 0) {
        counter++;
        if (counter % 100 === 0) {
            setTimeout(analyze, 5000);
        } else {
            const dp = dpList.shift();
            const dpName = influxDbVersion == 2 ? dp._value : dp.name;

            let query = `SELECT FIRST(ack) AS val FROM "${dpName}"`;
            if (deepAnalyze) {
                query += `;SELECT COUNT(ack) AS val FROM "${dpName}" where time < now() group by time(1d)`;
                query += `;SELECT LAST(value) as val FROM "${dpName}"`;
            }

            if (influxDbVersion == 2) {
                query = `from(bucket: "${bucket}")
                    |> range(start: 0)
                    |> filter(fn: (r) => r["_measurement"] == "${dpName}")
                    |> filter(fn: (r) => r["_field"] == "value")
                    |> first()`;

                if (deepAnalyze) {
                    query += `;from(bucket: "${bucket}")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "${dpName}")
                        |> filter(fn: (r) => r["_field"] == "value")
                        |> aggregateWindow(every: 1d, fn: count, createEmpty: false)`;

                    query += `;from(bucket: "${bucket}")
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "${dpName}")
                        |> filter(fn: (r) => r["_field"] == "value")
                        |> last()`;
                }
            }

            adapter.sendTo(influxInstance, 'query', query, resultDP => {
                if (resultDP.error) {
                    console.error(resultDP.error);
                } else {
                    if (resultDP.result[0]) {
                        earliestDBValue[dpName] = resultDP.result[0][0].ts;
                        console.log(
                            `FirstVal ID: ${dpName}, Rows: ${JSON.stringify(resultDP.result[0])} --> ${new Date(earliestDBValue[dpName]).toString()}`,
                        );
                    }

                    if (deepAnalyze) {
                        if (resultDP.result[1]) {
                            existingData[dpName] = [];
                            for (var j = 0; j < resultDP.result[1].length; j++) {
                                if (influxDbVersion == 2) {
                                    if (resultDP.result[1][j]._value > 0) {
                                        var ts = new Date(resultDP.result[1][j]._time);
                                        existingData[dpName].push(parseInt(ts2day(ts), 10));
                                    }
                                } else {
                                    if (resultDP.result[1][j].val > 0) {
                                        var ts = new Date(resultDP.result[1][j].ts);
                                        existingData[dpName].push(parseInt(ts2day(ts), 10));
                                    }
                                }
                            }
                            console.log(`DayVals ID: ${dpName}: ${JSON.stringify(existingData[dpName])}`);
                        }

                        if (resultDP.result[2] && resultDP.result[2][0]) {
                            existingTypes[dpName] =
                                influxDbVersion == 2
                                    ? typeof resultDP.result[2][0]._value
                                    : typeof resultDP.result[2][0].val;
                            console.log(`ValType ID: ${dpName}: ${JSON.stringify(existingTypes[dpName])}`);
                        }
                    }
                }

                // next
                setTimeout(analyze, 100);
            });
        }
    } else {
        console.log(`Writing files to ${__dirname} ...`);

        if (deepAnalyze) {
            fs.writeFileSync(existingDataCachefile, JSON.stringify(existingData, null, 2));
            console.log(`    - saved ${existingDataCachefile}`);

            fs.writeFileSync(existingTypesCachefile, JSON.stringify(existingTypes, null, 2));
            console.log(`    - saved ${existingTypesCachefile}`);
        }

        fs.writeFileSync(earliesValCachefile, JSON.stringify(earliestDBValue, null, 2));
        console.log(`    - saved ${earliesValCachefile}`);

        process.exit();
    }
}

function ts2day(ts) {
    const dateObj = new Date(ts);
    const y = dateObj.getFullYear();
    const m = dateObj.getMonth() + 1;
    const d = dateObj.getDate();

    return `${y}${m < 10 ? `0${m}` : m}${d < 10 ? `0${d}` : d}`;
}

process.on('SIGINT', function () {
    console.log('SIGINT');
    breakIt = true;
});

process.on('uncaughtException', function (err) {
    console.log(`uncaughtException: ${err}`);
    breakIt = true;
});
