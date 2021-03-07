This adapter saves state history in a two-staged process.
At first data points are stored in RAM, as soon as they reach maxLength they will be stored on disk.

To set up some data points to be stored they must be configured in admin "Objects" Tab (last button).

To enable charts you have to install **flot** adapter.

## Settings

- **Storage directory** - Path to the directory, where the files will be stored. It can be done relative to "iobroker-data" or absolute, like "/mnt/history" or "D:/History"
- **Maximal number of stored in RAM values** - After this number of values reached in RAM they will be saved on disk.
- **Store origin of value** - If "from" field will be stored too. Can save place on disk.
- **De-bounce interval** - Protection against too often changes of some value and defined tha time in ms in which after one value change other changes are not logged
- **Storage retention** - How many values in the past will be stored on disk.
- **Log unchanged values any(s)** - When using "log changes only" you can set a time interval in seconds here after which also unchanged values will be re-logged into the DB

Most of these values will be pre filled on the detail settings for the datapoint and can be changed there. Additionally you can an "alias ID" on the datapoint page. With this you can, e.g. after switching a device and datapoint names changed, still log the data to the former ID by just entering this ID there and all data will be logged as this one.


## Access values from Javascript adapter
The sorted values can be accessed from Javascript adapter.

* Get 50 last stored events for all IDs
```
sendTo('history.0', 'getHistory', {
    id: '*',
    options: {
        end:       Date.now(),
        count:     50,
        aggregate: 'onchange'
    }
}, function (result) {
    for (var i = 0; i < result.result.length; i++) {
        console.log(result.result[i].id + ' ' + new Date(result.result[i].ts).toISOString());
    }
});
```

* Get stored values for "system.adapter.admin.0.memRss" in last hour
```
var end = Date.now();
sendTo('history.0', 'getHistory', {
    id: 'system.adapter.admin.0.memRss',
    options: {
        start:      end - 3600000,
        end:        end,
        aggregate: 'onchange'
    }
}, function (result) {
    for (var i = 0; i < result.result.length; i++) {
        console.log(result.result[i].id + ' ' + new Date(result.result[i].ts).toISOString());
    }
});
```

Possible options:
- **start** - (optional) time in ms - *Date.now()*'
- **end** - (optional) time in ms - *Date.now()*', by default is (now + 5000 seconds)
- **step** - (optional) used in aggregate (m4, max, min, average, total) step in ms of intervals
- **count** - number of values if aggregate is 'onchange' or number of intervals if other aggregate method. Count will be ignored if step is set, else default is 500 if not set
- **from** - if *from* field should be included in answer
- **ack** - if *ack* field should be included in answer
- **q** - if *q* field should be included in answer
- **addId** - if *id* field should be included in answer
- **limit** - do not return more entries than limit
- **ignoreNull** - if null values should be include (false), replaced by last not null value (true) or replaced with 0 (0)
- **aggregate** - aggregate method:
    - *minmax* - used special algorithm. Splice the whole time range in small intervals and find for every interval max, min, start and end values.
    - *max* - Splice the whole time range in small intervals and find for every interval max value and use it for this interval (nulls will be ignored).
    - *min* - Same as max, but take minimal value.
    - *average* - Same as max, but take average value.
    - *total* - Same as max, but calculate total value.
    - *count* - Same as max, but calculate number of values (nulls will be calculated).
    - *none* - No aggregation at all. Only raw values in given period.

The first and last points will be calculated for aggregations, except aggregation "none".
If you manually request some aggregation you should ignore first and last values, because they are calculated from values outside of period.

## storeState
If you want to write other data into the InfluxDB you can use the build in system function **storeState**.
This function can also be used to convert data from other History adapters like History or SQL.

The given ids are not checked against the ioBroker database and do not need to be set up there, but can only be accessed directly.

The Message can have one of the following three formats:
* one ID and one state object
* one ID and array of state objects
* array of multiple IDs with state objects

## History Logging Management via Javascript
The adapter supports enabling and disabling of history logging via JavaScript and also retrieving the list of enabled datapoints with their settings.

### enable
The message requires to have the "id" of the datapoint.Additionally optional "options" to define the datapoint specific settings:

```
sendTo('history.0', 'enableHistory', {
    id: 'system.adapter.history.0.memRss',
    options: {
        changesOnly:  true,
        debounce:     0,
        retention:    31536000,
        maxLength:    3,
        changesMinDelta: 0.5,
        aliasId: ''
    }
}, function (result) {
    if (result.error) {
        console.log(result.error);
    }
    if (result.success) {
        //successfull enabled
    }
});
```

### disable
The message requires to have the "id" of the datapoint.

```
sendTo('history.0', 'disableHistory', {
    id: 'system.adapter.history.0.memRss',
}, function (result) {
    if (result.error) {
        console.log(result.error);
    }
    if (result.success) {
        //successfull enabled
    }
});
```

### get List
The message has no parameters.

```
sendTo('history.0', 'getEnabledDPs', {}, function (result) {
    //result is object like:
    {
        "system.adapter.history.0.memRss": {
            "changesOnly":true,
            "debounce":0,
            "retention":31536000,
            "maxLength":3,
            "changesMinDelta":0.5,
            "enabled":true,
            "changesRelogInterval":0,
            "aliasId": ""
        }
        ...
    }
});
```

## Data converter
### General idea
When you have more data over time then the history adapter may not be the best choice and a real database is better. For this there are two more History-Adapters for SQL databases (PostgreSQL, MS-SQL, MySQL, SQLite) and InfluxDB.
With this change the quesion comes up how to convert the collected data from the past to these new adapters.

For this some converter scripts have been prepared that can help and do the job. These scripts are called from the command line.

### Prepare and analyze existing data in transfer target
When converting data only those data should be transferred that are not already there. Therefor the first set of scripts exists called **analyze<db>.js**. This script should be executed once at the beginning to collect some data for existing data and store them in local .json files to be used by the real converter script.
Two kind of data are collected:
- **earliest value for datapoint ID**: The timestamp of the very first entry for each existing datapoint is stored and is used by imported to ignore all newer values by default. The assumption is that the data are filled completely beginning with this first entry and all earlier values would else be duplicated. This assumption can be overwritten on import by parameters.
- **existing values per day per datapoint ID**: The existing data are analyzed on a per day basis and each day is stored where data exist already. This can be used as alternative to the first data to be able to also fill "holes" in the data.

#### analyzeinflux.js
The analyzeinflux.js can be found in the directory "converter".
This script will collect the above mentioned data for an InfluxDB instance.

**Usage**: nodejs analyzeinflux.js [InfluxDB-Instance] [Loglevel] [--deepAnalyze]

**Example**: nodejs analyzeinflux.js influxdb.0 info --deepAnalyze

Parameters:
- **<InfluxDB-Instance>**: which influxdb-Adapter instance should be used? (Default: influxdb.0) If set needs to be first parameter after scriptname.
- **<Loglevel>**: Loglevel for output (Default: info). If set needs to be second parameter after scriptname.
- **--deepAnalyze**: collect the existing values per day too, by default only the earliest value is queried.

The script will then generate one or three .json files with the collected data. These files are then used by the real converter script.

#### analyzesql.js
The analyzesql.js can be found in the directory "converter".
This script will collect parts of the above mentioned data for an SQL instance.

**Usage**: nodejs analyzesql.js [<SQL-Instance>] [<Loglevel>]
**Example**: nodejs analyzesql.js sql.0 info

Parameters:
- **<SQL-Instance>**: which SQL-Adapter instance should be used? (Default: sql.0) If set needs to be first parameter after scriptname.
- **<Loglevel>**: Loglevel for output (Default: info). If set needs to be second parameter after scriptname.

The script will then generate two .json files with the collected data. These files are then used by the real converter script.
Currently --processNonExistingValuesOnly for converter script can not be used because the data are not collected.

### Convert History-Data to a DB
The history2db.js can be found in the directory "converter".

The script will directly use the generated JSON files from the history adapter on disk to transfer them into the Database.
Additionally it uses the pre-generated data files for already existing values in the target DB to only convert not existing data.

The script can be run without any analyze step beforehand then you need to set the startdata as parameter and it will simply convert anything from that timepoint backwards in time.
When you have run an analyze before and the earliestDBValues.json file exists then only these datapoints are converted, unless you use parameters to change that.
When an analyze was run before and the datafiles are used, they are also updated with all converted data, so a second run will normally not generate duplicates.
To reset the data delete the File "earliestDBValues.json", "existingDBValues.json" and/or "existingDBTypes.json".

The Converter then goes backward in time through all the days available as data and will determine which data to transfer to InfluxDB.

If you want to abort the process you can press "x" or "<CTRL-C>" and the converter aborts after the current datafile.

The converter script itself should work with all History adapters that support "storeState" methods.

Note: Migrating many data will produce a certain load on the system, especially when converter and target database instance are running on the same machine. Monitor your systems load and performance during the action and maybe use the "delayMultiplicator" parameter to increase delays in the converter.

**Usage:** nodejs history2db.js DB-Instance [Loglevel] [Date-to-start|0] [path-to-Data] [delayMultiplicator] [--logChangesOnly [relog-Interval(m)]] [--ignoreExistingDBValues] [--processNonExistingValuesOnly] [--processAllDPs]  [--simulate]
**Example**: nodejs history2db.js influxdb.0 info 20161001 /path/to/data 2 --logChangesOnly 30 --processNonExistingValuesOnly

Possible options and Parameter:
- **DB-Instance**: DB-Instance to send the data to.Required parameter. Needs to be first parameter after scriptname.
- **Loglevel**: Loglevel for output (Default: info). If set needs to be second parameter after scriptname.
- **Date-to-start**: Day to start in format yyyymmdd (e.g. 20161028). Use "0" to use detected earliest values. If set needs to be third parameter after scriptname.
- **path-to-Data**: Path to the datafiles. Defauls to iobroker-install-directory/iobroker-data/history-data . If set needs to be fourth parameter after scriptname.
- **<delayMultiplicator>**: Modify the delays between several actions in the script by a multiplicator. "2" would mean that the delays the converted had calculated by itself are doubled. If set needs to be fifth parameter after scriptname.
- **--logChangesOnly [relog-Interval(m)]**: when --logChangesOnly is set the data are parsed and reduced, so that only changed values are stored in InfluxDB. Additionally a "relog-Interval(s)"" can be set in minutes to re-log unchanged values after this interval.
- **--ignoreExistingDBValues**: With this parameter all existing data are ignored and all data are inserted into DB. Please make sure that no duplicates are generated. This option is usefull to fix "holes" in the data where some data are missing. By default it only fills all datapoints with at least one entry in the DB. This can be overwritten by --processAllDPs
- **--processNonExistingValuesOnly**: With this parameter the "existing datapoints by day" file from the analyze script is used and checked for each day and datapoint. In this mode the existing-DB-Values are always ignored, and also not updated, so please do another analyze run after using that mode!!!
- **--processAllDPs**: With this parameter you make sure that all existing datapoints from the history files is transferred into the DB, also if these are not existing in that DB so far.
- **--simulate**: With this parameter you enable the simulation mode, means that no real write happends and also the analyze-datafiles will not be updated on exit.

### Best practice when executing the conversion
If you move from one history method to another I propose the following process:
* activate the new history method (sql/influxdb) for the relevant states and start logging and check that this works as expected. This means you log "twice".
* Then run the analyze scripts to get the "cut off" points where the duplicate logging started.
* Then stop the history adapter and execute the migration (this can take some time). So the older values will be added to the new ones.
* Then when you are sure you did anything and also checked the error files and such delete the history json files to get some space back.

### Best practice when executing the conversion
If you move from one history method to another I propose the following process:
* activate the new history method (sql/influxdb) for the relevant states and start logging and check that this works as expected. This means you log "twice".
* Then run the analyze scripts to get the "cut off" points where the duplicate logging started.
* Then stop the history adapter and execute the migration (this can take some time). So the older values will be added to the new ones.
* Then when you are sure you did anything and also checked the error files and such delete the history json files to get some space back.