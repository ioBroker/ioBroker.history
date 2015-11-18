// todo     add cache data
// todo     error tests
// todo     more speed/performance ???
// todo     clean up


var fs        = require('fs');

var options =JSON.parse(process.argv[2]);

var agg_start;
var agg_end;
var agg_step;
var agg_stepEnd;
var agg_result ;
var agg_iStep ;
var _data;
var agg_value;
var agg_count;


function getDirectories(path) {
    return fs.readdirSync(path).filter(function (file) {
        return fs.statSync(path + '/' + file).isDirectory();
    });
}

function getFileData(id, options, callback) {
    var day_start = options.start ? ts2day(options.start) : null;
    var day_end = ts2day(options.end);
    var data = [];

    // get list of directories
    var day_list = getDirectories(options.path).sort(function (a, b) {
        return a - b;
    });
    var filedata
    // get all files in directory
    for (var i in day_list) {
        var day = parseInt(day_list[i]);
        if (!isNaN(day)) {
            if (day > day_end) {
                break;
            } else if (day >= day_start && day <= day_end) {

                var file = options.path + day_list[i].toString() + '/history.' + id + '.json';

                if (fs.existsSync(file)) {

                    try {

                        aggregate(JSON.parse(fs.readFileSync(file)).sort(function (a, b) {
                            return b.ts - a.ts
                        }))

                    } catch (e) {
                        console.log('Cannot parse file ' + file + ': ' + e.message);
                    }
                }
            }
        }
    }

    pushAggregateResult();


    callback(data);
}

function initAggregate(options){
    //agg_options = {
    //    start: Math.round(options.start / 1000),
    //    end : Math.round(options.end / 1000),
    //    aggregate: 'average',
    //    count: 365
    //};
    //if (typeof data[0].val !== 'number') {
    //    return {result: data, step: 0, sourceLength: data.length};
    //}


    agg_start = new Date(options.start * 1000);
    agg_end   = new Date(options.end * 1000).getTime() ;

    agg_step = 1; // 1 Step is 1 second
    if (options.step != null) {
        agg_step = options.step;
    } else{
        agg_step = parseFloat((options.end - options.start) / options.count);
    }

    // Limit 2000
    if ((options.end - options.start) / agg_step > options.limit){
        agg_step = (options.end - options.start) / options.limit;
    }


    agg_value = null;
    agg_count = 0;
    agg_result = [];
    agg_iStep = 0;
    agg_stepEnd = agg_start.getTime() + (agg_step * 1000);
    options.aggregate = options.aggregate || 'average';
};

function pushAggregateResult(){

    if (options.aggregate == 'average') {
        if (!agg_count) {
            agg_value = null;
        } else {
            agg_value /= agg_count;
            agg_value = Math.round(agg_value * 100) / 100;
        }
        agg_count = 0;
    }

    // todo testen ob push schneller ist ???
    //if (agg_value !== null || !options.ignoreNull) {
    agg_result[agg_iStep] = {ts: agg_stepEnd / 1000 };
    agg_result[agg_iStep].val = agg_value;

    agg_iStep++;
    //}
    agg_value = null;
    agg_start = agg_stepEnd;
    agg_stepEnd += (agg_step *1000);
    return null
}

function aggregate(data ) {

    while (0 < data.length && data[data.length -1 ].ts *1000 < agg_end) {


        while (0 < data.length && data[data.length - 1 ].ts * 1000 < agg_stepEnd ) {

            _data = data.pop();

            if (options.aggregate == 'max') {
                // Find max
                if (agg_value === null || _data.val > agg_value) agg_value = _data.val;
            } else if (options.aggregate == 'min') {
                // Find min
                if (agg_value === null || _data.val < agg_value) agg_value = _data.val;
            } else if (options.aggregate == 'average') {
                if (agg_value === null) agg_value = 0;
                agg_value += _data.val;
                agg_count++;
            } else if (options.aggregate == 'total') {
                // Find sum
                if (agg_value === null) agg_value = 0;
                agg_value += parseFloat(_data.val);
            }
        }

        //if (data.length > 0 ) {
        //    pushAggregateResult()
        //}else{
        //    agg_stepEnd += (agg_step *1000);
        //}


        if (data.length > 0) {

            if (data[data.length - 1].ts * 1000 > agg_start) {

                pushAggregateResult()
            } else {

                data.pop()
            }
        }

    }
    return null;
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

initAggregate(options);
getFileData(options.id, options, function () {
    process.send(["response",agg_result, agg_step])

})