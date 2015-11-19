// todo     add cache data
// todo     error tests
// todo     clean up

var fs = require('fs');

var options = JSON.parse(process.argv[2]);

var overall_length;
var averageCount;
var aggregate;
var maxIndex;
var start;
var end;
var step;
var result;
var value;

function getDirectories(path) {
    return fs.readdirSync(path).filter(function (file) {
        return fs.statSync(path + '/' + file).isDirectory();
    });
}

function getFileData() {
    var day_start = ts2day(start);
    var day_end = ts2day(end);
    var day_list = getDirectories(options.path)
    var filedata;

    for (var i in day_list) {
        var day = parseInt(day_list[i]);

        if (!isNaN(day)) {
            if (day >= day_start && day <= day_end) {
                var file = options.path + day_list[i].toString() + '/history.' + options.id + '.json';
                if (fs.existsSync(file)) {
                    try {
                        filedata = JSON.parse(fs.readFileSync(file))
                    } catch (e) {
                        filedata = null;
                        console.log('Cannot parse file ' + file + ': ' + e.message);
                    }
                    if (filedata) {
                        aggretation(filedata)
                    }
                }
            }
        }
    }

    if (aggregate == 'average') {
        for (var ii = 0; ii < result.length; ii++) {
            result[ii].val = Math.round(result[ii].val / averageCount[ii] * 100) / 100;
        }
    }

    return null
}

function initAggregate() {


    start = options.start;
    end = options.end;

    step = 1; // 1 Step is 1 second
    if (options.step != null) {
        step = options.step;
    } else {
        step = (options.end - options.start) / options.count;
    }

    // Limit 2000
    if ((options.end - options.start) / step > options.limit) {
        step = (options.end - options.start) / options.limit;
    }

    maxIndex = ((end - start) / step) -1;
    result = [];
    averageCount = [];
    aggregate = options.aggregate || 'average';
}

function aggretation(data) {
    var index;
    for (var i in data) {
        index = Math.round((data[i].ts - start) / step);

        if (index > -1 && index <= maxIndex) {
            overall_length++;
            if (!result[index]) {
                result[index] = {
                    'ts': Math.round(start + ((index + 0.5) * step )),
                    'val': null
                };
                if (aggregate == 'average') {
                    averageCount[index] = 0;
                }
            }
            if (aggregate == 'max') {
                if (result[index] == null || result[index].val < data[i].val) result[index].val = data[i].val;
            } else if (aggregate == 'min') {
                if (result[index] == null || result[index].val > data[i].val) result[index].val = data[i].val;
            } else if (aggregate == 'average') {
                if (value === null) value = 0;
                result[index].val += data[i].val;
                averageCount[index]++;
            } else if (aggregate == 'total') {
                if (value === null) value = 0;
                result[index].val += parseFloat(data[i].val);
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

initAggregate();
getFileData(function () {
    process.send(["response", result, overall_length, step])
});
