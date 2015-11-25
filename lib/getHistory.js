// todo     add cache data
// todo     error tests
// todo     clean up

var fs = require('fs');

var options = JSON.parse(process.argv[2]);

var overallLength = 0;
var averageCount;
var aggregate;
var maxIndex;
var start;
var end;
var step;
var result;
var value;

var finish = false;

process.on('message', function (msg){
    if (msg[0] == 'cacheData'){
        if (msg[1]){
            aggregation(msg[1]);
        }

        if (finish){
            response();
        }else{
            finish = true;
        }
    }
});

function getDirectories(path) {
    return fs.readdirSync(path).filter(function (file) {
        return fs.statSync(path + '/' + file).isDirectory();
    });
}

function getFileData() {

    var dayStart = ts2day(start);
    var dayEnd   = ts2day(end);
    var dayList  = getDirectories(options.path);
    var fileData;

    for (var i in dayList) {
        var day = parseInt(dayList[i]);

        if (!isNaN(day) && day > 20100101) {
            if (day >= dayStart && day <= dayEnd) {
                var file = options.path + dayList[i].toString() + '/history.' + options.id + '.json';
                
                if (fs.existsSync(file)) {
                    try {
                        fileData = JSON.parse(fs.readFileSync(file));
                    } catch (e) {
                        fileData = null;
                    }
                    if (fileData) aggregation(fileData);
                }
            }
        }
    }
    return null;
}

function initAggregate() {

    start = options.start;
    end   = options.end;

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

    maxIndex = ((end - start) / step) - 1;
    result = [];
    averageCount = [];
    aggregate = options.aggregate || 'average';

    for (var i= 0; i <= maxIndex; i++){
        result[i] = {
            ts:  Math.round(start + ((i + 0.5) * step )),
            val: null
        };

        if (aggregate == 'average') {
            averageCount[i] = 0;
        }
    }
}

function aggregation(data) {
    var index;
    for (var i in data) {
        index = Math.round((data[i].ts - start) / step);

        if (index > -1 && index <= maxIndex) {
            overallLength++;
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

function response(){
    if (aggregate == 'average') {
        for (var ii = 0; ii < result.length; ii++) {
            result[ii].val = result[ii].val ? Math.round(result[ii].val / averageCount[ii] * 100) / 100 : null;
        }
    }
    process.send(['response', result, overallLength, step]);
    setTimeout(function(){
        process.exit();
    }, 500);
}

initAggregate();
process.send(['getCache']);
getFileData();

if (finish) {
    response();
} else {
    finish = true;
}


