/**
 * @author  Cristian Vertiz
 * @date    06/16/2016
 *
 */
function (options) {


    function cleanFormat(val) {
        var out = val.toString();
        out = out.replace(/ISODate\((.*?)\)/g, "$1");
        out = out.replace(/NumberLong\((.*?)\)/g, "$1");
        out = out.replace(/NumberInt\((.*?)\)/g, "$1");
        out = out.replace(/BinData\((.*?)\)/g, "$1");
        out = out.replace(/ObjectId\((.*?)\)/g, "$1");
        out = out.replace(/Timestamp\((.*?)\)/g, "$1");
        out = out.replace(undefined, "$1");
        return out;
    }

    function makeRow(row) {
        if (options.pageSize == -1) {
            print(row);
        } else {
            output.push(row);
        }
    }

    // the time bucket size, in minutes
    var window = 1;

    // set to your local UTC offset
    var utcOffset = -4;  // -4 for Bolivia

    var numberOfHours = 7;

    var now = new Date();
    var start = new Date(Math.floor(now.getTime() / 60000) * 60000 - numberOfHours * 3600 * 1000);
    //var end = new Date(Math.floor(now.getTime() / 60000) * 60000 - 6 * 3600 * 1000);
    var end = now;

    //var start = ISODate("2016-07-31 00:00:00.000");  // Add utcOffset to get offset time 
    //var end   = ISODate("2016-07-31 05:00:00.000");  // Add utcOffset to get offset time 

    function timestampToHHMM(timestamp) {
        function pad(s) {
            return (s < 10) ? '0' + s : s;
        }

        var d = new Date(timestamp);
        return pad(d.getUTCMonth()) + "/" + pad(d.getUTCDate()) + " " + [pad(d.getUTCHours()), pad(d.getUTCMinutes())].join(':');
    }

    Array.prototype.contains = function (obj) {
        var i = this.length;
        while (i--) {
            if (this[i] === obj) {
                return true;
            }
        }
        return false;
    };

    var table = new Object();
    table.options = JSON.stringify(options);
    table.title = "ViZix Timeseries Upsert/s (by Source), External Timestamp";
    table.labelX = "Upserts/second";
    table.labelY = "Time (UTC" + utcOffset + ")";

    table.columnNames = [];
    table.rowNames = [];

    var result = {};

    var a = db.thingSnapshots.aggregate(
        [
            {"$match": {'value.thingTypeCode': {$in: ['default_rfid_thingtype']}, 
            time: {$gte: start, $lte: end}//, 
//            'value.source.value' : { $exists: true } 
            } },
            {"$project" : {
                "_id" : "$_id",
                "serialNumber" : "$value.serialNumber",
                "source" : "$value.source.value",    
                "time" : "$value.modifiedTime",
                // "time" : {"$add": [new Date(0), "$value.tsCoreIn.value"]},
//                 "ts" : "$value.tsCoreIn.value"
            }},
            {"$sort": {"time": 1}},
            {
                "$group": {
                    "_id": {
                        "range": {"$divide": [{"$subtract": [{"$minute": "$time"}, {"$mod": [{"$minute": "$time"}, window]}]}, window]},
                        "minute": {"$minute": "$time"},
                        "hour": {"$hour": "$time"},
                        "day": {"$dayOfMonth": "$time"},
                        "month": {"$month": "$time"},
                        "year": {"$year": "$time"},
                        "time": "$value.modifiedTime",
                        "serialNumber": "$serialNumber",
                        "source": "$source"
                    }
                }
            },
            {
                "$group": {
                    "_id": {
                        "range": "$_id.range",
                        "hour": "$_id.hour",
                        "day": "$_id.day",
                        "month": "$_id.month",
                        "year": "$_id.year",
                        "source": "$_id.source"
                    },
                    "count": {"$sum": 1}
                }
            },
            {"$sort": {"_id.year": 1, "_id.month": 1, "_id.day": 1, "_id.hour": 1, "_id.range": 1}}
        ],
        {allowDiskUse: true}
    ).map(function (xX) {

        var minStart = (xX._id.range * window);
        if (minStart.toString().length == 1) {
            minStart = "0" + minStart;
        }
        // var minEnd = ((x._id.range + 1) * window) - 1;
        // if (minEnd.toString().length == 1) {
        //     minEnd = "0" + minEnd;
        // }
        //var date = x._id.year + "-" + x._id.month + "-" + x._id.day + " " + x._id.hour + ":" + minStart;

        //var d = new Date(year, month, day, hours, minutes, seconds, milliseconds);
        var d = new Date(xX._id.year, xX._id.month, xX._id.day, xX._id.hour, minStart, 0, 0);

        //rowHeader.push(date);
        var rowHeader = timestampToHHMM(d.getTime() + utcOffset * 3600 * 1000);
        if (!table.rowNames.contains(rowHeader)){
            table.rowNames.push(rowHeader);
        }
        var colHeader = xX._id.source;
        if(colHeader == undefined){
            colHeader = "undefined";
        }
        if (!table.columnNames.contains(colHeader) && options.pageSize != -1){
            table.columnNames.push(colHeader);
        }

        var value = parseFloat((NumberInt(xX.count) / (window * 60))).toFixed(2);
        
        if(result[colHeader]){
            result[colHeader][rowHeader] = value;
        }else{
            var row = {};
            row[rowHeader] = value;
            result[colHeader] = row;    
        }
        

        //return [NumberInt(x.count),(NumberInt(x.count)/(window*60)).toFixed(2) ];
        // return [ (NumberInt(x.count)/(window*60)).toFixed(2) ];
    });

    table.data = [];
    
    var output = [];
    for (rowKey = 0; rowKey < table.rowNames.length; rowKey++) {
        var rowResult = [];
        var total = 0.0;
        for (colKey = 0; colKey < table.columnNames.length; colKey++) {
            if(result[table.columnNames[colKey]][table.rowNames[rowKey]]){
                total += parseFloat(result[table.columnNames[colKey]][table.rowNames[rowKey]]);
//                makeRow(cleanFormat(result[table.columnNames[colKey]][table.rowNames[rowKey]]));
                rowResult.push(result[table.columnNames[colKey]][table.rowNames[rowKey]]);
            }else{
                rowResult.push(0);
//                makeRow(cleanFormat(0));
            }
        }
         rowResult.push(total.toFixed(2));
        //makeRow(cleanFormat(total.toFixed(2)));
        if (options.pageSize != -1){
            table.data.push(output);
        }
    }

     table.data = result;

        table.columnNames.push("Total");


    var arr =[];
    var b = table.data.undefined;
    for( var i in b ) {
        if (b.hasOwnProperty(i)){
            //arr.push(parseInt(b[i]));
            arr.push([
               //i.split(" ")[1],
               parseFloat(b[i])
            ]);
        }
    }
    table.data = arr;
    
    table.options = JSON.parse(table.options);
    table.columnNames = ["SPARK CB"];

    // table.data.splice(-1, 1);
    // if (options.pageSize != -1){
        return table;
    // } else {
    //     makeRow(table.columnNames);
    //     for (i=0;i<table.data.size();i++){
    //         makeRow(table.data[i]);
    //     }
    // }
 
}