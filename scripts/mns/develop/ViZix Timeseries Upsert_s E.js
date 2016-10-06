/**
 * @author  Cristian Vertiz
 * @date    06/16/2016
 *
 */
function (options) {
    //var options = {pageSize: 10, pageNumber: 1};
    // the time bucket size, in minutes
    var window = 1;

    // set to your local UTC offset
    var utcOffset = -4;  // -4 for Bolivia

    var numberOfHours = 8;

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

    a = db.thingSnapshots.aggregate(
        [
            {"$match": {"value.thingTypeCode": {"$in": ['item']},
                "time": {"$gte": start, "$lte": end},
                "value.source.value" : { $exists: true } }
            },
            {"$project" : {
                "_id" : "$_id",
                "serialNumber" : "$value.serialNumber",
                "source" : "$value.source.value",
                //"time" : "$time"
                "time" : "$value.modifiedTime"
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
                        //"time": "$time",
                        "time" : "$value.modifiedTime",
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
    ).map(function (x) {
        var minStart = (x._id.range * window);
        if (minStart.toString().length == 1) {
            minStart = "0" + minStart;
        }
        var d = new Date(x._id.year, x._id.month, x._id.day, x._id.hour, minStart, 0, 0);
        var rowHeader = timestampToHHMM(d.getTime() + utcOffset * 3600 * 1000);

        if (!table.rowNames.contains(rowHeader)){
            table.rowNames.push(rowHeader);
        }
        var colHeader = x._id.source;
        if (!table.columnNames.contains(colHeader)){
            table.columnNames.push(colHeader);
        }
        var value = parseFloat((NumberInt(x.count) / (window * 60))).toFixed(2);

        if(result[colHeader]){
            result[colHeader][rowHeader] = value;
        }else{
            var row = {};
            row[rowHeader] = value;
            result[colHeader] = row;
        }

    });

    table.data = [];

    for (rowKey = 0; rowKey < table.rowNames.length; rowKey++) {
        var rowResult = [];
        var total = 0.0;
        for (colKey = 0; colKey < table.columnNames.length; colKey++) {
            if(result[table.columnNames[colKey]][table.rowNames[rowKey]]){
                total += parseFloat(result[table.columnNames[colKey]][table.rowNames[rowKey]]);
                rowResult.push(result[table.columnNames[colKey]][table.rowNames[rowKey]]);
            }else{
                rowResult.push(0);
            }
        }
        rowResult.push(total.toFixed(2));
        table.data.push(rowResult)
    }

    table.columnNames.push("Total");
    //     table.data = result;


    //table.data.splice(-1, 1);

    return table;
}