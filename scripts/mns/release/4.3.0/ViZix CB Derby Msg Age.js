/*
*  Core Bridge Message Age
*
*/
function( options ) {
    //var options = {"pageSize": 1, "pageNumber": 50}
    //var start = ISODate("2016-06-12 20:00:00.000BOT");
    //var end = ISODate("2016-09-13 04:00:00.000BOT");

    
    var numberOfHours = 8;

    var now = new Date();
    var start = new Date(Math.floor(now.getTime() / 60000) * 60000 - numberOfHours * 3600 * 1000);
    var end = now;
    
    var utcOffset = -4;
    
    
    function timestampToHHMM( timestamp ) {
        function pad(s) { return (s < 10) ? '0' + s : s; }
        var d = new Date( timestamp );
        return [ pad( d.getUTCHours() ), pad( d.getUTCMinutes() ) ].join( ':' );
    }
    
    var table = new Object();

    table.options = options;
    table.title = "ViZix Core Bridge Derby Message Age";
    table.labelX = "Message Age (seconds)";
    table.labelY = "Time (UTC" + utcOffset + ")";

    var colHeader = [];
    var rowHeader = [];

    var res = db.getCollection( 'thingSnapshots' )
        .find( { "value.thingTypeCode" : {$in : ["edgeBridge","coreBridge"] }, 
                 "time": { "$gte": start, "$lte": end },
                 'value.serialNumber': {"$in" : [/.*Derby.*/, /.*DERBY.*/] }, 
                 "value.age": { "$exists": true } } )
        .sort( { "value.age.time" : 1, 
                'x.value.serialNumber' : 1 } )
        //.limit(500)
        .map( function( x ) {  
            var rounded = Math.floor( x.value.age.time / 60000 ) * 60000;
            return [ rounded, x.value.serialNumber, x.value.age.value ];
        } );
    
    var map = {};
    var index = 0;
    var result = [];
    var row = [];
    var lastt = 0;
    for( var i = 0; i < res.length; i++ )
    {
        var timestamp = res[i][0];
        var serialNumber = res[i][1];
        var value = res[i][2];
        
        if( lastt == 0 )
        {
            lastt = timestamp;
        }
       
        if( map[serialNumber] == undefined )
        {
            colHeader.push( serialNumber );
            map[serialNumber] = index;
            index++;
        }
        
        if( timestamp != lastt )
        {
            row = [];
            rowHeader.push( timestampToHHMM( timestamp + utcOffset * 3600 * 1000 ) );
            result.push( row );
            lastt = timestamp;
        }
        
        //row[ map[serialNumber] ] = value.toFixed( 2 );
        row[ map[serialNumber] ] = parseFloat(value).toFixed(2);
    }
    
    for( var i = 0; i < result.length; i++ )
    {
         for( var j = 0; j < index; j++ )
        {
            if( ! result[i][j] )
            {
                //result[i][j] = 0;
                result[i][j] = -0.1;
            }
        }
    }
    
    // optional
    table.columnNames = colHeader;

    // optional
    table.rowNames = rowHeader;

    // required
    table.data = result;

    return table;
}