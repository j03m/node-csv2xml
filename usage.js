var lineByline = require('./linebyline');

var desc = {"onLine":CsvToXML};
lineByline.processFile(desc);


var FIXERR = 1;
function CsvToXML(line)
{
	
	var builder = require('xmlbuilder');
	var doc = builder.create();

	var dupeColhandler = {};	
	var cols = line.trim().split(pattern);

	//custom error handler, set FIXERR to 0 and ignore.
	if (FIXERR && cols.length == 429)
	{
		//colapse fields
		//console.log("Fixing name issue: " + cols[72] + ", " + cols[73] );
		cols[72] = cols[72] + ", " + cols[73]; 
		cols.splice(73,1);
		//console.log("New val: " + cols[72]);
	}

	if (cols.length == fieldDescriptors.length)
	{
		var root = doc.begin("row");
		for(var ii=0;ii<cols.length;ii++)
		{

			if (fieldDescriptors[ii] != undefined )
			{
				if (cols[ii] == ""){
					cols[ii] = "\t";
				}
				if (trimQuotes == 1)
				{
					cols[ii]=cols[ii].replace(/\"/g, "");
				}

				var colName = fieldDescriptors[ii];
				var colNameCheck = colName;
				var count = 1;
				if(dupeColhandler[colName]!=undefined)
				{
					while(dupeColhandler[colNameCheck]!=undefined){
						colNameCheck = colName + count;
						count++; 
					}

					root.ele(colNameCheck).txt(cols[ii]);	
					dupeColhandler[colNameCheck] = 1;		
				}
				else
				{
					root.ele(colName).txt(cols[ii]);	
					dupeColhandler[colName] = 1;
				}
			}
		}	
	}
	
	
	var ret = doc.toString({pretty:true});
	doc = null;
	builder = null;
	root = null;
	return ret;
	
}