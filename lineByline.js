var fs = require('fs');
var sys = require('util');
var spawn = require('child_process').spawn;
var exec = require('child_process').exec;


var file = "UsBiz.csv";
//var file = "source.csv";
var desc = "fields.csv";
var useHeaders = 0;
var recordsPerFile = 150000;
var trimQuotes=1;
var stop = -1;
var FIXERR = 1; //custom error handling for my file...remove if you don't need.
var chunk = 0;
var lineCount = chunk*recordsPerFile;
var totalExceptions = 0;
var pathAndPrefix = "temp/output_chunk";
var writeStream = fs.createWriteStream(pathAndPrefix + chunk + ".xml");


var pattern = /,(?!(?:[^",]|[^"],[^"])+")/;
var exceptionStream = fs.createWriteStream("exceptions.txt");

var fieldDescriptors;
if (useHeaders==0)
{
	fieldDescriptors = fs.readFileSync(desc, "ascii");
	fieldDescriptors = fieldDescriptors.trim().split(pattern);
} 


var count = 0;
var stream = fs.createReadStream(file);

stream.on('end', function()
{
	writeStream.write("</root>", "ascii");
	writeStream.end();
	writeStream.destroySoon();
});

writeStream.on('drain', function(){
	stream.resume();
})

var buffer = "";

writeStream.write("<root>", "ascii");
stream.on('data', function(data){
	buffer+=data;
	
    var index = buffer.indexOf('\n');
    while (index > -1) {
      var line = buffer.substring(0, index);
      buffer = buffer.substring(index + 1);
      parseLine(line);
      index = buffer.indexOf('\n');
    }
});
	
function parseLine(line)
{
		console.log(lineCount);
		if (useHeaders == 1 && lineCount == 0)
		{
			//parse headers
			fieldDescriptors = line.split(pattern);
			lineCount++;
			return;
		}
		
		var xml = CsvToXML([line], chunk);
		lineCount++;
		count++;
		if (!writeStream.write(xml, "ascii"))
		{
			stream.pause();
		}
		xml=null;
		if (count>=recordsPerFile)
		{
		
			if (!writeStream.write("</root>", "ascii"))
			{
				stream.pause();
			}
			writeStream.end();
			writeStream.destroySoon();
			chunk++;
			count = 0;
			writeStream = fs.createWriteStream(pathAndPrefix + chunk + ".xml");	
			if (!writeStream.write("<root>", "ascii"))
			{
				stream.pause();
			}
		}
	
}


function CsvToXML(data)
{
	
 	var lines = data;
	var builder = require('xmlbuilder');
	var doc = builder.create();

	var dupeColhandler = {};	
	for(var i =0; i<lines.length;i++)
	{
		var cols = lines[i].trim().split(pattern);
		
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
	
	}
	
	//var ret = "hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh";
	//ret = ret + ret + ret + ret + ret + ret + ret + ret + ret + ret + ret;
	var ret = doc.toString({pretty:true});
	doc = null;
	builder = null;
	root = null;
	return ret;
	
}
