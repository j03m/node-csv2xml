var fs = require('fs');
var sys = require('util');
var spawn = require('child_process').spawn;
var exec = require('child_process').exec;
var builder = require('xmlbuilder');
var lazy = require('lazy');

var file = "source.csv";
//var file = "test.txt";
var desc = "fields.csv";
var useHeaders = 1;
//var desc = "descriptor.txt";
var recordsPerFile = 150000;
var trimQuotes=1;
//var recordsPerFile = 100;

var chunk = 0;
var lineCount = 0;
var totalExceptions = 0;
var writeStream = fs.createWriteStream("output_chunk" + chunk + ".xml");
writeStream.write("<root>", "ascii");
var pattern = /,(?!(?:[^",]|[^"],[^"])+")/;
var exceptionStream = fs.createWriteStream("exceptions.txt");

var fieldDescriptors;
if (useHeaders==0)
{
	fieldDescriptors = fs.readFileSync(desc, "ascii");
	fieldDescriptors = fieldDescriptors.trim().split(pattern);
} 


MyExec();

function MyExec()
{
	var count = 0;
	var stream = fs.createReadStream(file);

	stream.on('end', function()
	{
		writeStream.write("<\root>", "ascii");
	});
	
	new lazy(stream).lines.forEach(function(line){
		console.log(lineCount);
		if (useHeaders == 1 && lineCount == 0)
		{
			//parse headers
			fieldDescriptors = line.toString().trim().split(pattern);
			lineCount++;
			return;
		}


		var xml = CsvToXML([line.toString().trim()], chunk, fieldDescriptors,  "output");
		lineCount++;
		count++;
		writeStream.write(xml, "ascii");
		if (count>=recordsPerFile)
		{
		
			writeStream.write("</root>", "ascii");
			chunk++;
			count = 0;
			writeStream = fs.createWriteStream("output_chunk" + chunk + ".xml");	
			writeStream.write("<root>", "ascii");
		}
	});		
}
var FIXERR = 0; //custom error handling for my file...remove if you don't need.
function CsvToXML(data, chunkNumber, fieldDescriptors, output)
{
	
 	var lines = data;
	var doc = builder.create();

	
	for(var i =0; i<lines.length;i++)
	{
		var cols = lines[i].trim().split(pattern);
		
		//custom error handler, set FIXERR to 0 and ignore.
		if (FIXERR && cols.length == 429)
		{
			//colapse fields
			console.log("Fixing name issue: " + cols[72] + ", " + cols[73] );
			cols[72] = cols[72] + ", " + cols[73]; 
			cols.splice(73,1);
			console.log("New val: " + cols[72]);
		}
		
		if (cols.length == fieldDescriptors.length)
		{
			var root = doc.begin("row");
			for(var ii=0;ii<cols.length;ii++)
			{
				//console.log("field:" + fieldDescriptors[ii] + ":" + cols[ii]);
				if (fieldDescriptors[ii] != undefined )
				{
					if (cols[ii] == ""){
						cols[ii] = "\t";
					}
					if (trimQuotes == 1)
					{
						cols[ii]=cols[ii].replace(/\"/g, "");
					}
					root.ele(fieldDescriptors[ii]).txt(cols[ii]);
				}
			}	
		}
		else
		{
			totalExceptions++;			
			console.log("Format Exception line: " + lineCount + " total exceptions so far: " + totalExceptions);
			console.log(fieldDescriptors.length + " " + cols.length);			
			exceptionStream.write(lineCount, 'ascii');
			
		}
	}
	
	return doc.toString({pretty:true});
	
}
