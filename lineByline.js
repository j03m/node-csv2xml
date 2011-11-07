var fs = require('fs');
var sys = require('util');
var spawn = require('child_process').spawn;
var exec = require('child_process').exec;
var builder = require('xmlbuilder');
var lazy = require('lazy');

var file = "UsBiz.csv";
//var file = "test.txt";
var desc = "fields.csv";
//var desc = "descriptor.txt";
var recordsPerFile = 150000;
//var recordsPerFile = 100;

var chunk = 0;
var lineCount = 0;
var totalExceptions = 0;
var writeStream = fs.createWriteStream("output_chunk" + chunk + ".xml");
writeStream.write("<root>", "ascii");

var exceptionStream = fs.createWriteStream("exceptions.txt", 'ascii');

var fieldDescriptors = fs.readFileSync(desc, "ascii");
fieldDescriptors = fieldDescriptors.trim().split(',');

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
 
function CsvToXML(data, chunkNumber, fieldDescriptors, output)
{
	
 	var lines = data;
	var exceptions = 0;
	var pattern = /,(?!(?:[^",]|[^"],[^"])+")/;
	var doc = builder.create();

	
	for(var i =0; i<lines.length;i++)
	{
		var cols = lines[i].trim().split(pattern);
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
					root.ele(fieldDescriptors[ii]).txt(cols[ii]);
				}
			}	
		}
		else
		{
			totalExceptions++;	
			console.log("Format Exception line: " + lineCount + " total exceptions so far: " + totalExceptions);			
			exceptionStream.write(lineCount, 'ascii');
			
		}
	}
	
	return doc.toString({pretty:true});
	
}
