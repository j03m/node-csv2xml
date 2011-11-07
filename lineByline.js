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
var writeStream = fs.createWriteStream("output_chunk" + chunk + ".xml");
writeStream.write("<root>", "utf-8");
var fieldDescriptors = fs.readFileSync(desc, "utf-8");
fieldDescriptors = fieldDescriptors.trim().split(',');

MyExec();

function MyExec()
{
	var count = 0;
	var stream = fs.createReadStream(file);

	stream.on('end', function()
	{
		writeStream.write("<\root>", "utf-8");
	});
	
	new lazy(stream).lines.forEach(function(line){
		console.log(lineCount);
		var xml = CsvToXML([line.toString().trim()], chunk, fieldDescriptors,  "output");
		lineCount++;
		count++;
		writeStream.write(xml, "utf-8");
		if (count>=recordsPerFile)
		{
		
			writeStream.write("</root>", "utf-8");
			chunk++;
			count = 0;
			writeStream = fs.createWriteStream("output_chunk" + chunk + ".xml");	
			writeStream.write("<root>", "utf-8");
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
				if (fieldDescriptors[ii]!=undefined)
				{
					root.ele(fieldDescriptors[ii]).txt(cols[ii]);
				}
			}	
		}
		else
		{
			exceptions++;
			console.log("Format Exception: Chunk: " + chunkNumber + " number: " + exceptions);
			console.log("Fields: " + fieldDescriptors.length + " columns in line: " + cols.length);
			
			console.log("Detected column length to descriptor mismatch. Line not processed.");			
			fs.writeFile(output + "_exceptions" + lineCount + ".csv", lines[i], function(err){
				if (err)
				{
					console.log("filed to write exception file.")
				}
				
			});
			
		}
	}
	
	return doc.toString({pretty:true});
	
}