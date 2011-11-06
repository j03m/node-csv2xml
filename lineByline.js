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
var doc = builder.create();
var root = doc.begin("root");
var row = "row";
var chunk = 0;

var fieldDescriptors = fs.readFileSync(desc, "utf-8");
fieldDescriptors = fieldDescriptors.trim().split(',');

MyExec();

function MyExec()
{
	var count = 0;
	var stream = fs.createReadStream(file);
	
	stream.on('end', function()
	{
		fs.writeFile("output_chunk" + chunk + ".xml", doc.toString({pretty:true}), function(err){
			console.log("chunk written.")
		});	
	});
	
	new lazy(stream).lines.forEach(function(line){
		CsvToXML([line.toString().trim()], chunk, fieldDescriptors,  "output");
		count++;
		if (count>=recordsPerFile)
		{
		
			fs.writeFile("output_chunk" + chunk + ".xml", doc.toString({pretty:true}), function(err){
				console.log("chunk written.")
			});	
			
			//make new doc
			doc = builder.create();
			root = doc.begin("root");
			chunk++;
			count = 0;
			
		}
	});		
}
 
function CsvToXML(data, chunkNumber, fieldDescriptors, output)
{
	
 	var lines = data;
	var exceptions = 0;
	var pattern = /,(?!(?:[^",]|[^"],[^"])+")/;
	for(var i =0; i<lines.length;i++)
	{
		var cols = lines[i].trim().split(pattern);
		if (cols.length == fieldDescriptors.length)
		{
			var ele = root.ele(row);
			for(var ii=0;ii<cols.length;ii++)
			{
				//console.log("field:" + fieldDescriptors[ii] + ":" + cols[ii]);
				if (fieldDescriptors[ii]!=undefined)
				{
					ele.ele(fieldDescriptors[ii]).txt(cols[ii]);
				}
			}	
		}
		else
		{
			exceptions++;
			console.log("Format Exception: Chunk: " + chunkNumber + " number: " + exceptions);
			console.log("Fields: " + fieldDescriptors.length + " columns in line: " + cols.length);
			
			console.log("Detected column length to descriptor mismatch. Line not processed.");			
			fs.writeFile(output + "_exceptions" + chunkNumber + "_" + exceptions + ".csv", lines[i], function(err){
				if (err)
				{
					console.log("filed to write exception file.")
				}
				
			});
			
		}
	}
	
}