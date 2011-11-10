var fs = require('fs');
var sys = require('util');
var spawn = require('child_process').spawn;
var exec = require('child_process').exec;

var defaultDescriptor = {
	"inputFile":"UsBiz.csv"
	"outputPrefix":"output",
	"outputPostFix":".xml",
	"outputEncoding":"utf-8",
	"onLine":undefined,
	"chunkSize":150000,
	"startChunk":0,
	"endChunk":-1,
	"procs":2,
	"useHeader":0,
	"fieldDescriptor":"fields.csv",
	"onComplete":function() { console.log ("done!"); },
	"onChunkStart":function(ws){ws.write("<root>", "ascii");},
	"onChunkEnd":function(ws){ws.write("</root>", "ascii");},
	"onHeaders":undefined
}

var pattern = /,(?!(?:[^",]|[^"],[^"])+")/;
var fieldDescriptors;
exports.processFile=(descriptor)
{
	var chunk =0;

	//merge, use default where undefined
	for (var attrname in descriptor) { 
		if (descriptor[attrname]!=undefined)
		{
			defaultDescriptor[attrname] = descriptor[attrname]; 
		}	
	}
	
	if (defaultDescriptor.onLine == undefined) { throw "Requires a line processing function."; }
	
	//set up streams
	makeInputStream(defaultDescriptor.inputFile);
	
	var chunk = defaultDescriptor.startChunk;
	makeOutputStream(defaultDescriptor.outputPrefix + chunk + defaultDescriptor.outputPostFix);
	if (!defaultDescriptor.useHeaders)
	{
		if (defaultDescriptor.fieldDescriptor == undefined) { throw "Either field descriptors must be set or useHeaders must be true"; }
		fieldDescriptors = fs.readFileSync(defaultDescriptor.fieldDescriptor, "ascii");
		fieldDescriptors = fieldDescriptors.trim().split(pattern);
		invoke('onHeaders',fieldDescriptors);
	}
		
}

function invoke(name,params)
{
		if (defaultDescriptor[name] != undefined)
		{
			defaultDescriptor[name](params);
		}
}

var stream;
function makeInputStream(inputFile)
{
	stream = fs.createReadStream(inputFile);
	var buffer = "";
	stream.on('end', function()
	{
		console.log("read stream closing.");
		invokeOnChunkEnd();
		writeStream.end();
		writeStream.destroySoon();
		invoke('onComplete');
	});
	
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
	
}

var writeStream;
function makeOutputStream()
{
		writeStream = fs.createWriteStream(pathAndPrefix + chunk + ".xml");	
		writeStream.on('drain', function(){
			console.log("drain");
			stream.resume();
		});
		invoke('onChunkStart',writeStream);
}

function parseLine(line)
{
		if (useHeaders == 1 && lineCount == 0)
		{
			//parse headers
			fieldDescriptors = line.split(pattern);
			invoke('onHeaders',writeStream);;
			lineCount++;
			return;
		}
		
		
		var record = defaultDescriptor.onLine(fieldDescriptors, line);
		lineCount++;
		count++;
	
		if (!writeStream.write(record, "ascii"))
		{
			stream.pause();
		}
	
		if (count>=defaultDescriptor.recordsPerFile)
		{
				
			invoke('onChunkEnd',writeStream);
			writeStream.end();
			writeStream.destroySoon();
			chunk++;
			count = 0;
			makeWriteStream();
		}
	
}


