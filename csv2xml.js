var fs = require('fs');
var sys = require('util');
var exec = require('child_process').exec;
var builder = require('xmlbuilder'); 

function usage()
{
	console.log("node csv2xml.js inputFileName numberOfChunks fieldDescriptor root row outputFilePrefix");
	console.log("inputFileName = the file you want to process.");
	console.log("numberOfChunks = numeric value indicating the number of output files.");
	console.log("filedDescriptors = a file with the comma separated list of column names, these will be tag names under row");
	console.log("root = root tag");
	console.log("row = row tag");
	console.log("outputFilePrefix - files will be named this + chunk + number.xml");
	process.argv.forEach(function(val, index, array){console.log(index + " " + val);});
	process.exit();
}


//get filename from options passed in
var filename = process.argv[2];
var chunks = process.argv[3];
var fieldDescriptors = fs.readFileSync(process.argv[4], "utf-8");
fieldDescriptors = fieldDescriptors.trim().split(',');

var root = process.argv[5];
var row = process.argv[6];
var output = process.argv[7];
if (output == undefined || row == undefined || root == undefined || filename == undefined || chunks == undefined || isNaN(chunks)  || chunks<0 || fieldDescriptors == undefined )
{
	usage();
}


//run wc to get the length
var child = exec('wc -l ' + filename, function(error, stdout, stderr)
{
	if (error != undefined) {
		console.log("err: couldn't get file length: " + error);
		process.exit();
	}
	
	
	var length = parseInt(stdout);
	console.log("file length: " + length);
	
	//get the lengths of the chunks to process
	var chunkLen = Math.floor(length/chunks);
	console.log("chunklen (fileLength/chunks): " + chunkLen )
	var leftOver = length - (chunkLen * chunks);
	console.log("remainder: " + leftOver);
	if (leftOver > 0 && leftOver < 1)
	{
		leftOver = 0;
	}
	
	//loop and use sed chunk out the files and then convert them to XML
	for(var i =0; i<chunks; i++)
	{
		MyExec(i, chunkLen, leftOver, filename, fieldDescriptors);
	}
});

function MyExec(chunk, chunkLen, leftOver, filename, fieldDescriptors)
{
	var chunkStart = (chunk*chunkLen) + 1;
	var chunkEnd = chunkStart + chunkLen;
	

	var sed = 'sed -n ' + chunkStart + "," + chunkEnd + "p " + filename;
	console.log(sed);
	//todo: this should really be spawn. Exec has buffer limits.
	exec(sed,{maxBuffer: 5000*1024}, function(error, stdout, stderr){
		if (error!=undefined){
			console.log("Err: couldn't chunk the files: " + error)
			process.exit();
		}
		
		CsvToXML(stdout, chunk, fieldDescriptors)
		
	});
	
}
 
function CsvToXML(data, chunkNumber, fieldDescriptors)
{
	console.log("INCOMING CHUNK " + chunkNumber + ":")
	var doc = builder.create();
	var docRoot = doc.begin(root);
 	var lines = data.split('\n');
	console.log("lines in chunk: " + lines.length);
	for(var i =0; i<lines.length;i++)
	{
		var cols = lines[i].trim().split(',');
		if (cols.length == fieldDescriptors.length)
		{
			console.log("columns: " + cols.length);
			var ele = docRoot.ele(row);
			for(var ii=0;ii<cols.length;ii++)
			{
				//console.log("field:" + fieldDescriptors[ii] + ":" + cols[ii]);
				ele.ele(fieldDescriptors[ii]).txt(cols[ii]);
			}	
		}
		else
		{
			console.log("detected column length to descriptor mismatch. Chunk not processed.")		
		}
	}	
	
	fs.writeFile(output + "_chunk" + chunkNumber + ".xml", doc.toString({pretty:true}), function(err){
		if (err!=undefined)
		{
			console.log("failed to write chunk: " + chunkNumber + " err: " + err);	
		}
		else
		{
			console.log("Chunk: " + chunkNumber + " written.");		
		}
	});
	
}