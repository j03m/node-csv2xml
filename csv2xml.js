var fs = require('fs');
var sys = require('util');
var spawn = require('child_process').spawn;
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
var fieldDescriptors = process.argv[4];
var root = process.argv[5];
var row = process.argv[6];
var output = process.argv[7];
if (output == undefined || row == undefined || root == undefined || filename == undefined || chunks == undefined || isNaN(chunks)  || chunks<0 || fieldDescriptors == undefined )
{
	usage();
}

var chunkArray = [];
chunkArray.length = chunks;

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
		var nodeCom = "node procChunk.js " + i + " " + chunkLen + " " +  leftOver + " " +  filename+ " " +  fieldDescriptors;
		exec(nodeCom, function(error, stdout, stderr)
		{
			if (error!= undefined)
			{
				console.log("failed to process chunk:" + error);	
			}
		});
	}
});


