var fs = require('fs');
var sys = require('util');
var spawn = require('child_process').spawn;
var exec = require('child_process').exec;
var builder = require('xmlbuilder'); 
var CPUs = require('os').cpus().length;
var Mem = require('os').totalmem();
var nodeMemLimit = 4194304;

function usage()
{
	console.log("node csv2xml.js inputFileName numberOfChunks fieldDescriptor root row outputFilePrefix [procLimit] [rowSizeEstimate]");
	console.log("inputFileName = the file you want to process.");
	console.log("numberOfChunks = numeric value indicating the number of output files.");
	console.log("filedDescriptors = a file with the comma separated list of column names, these will be tag names under row");
	console.log("root = root tag");
	console.log("row = row tag");
	console.log("outputFilePrefix - files will be named this + chunk + number.xml");
	console.log("procLimit - override the number of process this will spin up to chunk the file.");
	console.log("rowSizeEstimate - defaults to 5000, used to estimate memory requirements for in memory chunking.");
	console.log("estimateOnly - if defined (last param) will run only an estimate.");
	console.log("\n\nYou supplied: ");
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
var procLimit = process.argv[8];
var rowSizeEstimate = process.argv[9];
var estimateOnly = process.argv[10];
console.log("provided: " + process.argv);
if (output == undefined || row == undefined || root == undefined || filename == undefined || chunks == undefined || isNaN(chunks)  || chunks<0 || fieldDescriptors == undefined )
{
	usage();
}

var chunkArray = [];
chunkArray.length = chunks;

if (procLimit == undefined)
{
	procLimit = CPUs;
	console.log("Utilizing " + CPUs + " cores/procs.");
}
else
{
	console.log("Overriding " +  CPUs + " with " + procLimit + " workers.");
}

if (rowSizeEstimate == undefined)
{
	rowSizeEstimate = 5000;
	
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
	
	
	//estimate ram use	
	memNeeded = chunkLen * rowSizeEstimate;
	console.log("Process memory requirement: " + memNeeded/1024);
	if(memNeeded > nodeMemLimit)
	{
		console.log("Your estimated chunk size " + memNeeded/1024 + " is going to cause you to come up against the node process memory limits:" + nodeMemLimit/1024 +". Reduce your chunk size by increasing the number of chunks.");
		process.exit();
				
	}


	memNeeded = memNeeded * procLimit;
	console.log("System memory requirement: " + memNeeded/1024);	
	if (memNeeded > Mem)
	{
		console.log("looks like you need to reduce your chunk sizes or you'll run out of mem. Increase the number of chunks or reduce paralell processing.");
		console.log("System Mem:" + Mem/1024);
		console.log("Estimated Required Concurrent Mem: " + memNeeded/1024);
		process.exit();
	}
	
	if (estimateOnly != undefined)
	{
		console.log("Estimation complete.")
		process.exit();	
	}
	
	var total = 0;
	
	for(var i=0; i<procLimit; i++)
	{
		chunker();	
	}
	
	//loop and chunk out the files and then convert them to XML
	function chunker()
	{
		var nodeCom = "node procChunk.js " + total + " " + chunkLen + " " +  filename + " " +  fieldDescriptors + " " + root + " " + row + " " + output;
		exec(nodeCom, function(error, stdout, stderr)
		{
			if (error!= undefined)
			{
				console.log("failed to process chunk:" + error);	
			}
		
			if (total<chunks)
			{
				chunker();
			}			
		});			
		total++;
		
	}
	

		
	
});


