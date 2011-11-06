var fs = require('fs');
var sys = require('util');
var spawn = require('child_process').spawn;
var exec = require('child_process').exec;
var builder = require('xmlbuilder');


var chunk = process.argv[2];
var chunklen = process.argv[3];
var filename = process.argv[4];
var fieldDescriptors = process.argv[5];
var root = process.argv[6];
var row = process.argv[7];
var output = process.argv[8];

fieldDescriptors = fs.readFileSync(fieldDescriptors, "utf-8");
fieldDescriptors = fieldDescriptors.trim().split(',');
var d = new Date();
var time = d.getMilliseconds();


MyExec(chunk, chunklen,filename, fieldDescriptors, root, row, output);

function MyExec(chunk, chunkLen, filename, fieldDescriptors, root, row, output)
{
	var chunkStart = (chunk*chunkLen) + 1;
	var chunkEnd = chunkStart + chunkLen;
	var sed = spawn('sed', ['-n', chunkStart + "," + chunkEnd + 'p', filename]);	
	var chunkObj = {"proc":sed, "data":""};
	
	chunkObj.proc.on('exit', function(code){
		if (code!=0)
		{
			console.log("chunk failed...");
		}
		CsvToXML(chunkObj.data, chunk, fieldDescriptors, root, row, output);

	});
	
		chunkObj.proc.stdout.on('data',function(data){
			chunkObj.data +=data;
	});

}
 
function CsvToXML(data, chunkNumber, fieldDescriptors, root, row, output)
{
	console.log("INCOMING CHUNK " + chunkNumber + ":")
	var doc = builder.create();
	var docRoot = doc.begin(root);
 	var lines = data.trim().split('\n');
	for(var i =0; i<lines.length;i++)
	{
		var cols = lines[i].trim().split(',');
		if (cols.length == fieldDescriptors.length)
		{
			var ele = docRoot.ele(row);
			for(var ii=0;ii<cols.length;ii++)
			{
				//console.log("field:" + fieldDescriptors[ii] + ":" + cols[ii]);
				ele.ele(fieldDescriptors[ii]).txt(cols[ii]);
			}	
		}
		else
		{
			console.log("detected column length to descriptor mismatch. Line not processed.")		
		}
	}	
	
	fs.writeFile(output + "_chunk" + chunkNumber + ".xml", doc.toString({pretty:true}), function(err){
		var d2= new Date();
		var time2 = d2.getMilliseconds();
		var diff = time2 - time;
		console.log("Chunk processed in: " + diff/1000 );
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