var fs = require('fs');
var data = "";

for(var i =0; i<1000; i++)
{
	var row = "";
	for (var ii=0; ii<6; ii++)
	{
		row+="data-"+i+"-"+ii;
		if (ii!=5){
			row+=",";
		}		
	}	
	row+="\n";
	console.log(row);
	data+=row;
}

fs.writeFile("test.txt",data, function(err){
	console.log("done!");
});