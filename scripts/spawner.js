//as arguments it takes, the number of files and the number of processes, they should be one factor of the other

//it then call the transfere which takes the initial sample (number) and the final one, starting from zero
//
//
var exec = require('child_process').exec;
var total = parseInt(process.argv[2]) 
 var processes= parseInt(process.argv[3])
console.log("total is "+total)
console.log("processes is "+processes)
for (i=0; i<processes+1; i++){
exec('node Logloader '+(i*(total/processes))+' '+((i+1)*(total/processes)-1) , function(error, stdout, stderr) {
    console.log('process number is ' +i+' stdout: ' + stdout);
    console.log('stderr: ' + stderr);
    if (error !== null) {
        console.log('exec error: ' + error);
    }
});
}

