var sh = require('execSync');
process.argv.forEach(function (val, index, array) {
  console.log(index + ': ' + val);
});
var fs= require('fs')
prefix='/project/production/RD-Connect/'
dataFile=''
fs.readFile('./200VCFpaths', 'utf8', function (err,data) {
  if (err) {
    return console.log(err);
  }
  arr = data;
  //console.log('prefix si'+prefix+data);
  
  files=arr.split("\n")
  files.pop() //remove last empty
  for (i = process.argv[2]; i < process.argv[3]; i++) {
      command= "ssh dpiscia@172.16.10.21 'cat "+prefix+files[i]+"' | ssh dpiscia@10.10.0.61 'hadoop fs -put - /user/dpiscia/ALL2/"+files[i].split("/")[3]+"'"
      console.log(command)      
    // sh.exec(command)
          }
          });
  //
