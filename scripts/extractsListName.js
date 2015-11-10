var fs= require('fs')
prefix='/project/production/RD-Connect/'
dataFile=''
fs.readFile('./200VCFpaths', 'utf8', function (err,data) {
  if (err) {
    return console.log(err);
  }
  arr = data;
  files=arr.split("\n")
  files.pop() //remove last empty
   var a=files.map(function(name){ return name.split("/")[3].split(".")[0]})
  uniques=a.filter(function(elem, pos) {
    return a.indexOf(elem) == pos;
})  
console.log(uniques)
  });
