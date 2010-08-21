var crypto = require("crypto"),
	url = require("url"),
  fs = require("fs"),
  sys = require("sys")
	;

eval(fs.readFileSync('../lib/json2.js'));

function computeCacheKey(request) {
	var hash = crypto.createHash('md5');
	hash.update(request.url);
  var cloneHeaders = {};
  for (var i in request.headers) 
  {
    if (i != 'connection' && i != 'keep-alive') 
    {
      cloneHeaders[i] = request.headers[i];
    }
  }
  var headerJSON = JSON.stringify(cloneHeaders);
  hash.update(headerJSON);
	var sig = hash.digest('binary');
	return sig;
}


module.exports = { computeCacheKey : computeCacheKey } 
