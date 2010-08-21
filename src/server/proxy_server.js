var http = require('http');
var sys  = require('sys');
var fs   = require('fs');
var CacheManager = require('../util/CacheManager');
var CacheUtils = require('../util/CacheUtils');

eval(fs.readFileSync('../lib/json2.js'));

var blacklist = [];
var iplist    = ["127.0.0.1"];

fs.watchFile('./blacklist', function(c,p) { update_blacklist(); });
fs.watchFile('./iplist', function(c,p) { update_iplist(); });

function update_blacklist() {
  fs.stat('./blacklist', function(err, stats) {
    if (!err) {
      sys.log("Updating blacklist.");
      blacklist = fs.readFileSync('./blacklist').split('\n')
                  .filter(function(rx) { return rx.length })
                  .map(function(rx) { return RegExp(rx) });
    }
  });
}

function update_iplist() {
  fs.stat('./iplist', function(err, stats) {
    if (!err) {
      sys.log("Updating iplist.");
      iplist = fs.readFileSync('./iplist').split('\n')
               .filter(function(rx) { return rx.length });
    }
  });
}

function ip_allowed(ip) {
  // shortcut
  return true;
  for (i in iplist) {
    if (iplist[i] == ip) {
      return true;
    }
  }
  return false;
}

function host_allowed(host) {
  for (i in blacklist) {
    if (blacklist[i].test(host)) {
      return false;
    }
  }
  return true;
}

function deny(response, msg) {
  response.writeHead(401);
  response.write(msg);
  response.end();
}

var cacheManager = new CacheManager();

http.createServer(function(request, response) {
  var ip = request.connection.remoteAddress;
  if (!ip_allowed(ip)) {
    msg = "IP " + ip + " is not allowed to use this proxy";
    deny(response, msg);
    sys.log(msg);
    return;
  }

/*  if (!host_allowed(request.url)) {
    msg = "Host " + request.url + " has been denied by proxy configuration";
    deny(response, msg);
    sys.log(msg);
    return;
  }
*/
  sys.log(ip + ": " + request.method + " " + request.url);
sys.puts(sys.inspect(request.headers));
  try {
  var proxy = http.createClient(80, request.headers['host']);
  var cacheKey = CacheUtils.computeCacheKey(request);
  console.log("CacheKey = " + cacheKey.toString(16) + " , Url = " + request.url);
  cacheManager.get(cacheKey, 
	function (found, metadata, body) {
		  if (found) { 
      var meta = JSON.parse(metadata.toString('utf8'));
sys.puts(sys.inspect(meta));
      response.writeHead(meta.statusCode, meta.header);
			response.write(body, 'binary');
			response.end();
		  }
		  else {
  		  var proxy_request = proxy.request(request.method, request.url, request.headers);
		  var currentBufferCapacity = 1000000; // 1MB ?
		  var buffer = new Buffer(currentBufferCapacity);
		  var contentLength = 0;
		  proxy_request.addListener('response', function(proxy_response) {
			proxy_response.addListener('data', function(chunk) {
						if ((contentLength + chunk.length) > currentBufferCapacity) 
						{
							while ((contentLength + chunk.length) > currentBufferCapacity)
							{
								// double the buffer
								currentBufferCapacity = currentBufferCapacity * 2;
								console.log("expanding the buffer to size " + currentBufferCapacity);
							}
							var tempBuffer = new Buffer(currentBufferCapacity);
							buffer.copy(tempBuffer, 0, 0, contentLength);
							buffer = tempBuffer; 
						}
							chunk.copy(buffer, contentLength, 0, chunk.length);
							contentLength += chunk.length
							response.write(chunk, 'binary');
							//console.log('buffer length = ' + contentLength);
			});
			proxy_response.addListener('end', function() {
        var meta = { 
            'statusCode' : proxy_response.statusCode,
            'header' : proxy_response.headers
        }; 
sys.puts(sys.inspect(meta));
        var metaJSON = JSON.stringify(meta);
			  response.end();
			  cacheManager.put(cacheKey, new Buffer(metaJSON, 'utf8'), buffer.slice(0, contentLength));
			});
			response.writeHead(proxy_response.statusCode, proxy_response.headers);
		  });
		  request.addListener('data', function(chunk) {
				try {
					proxy_request.write(chunk, 'binary');
				}
				catch (err) {
					console.log(err);
				}
		  });
		  request.addListener('end', function() {
			proxy_request.end();
		  });
	  }
	});
  }
  catch (err) {
	console.log("Caught exception : " + err);
  }
}).listen(8080);

update_blacklist();
update_iplist();

/*
process.on('uncaughtException', function (err) {
  console.log('Caught fatal exception: ' + err);
});
*/
