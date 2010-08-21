#!/usr/local/bin/node

var sys = require("sys"),
    http = require("http"),
    url = require("url"),
    path = require("path"),
    fs = require("fs"),
	CircularCacheFile = require("../util/circularCacheFile"),
	CacheIndex = require('../util/CacheIndex');


process.argv.forEach(function (val, index, array) {
  console.log(index + ': ' + val);
});


var cacheFileName = process.argv[2];
var cacheFileSize = process.argv[3];
var truncate = process.argv[4];
var myCacheFile = new CircularCacheFile(cacheFileName, cacheFileSize, truncate);
var maxCacheIndexSize = 1000000;
var myCacheIndex = new CacheIndex("/tmp",maxCacheIndexSize);
 
var server = http.createServer(function(request, response) {
	var operation = url.parse(request.url).pathname;
    switch (operation) {
	case '/':
		showForm(request, response);
		break;
	case '/get':
		getResource(request, response);
		break;
	case '/put':
		putResource(request, response);
		break;
	default:
		show404(request, response, "Sorry no such operation: " + operation);
		break;
	}
});

server.listen(8080);


function showForm(req, res) {
  res.sendHeader(200, {'Content-Type': 'text/html'});
  res.write(
    '<form action="/put" method="post" enctype="multipart/form-data">'+
    '<input type="text" name="json-body">'+
    '<input type="submit" value="Put">'+
    '</form>'
  );
  res.close();
}

function show404(request, response, errorText) {
	response.sendHeader(404);
	response.write(errorText, "UTF-8");
	response.close();
}

function getResource(request, response) {
	var uriParsed = url.parse(request.url, true);
	if (uriParsed.hasOwnProperty('query') && uriParsed.query.hasOwnProperty('id')) {
			var idRequested = uriParsed.query.id;
    		response.sendHeader(200);
    		response.write("get resource" + idRequested, "UTF-8");
			response.close();
	  }
	else 
	 {
		show404(request, response, "get request was malformed. Usage: /get?id=resource_id");
	}
	
}


function putResource(request, response) {
    response.sendHeader(200);
    response.write(textResponse, "UTF-8");
    response.close();
}

sys.puts("Server running at http://localhost:8080/");

