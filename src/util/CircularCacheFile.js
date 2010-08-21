var sys = require('sys'),
    fs = require('fs');
    
function CircularCacheFile(fileName, maxSize, truncate) {
    this.fileName = fileName;
	this.offset = 0;
	console.log("constructing cache file");
}

CircularCacheFile.prototype.put = 
    function(offset, sig, metatadata, body, callback) {
		this.offset = this.offset + 10;
        callback(null,this.offset);
    } 

CircularCacheFile.prototype.get = function(offset, sig, callback) {
    callback(null,new Buffer('metadata'),new Buffer('body'))
}

CircularCacheFile.prototype.getMetadata = function(offset, sig, callback) {
    callback(null,new Buffer('metadata'))
}


module.exports = CircularCacheFile
