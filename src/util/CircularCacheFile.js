var sys = require('sys'),
    fs = require('fs');
    
function CircularCacheFile(fileName, maxSize, truncate) {
    this.fileName = fileName;
}

CircularCacheFile.prototype.put = 
    function(offset, sig, metatadata, body, callback) {
        callback(null,"hello");
    } 

CircularCacheFile.prototype.get = function(offset, sig, callback) {
    callback(null,new Buffer('metadata'),new Buffer('body'))
}

CircularCacheFile.prototype.getMetadata = function(offset, sig, callback) {
    callback(null,new Buffer('metadata'))
}


module.exports = CircularCacheFile
