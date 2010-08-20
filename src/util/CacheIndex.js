var sys = require('sys'),
    fs = require('fs');
    
function CacheIndex(directory, maxSize) {
    this.directory = directory;
}

CacheIndex.prototype.load = 
    function(callback) {
        callback(null);
    } 

CacheIndex.prototype.persist = function(callback) {
    callback(null)
}

CacheIndex.prototype.reset = function() {
}


CacheIndex.prototype.get = function(key) {
    return { offset: 0, userData: 0};
}

CacheIndex.prototype.put = function(key, offset, userData) {
}

module.exports = CacheIndex
