var sys = require('sys'),
    CacheIndex = require('./CacheIndex'),
    CircularCacheFile = require('./circularCacheFile');

function CacheManager() {
	this.cacheIndex = new CacheIndex("/tmp", 10000);
	this.circularCacheFile = new CircularCacheFile("/tmp/circCacheFile", 1000000, true);
	// TODO : Get offset from cacheIndex
	this.offset = 0
}

CacheManager.prototype.get = 
	function(id, callback) {
		var offset_map = this.cacheIndex.get(id);
		if (offset_map && offset_map.offset>=0) {
			console.log("Cache index offset is " + offset_map.offset);
			this.circularCacheFile.get(offset_map.offset, id,
				function(error, metadata, body) {
					if (error) { 
						console.log('CacheManager error: ' + error);
						callback(false, metadata, body);
					}
					else {
						callback(true, metadata, body);
					}
			}
			);
			console.log("Cache file get finished ");
		}
		else {
			console.log("Not found in Index");
			callback(false, null, null);
		}

	}

CacheManager.prototype.put = 
	function(key, metadata, body) {
		console.log("CacheManager put called");
		var userData = 0;
    var that = this;
		console.log("CacheFile put being called on " + this.circularCacheFile);
		this.circularCacheFile.put(this.offset, key, metadata, body, 
			function(error, newOffset) {
				if (error) { 
					console.log("Error in writing to cache file. " + error) 
				}
				else {
sys.puts("put offset=" + that.offset);
					that.cacheIndex.put(key, that.offset, userData);
					that.offset = newOffset;
					console.log("CacheIndex put worked!");
				}
			}
			);
		console.log("CacheFile put finished being called");
	}
		


module.exports = CacheManager
