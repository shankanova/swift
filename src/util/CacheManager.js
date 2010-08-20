var CacheIndex = require('./CacheIndex'),
    CircularCacheFile = require('./circularCacheFile');

function CacheManager() {
	this.cacheIndex = new CacheIndex("/tmp", 10000);
	this.circularCacheFile = new CircularCacheFile("/tmp/circCacheFile", 1000000, true);
}

CacheManager.prototype.get = 
	function(id, callback) {
		var offset_map = this.cacheIndex.get(id);
		if (offset_map && offset_map.offset>0) {
			this.circularCacheFile.get(offset_map.offset, "sig", 
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
		}
		else {
			console.log("Not found in Index");
			callback(false, null, null);
		}

	}

module.exports = CacheManager
