var assert = require('assert'),
    sys = require('sys'),
    CacheIndex = require('./CacheIndex'),
    CircularCacheFile = require('./circularCacheFile');

function CacheManager() {
	this.cacheIndex = new CacheIndex("/tmp", 10000);
	this.circularCacheFile = new CircularCacheFile("/tmp/circCacheFile", 1000000, true);
	// TODO : Get offset from cacheIndex
	this.offset = 0

  this.id = 0;
  this.next = this;
  this.prev = this;
}

CacheManager.prototype.get = 
  function(key, callback) {
    for (var entry = this.next; entry != this; entry = entry.next) {
      if (entry.key == key) {
        console.log("Found in backlog");
        var now = (new Date()).valueOf();
        if (entry.expires <= now) {
          console.log("Cache entry expired " + ((now - expires) / 1000) + ' seconds ago');
          callback(false, null, null);
          return;
        }
        callback(true, entry.metadata, entry.body);
        return;
      }
    }
		var offset_map = this.cacheIndex.get(key);
		if (offset_map && offset_map.offset >= 0) {
      var expires = offset_map.userData;
      var now = (new Date()).valueOf();
      if (expires <= now) {
        console.log("Cache entry expired " + ((now - expires) / 1000) + ' seconds ago');
        callback(false, null, null);
        return;
      }
			console.log("Cache index offset is " + offset_map.offset);
			this.circularCacheFile.get(offset_map.offset, key,
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
	function(key, expires, metadata, body) {
    var entry = { 
      'id' : ++this.id,
      'prev' : this.prev,
      'next' : this, 
      'key' : key, 
      'expires' : expires,
      'metadata' : metadata, 
      'body' : body 
    };
		console.log("CacheManager put called assigned id " + entry.id);
    entry.prev.next = entry;
    entry.next.prev = entry;
    var that = this;
    if (this.next == entry) {
      var completionFunction =
	   		function(error, newOffset) {
		      console.log("CacheFile put finished being called on id " + entry.id);
          assert.equal(entry, that.next);
          assert.equal(entry.prev, that);
	  			if (error) { 
	  				console.log("Error in writing to cache file. " + error) 
	  			}
	  			else {
	  				that.cacheIndex.put(entry.key, that.offset, entry.expires);
	  				that.offset = newOffset;
	  				console.log("CacheIndex put worked!");
          }
          that.next = entry.next;
          that.next.prev = that;
          entry = that.next;
          if (entry != that) {
     	      console.log("CacheFile put being called on " + entry.id);
            that.circularCacheFile.put(that.offset, entry.key, entry.metadata, entry.body, completionFunction); 
          }
	  		}
     	console.log("CacheFile put being called on " + entry.id);
	  	this.circularCacheFile.put(this.offset, entry.key, entry.metadata, entry.body, completionFunction);
    }
	}
		


module.exports = CacheManager
