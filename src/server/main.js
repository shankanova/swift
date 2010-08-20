#!/usr/local/bin/node

var sys = require("sys"),  
    crypto = require("crypto");
    CircularCacheFile = require('../util/circularCacheFile'),
    CacheIndex = require('../util/CacheIndex');


function testCircularCacheFile(fileName, maxSize, numEntries, entrySize)
{
//    var myCacheFile = new CircularCacheFile(fileName, maxSize, false);
    var offset = 0;
    var buffer = new Buffer(entrySize);
    for (var i = 0; i < numEntries; i = i + 1) {
        var hash = crypto.createHash('md5');
        var sig = hash.update(i).digest('binary');
sys.puts(sig.length);
sys.puts(sig);
//        myCacheFile.put(offset, sig, buffer, sys.puts);
//myCacheFile.get(0,sig,sys.puts);
//myCacheFile.getMetadata(sig,'',sys.puts);
    }
}


testCircularCacheFile('/tmp/cacheIndex',10000, 100, 200);

