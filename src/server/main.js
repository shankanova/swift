#!/usr/local/bin/node

var sys = require("sys"),  
    CircularCacheFile = require('../util/circularCacheFile'),
    CacheIndex = require('../util/CacheIndex');


myCacheFile = new CircularCacheFile("/tmp/cache");
myCacheFile.put(0,56325623,new Buffer('metadata'),
    new Buffer('body'),sys.puts);
myCacheFile.get(0,56325623,sys.puts);
myCacheFile.getMetadata(56325623,'',sys.puts);


myCacheIndex = new CacheIndex('/tmp/cacheIndex',10000);

