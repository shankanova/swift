#!/usr/local/bin/node

var sys = require("sys"),  
    crypto = require("crypto");
    CircularCacheFile = require('../util/circularCacheFile'),
    CacheIndex = require('../util/CacheIndex');

function fill(buffer, s)
{
    for (var i = 0; i < buffer.length; i += s.length) 
    {
        buffer.write(s, i);
    }
}

function testCircularCacheFile(fileName, maxSize, numEntries, metaDataSize, dataSize, callback)
{
    var myCacheFile = new CircularCacheFile(fileName, maxSize, false);
    var offset = 0;
    var offsets = {};
    var bytesRead = 0;
    var entrySize = 0;
    var putter = function(index, callback) 
        {
            if (index >= numEntries) {
                callback(null);
                return;
            }
            var sig = crypto.createHash('md5').update(index).digest('binary');
            var metaData = new Buffer(metaDataSize);
            fill(metaData, "metadata" + index);
            var data = new Buffer(dataSize);
            fill(data, "data" + index);
            myCacheFile.put(offset, sig, metaData, data, function(err, newOffset) { 
                    // sys.puts("index=" + index + ", err=" + err + ", newOffset=" + newOffset);
                    if (err) {
                        callback(err);
                        return;
                    }
                    offsets[index] = offset;
                    if (index == 0) {
                        entrySize = newOffset - offset;
                    }
                    offset = newOffset;
                    putter(index + 1, callback);
                });
        };
    var getter = function(index, callback)
        {
            if (index < 0) {
                callback(null);
                return;
            }
            var sig = crypto.createHash('md5').update(index).digest('binary');
            myCacheFile.get(offsets[index], sig, function(err, metaData, data) 
                {
                     // sys.puts("index=" + index + ", err=" + err + ", metaData=" + metaData + ", data=" + data);
                     if (bytesRead + entrySize < maxSize) { 
                         if (err) {
                             callback(err);
                             return;
                         }
                         var expectedMetaData = new Buffer(metaDataSize);
                         fill(expectedMetaData, "metadata" + index);
                         var expectedData = new Buffer(dataSize);
                         fill(expectedData, "data" + index);
                         if (!expectedMetaData.equals(metaData)) {
                             sys.puts("expectedMetaData=" + expectedMetaData + ", metaData=" + metaData);
                             callback("expectedMetaData != metaData");
                             return;
                         }
                         if (expectedData.toString() != data.toString()) {
                             sys.puts("expectedData=" + expectedData + ", data=" + data);
                             callback("expectedData != data");
                             return;
                         }
                     }
                     else {
                         if (!err) {
                             callback("error expected due to wraparound");
                             return;
                         }
                     }
                     bytesRead += entrySize;
                     getter(index - 1, callback);
                 });
        };
    putter(0, function(err) 
        { 
            if (err) {
                sys.puts("putter err=" + err);
                return;
            }
            getter(numEntries - 1, callback); 
        });
    
}


testCircularCacheFile('/tmp/CircularCacheFile', 1000000, 10, 100, 200000, function(err) 
   {
        if (err) {
            sys.puts("failed err=" + err);
            return;
        }
        sys.puts("passed");
   });

