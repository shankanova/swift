var sys = require('sys'),
    fs = require('fs'),
    path = require("path");

var kDefaultMax = 1024;

//--------------------------------------------------------------------------------

var is_a_number = function (value) { return value === parseInt(value); }
var is_a_pos64bits_number = function (value) { 
    return (is_a_number(value) && value >= 0 && (value.toString(16).length <= 16))
     }
var is_a_16char_string = function (value) {
    return typeof key !== 'string' || key.length !== 16;
    }

//--------------------------------------------------------------------------------

var writeUInt64 = function(buffer,offset,int64)
{    
    for (var i = 0 ; i < 8 ; i++) {
        var byte = int64 % 256;
        int64 = Math.floor(int64 / 256);
        buffer[offset++] = byte;
    }
}

var readUInt64 = function(buffer,offset)
{  
    var int64 = 0;
    for (var i = 7 ; i >= 0 ; i--) {
        var byte = buffer[offset + i];
        int64 = int64 * 256 + byte;
        }
    return int64;
}

//--------------------------------------------------------------------------------

var fillBuffer = function(buffer, key,offset,userData)
{
    if (!is_a_16char_string(key))
        throw 'key must be a 16 character string';
        
     if (!is_a_pos64bits_number(offset))
        throw 'offset must be a positive 64 bits number';
    
     if (!is_a_pos64bits_number(userData))
        throw 'userData must be a positive 64 bits number';

    buffer.write(key,0,'binary');
    writeUInt64(buffer,16,offset);
    writeUInt64(buffer,24,userData);

    return buffer;
}

function CacheIndex(directory, maxSize)
{
    this.directory = directory;
    this.maxSize = maxSize || kDefaultMax;
    this._index = {};
    this._numEntries = 0;
}

CacheIndex.prototype.load = 
    function(callback) {
        var fileName = path.join(this.directory, 'cache.idx');
//        sys.puts(sys.inspect(this));
//        callback(fileName);
    } 

CacheIndex.prototype.persist = function(callback) {
    
}

CacheIndex.prototype.reset = function() {
    this._index = {};
}


CacheIndex.prototype.get = function(key) {
    if (is_a_16char_string(key))
        return this._index[key];
}

CacheIndex.prototype.put = function(key, offset, userData) {
//    sys.puts(sys.inspect(arguments));

    userData = userData || 0;
    
    
    var buffer = new Buffer(32);
    fillBuffer(buffer,key,offset,userData);
    //sys.puts(sys.inspect(buffer));

    this._index[key] = { offset: offset, userData: userData }
}

module.exports = CacheIndex
