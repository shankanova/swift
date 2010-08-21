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

var hashKey = function(key, numBuckets) 
{
    var hashKey = 0;
    for (var i = key.length - 1; i >= 0 ; i--) {
        hashKey = hashKey * 3723 + key.charCodeAt(i);
        hashKey = hashKey % 75987345873490234;
    }
    
    return hashKey;
}

//--------------------------------------------------------------------------------

var fillBuffer = function(buffer, position,  key,offset,userData)
{
    if (!is_a_16char_string(key))
        throw 'key must be a 16 character string';
        
     if (!is_a_pos64bits_number(offset))
        throw 'offset must be a positive 64 bits number';
    
     if (!is_a_pos64bits_number(userData))
        throw 'userData must be a positive 64 bits number';

    buffer.write(key,position,'binary');
    writeUInt64(buffer,position + 16,offset);
    writeUInt64(buffer,position + 24,userData);

    return buffer;
}

function CacheIndex(directory, maxSize)
{
    this.directory = directory;
    this._cacheFileName = path.join(this.directory, 'cache.idx');
    this.maxSize = maxSize || kDefaultMax;
    this._buffer = new Buffer(this.maxSize * 32);
    //console.log('buffer size is ' + this._buffer.length);
}

CacheIndex.prototype.load = 
    function(callback) {
        fs.stat(this._cacheFileName, function (err, stats) {
            console.log(sys.inspect(stats));
        });
//        sys.puts(sys.inspect(this));
//        callback(fileName);
    } 

CacheIndex.prototype.persist = function(callback) 
{
    var buffer = this._buffer;
    fs.open(this._cacheFileName, 'w+', 0666, function (err, fd) {
        if (!err) {
            fs.writeSync(fd, buffer, 0, buffer.length, 0);
            fs.closeSync(fd);
        }
        callback && callback(err);
    });
}

CacheIndex.prototype.reset = function()
{
    fs.stat(this._cacheFileName, function (err) {
        if (!err)
            fs.unlinkSync(this._cacheFileName);
    });
    this._buffer = new Buffer(this.maxSize * 32);
}

CacheIndex.prototype.get = function(key) {

    if (key === null)
        return { offset: this._specialOffset }
        
    if (is_a_16char_string(key)) {
        var bucket = hashKey(key) % this.maxSize;
        var bufPos = bucket * 32;
        var buffer = this._buffer;
        
    //sys.puts(sys.inspect(buffer.slice(bufPos,bufPos + 32)));
         var bufferedKey = buffer.toString('binary',bufPos,bufPos+16);
        //console.log('key = ' + key + ' bufKey = ' + bufferedKey);
        if (key == bufferedKey) {
            var offset = readUInt64(buffer,bufPos + 16);
            var userData = readUInt64(buffer,bufPos + 24);
            return { offset: offset , userData: userData };
        }
    }
}

CacheIndex.prototype.put = function(key, offset, userData) {
//    sys.puts(sys.inspect(arguments));

    if (key === null) {
        this._specialOffset = offset;
        return;
    }
        
    userData = userData || 0;
    
    var bucket = hashKey(key) % this.maxSize;
    //console.log("key = " + key + ", bucket = " + bucket);
    var bufPos = bucket * 32;
    var buffer = this._buffer;
    fillBuffer(buffer,bufPos,key,offset,userData);
    //sys.puts(sys.inspect(buffer.slice(bufPos,bufPos + 32)));
}

module.exports = CacheIndex
