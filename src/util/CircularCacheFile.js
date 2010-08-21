var sys = require('sys'),
    fs = require('fs');
    crypto = require('crypto');
   
function dumpString(s)
{
    for (var i = 0; i < s.length; ++i) {
        sys.puts("s[" + i + "]=" + s.charCodeAt(i));
    }
}

function dumpBuffer(buffer, begin, end)
{
    for (var i = begin; i < end; ++i) {
        sys.puts("buffer[" + i + "]=" + buffer[i]);
    }
}


function BufferStream(buffer, offset) 
{
    this.buffer = buffer;
    this.offset = offset;
}

BufferStream.numberLength = 4;

BufferStream.prototype.encodeNumber = function(number)
{
    for (var i = BufferStream.numberLength - 1; i >= 0; i--) {
        this.buffer[this.offset + i] = number % 256;
        number = Math.floor(number / 256);  
    }
    this.offset += BufferStream.numberLength;
    return number;
}

BufferStream.prototype.decodeNumber = function()
{
    var number = 0;
    var shift = 1;
    for (var i = BufferStream.numberLength - 1; i >= 0; i--) {
        number += this.buffer[this.offset + i] * shift;
        shift *= 256;
    }
    this.offset += BufferStream.numberLength;
    return number;
}

BufferStream.prototype.decodeString = function()
{
    var stringLength = this.decodeNumber();
    var o = this.offset;
    this.offset += stringLength;
    var s = this.buffer.toString('binary', o, this.offset);
    return s;
}

BufferStream.prototype.encodeString = function(s)
{
    this.encodeNumber(s.length);
    this.buffer.write(s, this.offset, 'binary');
    this.offset += s.length;
}

BufferStream.prototype.decodeFixed = function(size)
{
    var fixed = this.buffer.toString('binary', this.offset, this.offset + size);
    this.offset += size;
    return fixed;
}

BufferStream.prototype.encodeFixed = function(fixed)
{
    this.buffer.write(fixed, this.offset, 'binary');
    this.offset += fixed.length;
}

BufferStream.prototype.decodeBuffer = function(size)
{
    var result = new Buffer(size);
    this.buffer.copy(result, 0, this.offset, this.offset + size);
    this.offset += size;
    return result;
}

BufferStream.prototype.encodeBuffer = function(buf)
{
    buf.copy(this.buffer, this.offset, 0, buf.length);
    this.offset += buf.length;
}

BufferStream.prototype.getOffset = function()
{
    return this.offset;
}

BufferStream.prototype.setOffset = function(newOffset)
{
    this.offset = newOffset;
}

function CircularCacheFile(fileName, maxSize, truncate) {
    var err = null;
    this.fileName = fileName;
    this.maxSize = maxSize;
    // this.magic = crypto.createHash('md5').update('CircularCacheFile').digest('binary');
    this.magic = "__MAGIC__MAGIC__";
    var flags = process.O_CREAT | process.O_RDWR;
    if (truncate) {
        flags |= process.O_TRUNC;
    }
    this.fd = fs.openSync(fileName, flags, 0644);
}

CircularCacheFile.hashLength = 16;

CircularCacheFile.prototype.put = function(offset, sig, metaData, data, callback) 
{
    var numberLength = BufferStream.numberLength;
    var hashLength = CircularCacheFile.hashLength;
    var payloadLength = metaData.length + data.length;
    var headerLength = 
        this.magic.length + // magic
        numberLength +      // header length
        numberLength +      // signature length
        sig.length +        // signature data
        numberLength +      // metadata length
        hashLength +        // metadata hash 
        numberLength +      // data length
        hashLength +        // data hash 
        this.magic.length;  // magic again
    var trailerLength = this.magic.length;
    var bufferLength = headerLength + payloadLength + trailerLength;

    // sys.puts("headerLength=" + headerLength + ", payloadLength=" + payloadLength + ", trailerLength=" + trailerLength + ", bufferLength=" + bufferLength);

    var buffer = new Buffer(bufferLength);
    var bufferStream = new BufferStream(buffer, 0);
    bufferStream.encodeFixed(this.magic);
    bufferStream.encodeNumber(headerLength);
    bufferStream.encodeString(sig);
    bufferStream.encodeNumber(metaData.length);
    bufferStream.encodeFixed(crypto.createHash('md5').update(metaData).digest('binary'));
    bufferStream.encodeNumber(data.length);
    bufferStream.encodeFixed(crypto.createHash('md5').update(data).digest('binary'));
    bufferStream.encodeFixed(this.magic);
    bufferStream.encodeBuffer(metaData);
    bufferStream.encodeBuffer(data);
    bufferStream.encodeFixed(this.magic);

    var wrote = 0;

    var writeCB = function(err, written) 
        {
            sys.puts("fs.write callback err=" + err + ", written=" + written);
            if (err) {
                callback(err, offset + wrote);
                return;
            }
            wrote += written;
            if (wrote == bufferLength) {
                sys.puts("write done, offset=" + (offset + wrote));
                callback(null, offset + wrote);
                return;
            }
            fs.write(this.fd, buffer, wrote, bufferLength - wrote, offset + wrote, writeCB);
        };
    fs.write(this.fd, buffer, 0, bufferLength, offset, writeCB);
} 

CircularCacheFile.prototype.getHeader = function(offset, sig, callback) {
    var numberLength = BufferStream.numberLength;
    var hashLength = CircularCacheFile.hashLength;
    var magic = this.magic;
    var magicLength = this.magic.length;
    var headerLength = 
        magicLength +       // magic
        numberLength +      // header length
        numberLength +      // signature length
        sig.length +        // signature data
        numberLength +      // metadata length
        hashLength +        // metadata hash 
        numberLength +      // data length
        hashLength +        // data hash 
        magicLength;        // magic again
    var headerBuffer = new Buffer(headerLength);
    var read = 0;
    var fd = this.fd;

    var gotHeaderBuffer = function()
        {
            var bufferStream = new BufferStream(headerBuffer, 0);
            var magic1 = bufferStream.decodeFixed(magicLength);
            if (magic1 != magic) {
                callback("Header 1st magic incorrect", null);
                return;
            }
            var headerLength = bufferStream.decodeNumber();
            if (headerLength != headerBuffer.length) {
                callback("Header length incorrect", null);
                return;
            }
            var headerSig = bufferStream.decodeString();
            if (headerSig != sig) {
                callback("Header sig does not match expected sig", null);
                return;
            }
            var metaDataLength = bufferStream.decodeNumber();
            var metaDataHash = bufferStream.decodeFixed(hashLength);
            var metaDataOffset = offset + headerLength;
            var dataLength = bufferStream.decodeNumber();
            var dataHash = bufferStream.decodeFixed(hashLength);
            var dataOffset = offset + headerLength + metaDataLength;
            var magic2 = bufferStream.decodeFixed(magicLength);
            if (magic2 != magic) {
                callback("Header 2nd magic incorrect", null);
                return;
            }
            callback(null, 
                { 
                    "metaDataLength" : metaDataLength, 
                    "metaDataHash" : metaDataHash, 
                    "metaDataOffset" : metaDataOffset, 
                    "dataLength" : dataLength, 
                    "dataHash" : dataHash,
                    "dataOffset" : dataOffset
                });
        };
    
    var readCB = function(err, bytesRead) 
        {
            sys.puts("fs.read callback err=" + err + ", bytesRead=" + bytesRead);
            if (err) {
                callback(err, null);
                return;
            }
            read += bytesRead;
            if (read == headerLength) {
                gotHeaderBuffer();
                return;
            }
            fs.read(fd, headerBuffer, read, headerLength - read, offset + read, readCB);
        };

    fs.read(fd, headerBuffer, 0, headerLength, offset, readCB);
};
    
CircularCacheFile.prototype.getBuffer = function(offset, length, hash, callback) {

    var fd = this.fd;
    var buffer = new Buffer(length); 
    var read = 0;
    var readCB = function(err, bytesRead)
        {
            sys.puts("fs.read callback err=" + err + ", bytesRead=" + bytesRead);
            if (err) {
                callback(err, null);
                return;
            }
            read += bytesRead;
            if (read == length) {
                 var bufferHash = crypto.createHash('md5').update(buffer).digest('binary'); 
                 if (bufferHash != hash) {
                     callback("Hash is incorrect", null);
                     return;
                 }
                 callback(null, buffer);
                 return;
            }
            fs.read(fd, buffer, read, length - read, offset + read, readCB);
        };
    fs.read(fd, buffer, 0, length, offset, readCB);
}


CircularCacheFile.prototype.getMetadata = function(offset, sig, callback) {
    var fd = this.fd;
    var ccf = this;
    this.getHeader(offset, sig, function(err, header) 
        {
            if (err) {
                callback(err, null);
                return;
            }
            ccf.getBuffer(header.metaDataOffset, header.metaDataLength, header.metaDataHash, 
                function(err, buffer)
                    {
                        if (err) {
                            callback(err, null);
                            return;
                        }
                        callback(null, buffer);
                    });
        });
}

CircularCacheFile.prototype.getData = function(offset, sig, callback) {
    var fd = this.fd;
    var ccf = this;
    this.getHeader(offset, sig, function(err, header) 
        {
            if (err) {
                callback(err, null, null);
                return;
            }
            ccf.getBuffer(header.metaDataOffset, header.metaDataLength, header.metaDataHash, 
                function(err, buffer)
                    {
                        if (err) {
                            callback(err, null, null);
                            return;
                        }
                        var metaDataBuffer = buffer;
                        ccf.getBuffer(header.dataOffset, header.dataLength, header.dataHash, function(err, buffer) 
                            {
                              
                                if (err) {
                                    callback(err, null, null);
                                    return;
                                }
                                callback(null, metaDataBuffer, buffer);
                            });
                    });
        });
}


module.exports = CircularCacheFile
