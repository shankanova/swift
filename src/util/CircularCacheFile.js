var sys = require('sys'),
    fs = require('fs');
    crypto = require('crypto');
   
function BufferStream(buffer, offset) 
{
    this.buffer = buffer;
    this.offset = offset;
}

BufferStream.numberLength = 4;

BufferStream.computeHash = function(buffer)
{
    return crypto.createHash('md5').update(buffer).digest('binary');
}

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

BufferStream.prototype.decodeHash = function()
{
    return this.decodeFixed(CircularCacheFile.hashLength);
}

BufferStream.prototype.encodeHashOfBuffer = function(buffer)
{
    this.encodeFixed(BufferStream.computeHash(buffer));
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
    this.magic = "__MAGIC__MAGIC__";
    var flags = process.O_CREAT | process.O_RDWR;
    if (truncate) {
        flags |= process.O_TRUNC;
    }
    this.fd = fs.openSync(fileName, flags, 0644);
}

CircularCacheFile.hashLength = 16;
CircularCacheFile.version = 1;

function read(fd, buffer, offset, length, position, callback) 
{
    var read = 0;
    var readCB = function(err, bytesRead)
        {
            sys.puts("fs.read callback err=" + err + ", bytesRead=" + bytesRead);
            if (err) {
                callback(err, read);
                return;
            }
            read += bytesRead;
            if (read == length) {
                 sys.puts("fs.read done, position=" + (position + read));
                 callback(null, read);
                 return;
            }
            fs.read(fd, buffer, offset + read, length - read, position + read, readCB);
        };
    fs.read(fd, buffer, offset, length, position, readCB);
}

function getBuffer(fd, length, position, callback) 
{
    var buffer = new Buffer(length);
    read(fd, buffer, 0, length, position, function(err, bytesRead)
        {
            callback(err, err ? null : buffer);
        });
}

function getBufferWithWrap(fd, maxSize, length, position, callback)
{
    var endPosition = position + length;
    if (endPosition <= maxSize) {
        getBuffer(fd, length, position, callback); 
        return;
    }
    var left = 2;
    var buffer = new Buffer(length);
    var countDown = function(err, bytesRead)
        {
            if (err) {
                callback(err, null);
            }
            else {
                left--;
                if (left == 0) {
                    callback(null, buffer);
                }                
            }
        };
    var firstLength = maxSize - position;
    read(fd, buffer, 0, firstLength, position, countDown);
    var secondLength = length - firstLength;
    read(fd, buffer, firstLength, secondLength, 0, countDown);
}

function write(fd, buffer, offset, length, position, callback)
{
    var wrote = 0;
    var writeCB = function(err, written) 
        {
            sys.puts("fs.write callback err=" + err + ", written=" + written);
            if (err) {
                callback(err, wrote);
                return;
            }
            wrote += written;
            if (wrote == length) {
                sys.puts("write done, position=" + (position + wrote));
                callback(null, wrote);
                return;
            }
            fs.write(fd, buffer, offset + wrote, length - wrote, position + wrote, writeCB);
        };
    fs.write(fd, buffer, offset, length, position, writeCB);
}

function putBuffer(fd, buffer, position, callback)
{
    write(fd, buffer, 0, buffer.length, position, function(err, written)
        {
             callback(err, err ? null : position + written);
        });
}

function putBufferWithWrap(fd, maxSize, buffer, position, callback)
{
    var endPosition = position + buffer.length;
    if (endPosition <= maxSize) {
        putBuffer(fd, buffer, position, callback);
        return;
    }
    var left = 2;
    var countDown = function(err, wrote) 
        {
            if (err) { 
                callback(err, null);
            }
            else {
                left--;
                if (left == 0) {
                    callback(null, (position + buffer.length) % maxSize);
                }
            }
        };
    var firstLength = maxSize - position;
    write(fd, buffer, 0, firstLength, position, countDown);
    var secondLength = length - firstLength;
    write(fd, buffer, firstLength, secondLength, 0, countDown);
}

CircularCacheFile.prototype.getHeaderLength = function(sig)
{
    var numberLength = BufferStream.numberLength;
    var hashLength = CircularCacheFile.hashLength;
    var magic = this.magic;
    var headerLength = 
        magic.length +      // magic
        numberLength +      // version
        numberLength +      // header length
        numberLength +      // signature length
        sig.length +        // signature data
        numberLength +      // metadata length
        hashLength +        // metadata hash 
        numberLength +      // data length
        hashLength +        // data hash 
        magic.length;       // magic again
    return headerLength;
}

CircularCacheFile.prototype.getTrailerLength = function(sig)
{
    var numberLength = BufferStream.numberLength;
    return numberLength;
}

CircularCacheFile.prototype.put = function(position, sig, metaData, data, callback) 
{
    var headerLength = this.getHeaderLength(sig);
    var payloadLength = metaData.length + data.length;
    var trailerLength = this.getTrailerLength(sig);

    var bufferLength = headerLength + payloadLength + trailerLength;

    // sys.puts("headerLength=" + headerLength + ", payloadLength=" + payloadLength + ", trailerLength=" + trailerLength + ", bufferLength=" + bufferLength);

    var buffer = new Buffer(bufferLength);
    var bufferStream = new BufferStream(buffer, 0);
    bufferStream.encodeFixed(this.magic);
    bufferStream.encodeNumber(CircularCacheFile.version);
    bufferStream.encodeNumber(headerLength);
    bufferStream.encodeString(sig);
    bufferStream.encodeNumber(metaData.length);
    bufferStream.encodeHashOfBuffer(metaData);
    bufferStream.encodeNumber(data.length);
    bufferStream.encodeHashOfBuffer(data);
    bufferStream.encodeFixed(this.magic);
    bufferStream.encodeBuffer(metaData);
    bufferStream.encodeBuffer(data);
    bufferStream.encodeFixed(this.magic);

    putBufferWithWrap(this.fd, this.maxSize, buffer, position, callback);
} 

CircularCacheFile.prototype.getHeader = function(position, sig, callback) {

    var magic = this.magic;
    var headerLength = this.getHeaderLength(sig);

    getBufferWithWrap(this.fd, this.maxSize, headerLength, position, function(err, headerBuffer)
        {
            if (err) {
                callback(err, null);
                return;
            }
            var bufferStream = new BufferStream(headerBuffer, 0);
            var magic1 = bufferStream.decodeFixed(magic.length);
            if (magic1 != magic) {
                callback("Header 1st magic incorrect", null);
                return;
            }
            var version = bufferStream.decodeNumber();
            if (version != CircularCacheFile.version) {
                callback("Version incorrect", null);
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
            var metaDataHash = bufferStream.decodeHash();
            var metaDataPosition = position + headerLength;
            var dataLength = bufferStream.decodeNumber();
            var dataHash = bufferStream.decodeHash();
            var dataPosition = position + headerLength + metaDataLength;
            var magic2 = bufferStream.decodeFixed(magic.length);
            if (magic2 != magic) {
                callback("Header 2nd magic incorrect", null);
                return;
            }
            callback(null, 
                { 
                    "version" : version,
                    "metaDataLength" : metaDataLength, 
                    "metaDataHash" : metaDataHash, 
                    "metaDataPosition" : metaDataPosition, 
                    "dataLength" : dataLength, 
                    "dataHash" : dataHash,
                    "dataPosition" : dataPosition
                });
        });
};
    
CircularCacheFile.prototype.getMetadata = function(position, sig, callback) 
{
    var fd = this.fd;
    var maxSize = this.maxSize;
    this.getHeader(position, sig, function(err, header) 
        {
            if (err) {
                callback(err, null);
                return;
            }
            getBufferWithWrap(fd, maxSize, header.metaDataLength, header.metaDataPosition, function(err, metaDataBuffer)
                {
                    if (err) {
                        callback(err, null);
                        return;
                    }
                    var metaDataBufferHash = BufferStream.computeHash(metaDataBuffer);
                    if (metaDataBufferHash != header.metaDataHash) {
                        callback("Metadata hash is incorrect", null);
                        return;
                    }
                    callback(null, metaDataBuffer);
                });
        });
}

CircularCacheFile.prototype.getData = function(position, sig, callback) 
{
    var fd = this.fd;
    var maxSize = this.maxSize;
    this.getHeader(position, sig, function(err, header) 
        {
            if (err) {
                callback(err, null, null);
                return;
            }
            var bufferLength = header.metaDataLength + header.dataLength;
            getBufferWithWrap(fd, maxSize, bufferLength, header.metaDataPosition, function(err, buffer)
                {
                    if (err) {
                        callback(err, null, null);
                        return;
                    }
                    var metaDataBuffer = buffer.slice(0, header.metaDataLength);
                    if (BufferStream.computeHash(metaDataBuffer) != header.metaDataHash) {
                        callback("Metadata hash is incorrect", null, null);
                        return;
                    }
                    var dataBuffer = buffer.slice(header.metaDataLength, bufferLength);
                    if (BufferStream.computeHash(dataBuffer) != header.dataHash) {
                        callback("Data hash is incorrect", null, null);
                        return;
                    }
                    callback(null, metaDataBuffer, dataBuffer);
                });
        });
}

module.exports = CircularCacheFile
