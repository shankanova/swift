var sys = require('sys'),
    assert = require('assert'),
    crypto = require('crypto'),
    fs = require('fs');
    BufferStream = require('../util/BufferStream');
   
Buffer.prototype.equals = function(other)
{
    assert.ok(other instanceof Buffer);
    if (this.length != other.length) 
    {
        return false;
    }
    for (var i = 0; i < this.length; ++i) 
    {
        if (this[i] != other[i]) 
        {
            return false;
        }
    }
    return true;
}

function read(fd, buffer, offset, length, position, callback) 
{
    assert.equal(typeof fd, 'number');
    assert.ok(buffer instanceof Buffer);
    assert.equal(typeof offset, 'number');
    assert.equal(typeof length, 'number');
    assert.equal(typeof position, 'number');

    var read = 0;
    var readCB = function(err, bytesRead)
        {
            // sys.puts("fs.read callback err=" + err + ", bytesRead=" + bytesRead);
            if (err) {
                callback(err, read);
                return;
            }
            if (bytesRead == 0) {
                callback("unexpected end of file", bytesRead);
                return;
            }
            read += bytesRead;
            if (read == length) {
                 // sys.puts("fs.read done, position=" + (position + read));
                 callback(null, read);
                 return;
            }
            sys.puts("fs.read partial read=" + read);
            fs.read(fd, buffer, offset + read, length - read, position + read, readCB);
        };
    // sys.puts("fs.read length=" + length + ", position=" + position);
    fs.read(fd, buffer, offset, length, position, readCB);
}

function getBuffer(fd, length, position, callback) 
{
    assert.equal(typeof fd, 'number');
    assert.equal(typeof length, 'number');
    assert.equal(typeof position, 'number');

    var buffer = new Buffer(length);
    read(fd, buffer, 0, length, position, function(err, bytesRead)
        {
            if (err) {
                callback(err, null);
                return;
            }
            assert.equal(bytesRead, buffer.length);
            callback(null, buffer);
        });
}

function getBufferWithWrap(fd, maxSize, length, position, callback)
{
    assert.equal(typeof fd, 'number');
    assert.equal(typeof maxSize, 'number');
    assert.equal(typeof length, 'number');
    assert.equal(typeof position, 'number');

    if (length > maxSize) {
        callback("length > maxSize", null);
        return;
    }
    position = position % maxSize;
    var endPosition = position + length;
    if (endPosition <= maxSize) {
        getBuffer(fd, length, position, callback); 
        return;
    }
    var left = 2;
    var totalBytes = 0;
    var buffer = new Buffer(length);
    var countDown = function(err, bytesRead)
        {
            if (err) {
                callback(err, null);
            }
            else {
                left--;
                totalBytes += bytesRead;
                if (left == 0) {
                    callback(null, buffer);
                    assert.equal(totalBytes, length);
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
    assert.equal(typeof fd, 'number');
    assert.ok(buffer instanceof Buffer);
    assert.equal(typeof offset, 'number');
    assert.equal(typeof length, 'number');
    assert.equal(typeof position, 'number');

    var wrote = 0;
    var writeCB = function(err, written) 
        {
            // sys.puts("fs.write callback err=" + err + ", written=" + written);
            if (err) {
                callback(err, wrote);
                return;
            }
            if (written == 0) {
                callback("unexpected 0-length write", written);
                return;
            }
            wrote += written;
            if (wrote == length) {
                // sys.puts("fs.write done, position=" + (position + wrote));
                callback(null, wrote);
                return;
            }
            sys.puts("fs.write partial wrote=" + wrote);
            fs.write(fd, buffer, offset + wrote, length - wrote, position + wrote, writeCB);
        };
    // sys.puts("fs.write length=" + length + ", position=" + position);
    fs.write(fd, buffer, offset, length, position, writeCB);
}

function putBuffer(fd, buffer, position, callback)
{
    assert.equal(typeof fd, 'number');
    assert.ok(buffer instanceof Buffer);
    assert.equal(typeof position, 'number');

    write(fd, buffer, 0, buffer.length, position, function(err, written)
        {
            if (err) {
                callback(err, null);
                return;
            }
            assert.equal(written, buffer.length);
            callback(null, position + written);
        });
}

function putBufferWithWrap(fd, maxSize, buffer, position, callback)
{
    assert.equal(typeof fd, 'number');
    assert.equal(typeof maxSize, 'number');
    assert.ok(buffer instanceof Buffer);
    assert.equal(typeof position, 'number');

    var length = buffer.length;
    if (length > maxSize) {
        callback("length > maxSize", null);
        return;
    }
    position = position % maxSize;
    var endPosition = position + length;
    if (endPosition <= maxSize) {
        putBuffer(fd, buffer, position, callback);
        return;
    }
    var left = 2;
    var totalBytes = 0;
    var countDown = function(err, wrote) 
        {
            if (err) { 
                callback(err, null);
            }
            else {
                left--;
                totalBytes += wrote;
                if (left == 0) {
                    assert.equal(totalBytes, buffer.length);
                    callback(null, (position + length) % maxSize);
                }
            }
        };
    var firstLength = maxSize - position;
    write(fd, buffer, 0, firstLength, position, countDown);
    var secondLength = length - firstLength;
    write(fd, buffer, firstLength, secondLength, 0, countDown);
}

function CircularCacheFile(fileName, maxSize, truncate) 
{
    assert.equal(typeof maxSize, 'number');

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

CircularCacheFile.version = 1;

CircularCacheFile.prototype.getHeaderLength = function(sig)
{
    assert.equal(typeof sig, 'string');

    var numberLength = BufferStream.numberLength;
    var hashLength = BufferStream.hashLength;
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
    assert.equal(typeof sig, 'string');
        
    return this.getHeaderLength(sig);
}

CircularCacheFile.prototype.put = function(position, sig, metaData, data, callback) 
{
    if (metaData == null) {
        metaData = new Buffer(0);
    }

    assert.ok(typeof position, 'number');
    assert.equal(typeof sig, 'string');
    assert.ok(metaData instanceof Buffer);
    assert.ok(data instanceof Buffer);

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
    bufferStream.encodeBuffer(buffer, 0, trailerLength);

    assert.equal(bufferLength, bufferStream.getOffset());

    putBufferWithWrap(this.fd, this.maxSize, buffer, position, callback);
} 

CircularCacheFile.prototype.getHeader = function(position, sig, callback) 
{
    assert.equal(typeof position, 'number');
    assert.equal(typeof sig, 'string');

    var magic = this.magic;
    var headerLength = this.getHeaderLength(sig);

    getBufferWithWrap(this.fd, this.maxSize, headerLength, position, function(err, headerBuffer)
        {
            if (err) {
                callback(err, null, null);
                return;
            }

            var bufferStream = new BufferStream(headerBuffer, 0);
            var magic1 = bufferStream.decodeFixed(magic.length);
            if (magic1 != magic) {
                callback("Header 1st magic incorrect", null, null);
                return;
            }
            var version = bufferStream.decodeNumber();
            if (version != CircularCacheFile.version) {
                callback("Version incorrect", null, null);
                return;
            }
            var headerLength = bufferStream.decodeNumber();
            if (headerLength != headerBuffer.length) {
                callback("Header length incorrect", null, null);
                return;
            }
            var headerSig = bufferStream.decodeString();
            if (headerSig != sig) {
                callback("Header sig does not match expected sig", null, null);
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
                callback("Header 2nd magic incorrect", null, null);
                return;
            }
 
            assert.equal(headerBuffer.length, bufferStream.getOffset());

            callback(null, 
                { 
                    "version" : version,
                    "metaDataLength" : metaDataLength, 
                    "metaDataHash" : metaDataHash, 
                    "metaDataPosition" : metaDataPosition, 
                    "dataLength" : dataLength, 
                    "dataHash" : dataHash,
                    "dataPosition" : dataPosition
                },
                headerBuffer);
        });
};
    
CircularCacheFile.prototype.getMetadata = function(position, sig, callback) 
{
    assert.equal(typeof position, 'number');
    assert.equal(typeof sig, 'string');

    var fd = this.fd;
    var maxSize = this.maxSize;
    this.getHeader(position, sig, function(err, header, headerBuffer) 
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

CircularCacheFile.prototype.get = function(position, sig, callback) 
{
    assert.equal(typeof position, 'number');
    assert.equal(typeof sig, 'string');

    var fd = this.fd;
    var maxSize = this.maxSize;
    this.getHeader(position, sig, function(err, header, headerBuffer) 
        {
            if (err) {
                callback(err, null, null);
                return;
            }
            var bufferLength = header.metaDataLength + header.dataLength + headerBuffer.length;
            getBufferWithWrap(fd, maxSize, bufferLength, header.metaDataPosition, function(err, buffer)
                {
                    if (err) {
                        callback(err, null, null);
                        return;
                    }
                    var trailerBuffer = buffer.slice(header.metaDataLength + header.dataLength, bufferLength);
                    if (!headerBuffer.equals(trailerBuffer)) 
                    {
                        callback("Trailer is incorrect", null, null);
                        return;
                    }
                    var metaDataBuffer = buffer.slice(0, header.metaDataLength);
                    if (BufferStream.computeHash(metaDataBuffer) != header.metaDataHash) {
                        callback("Metadata hash is incorrect", null, null);
                        return;
                    }
                    var dataBuffer = buffer.slice(header.metaDataLength, header.metaDataLength + header.dataLength);
                    if (BufferStream.computeHash(dataBuffer) != header.dataHash) {
                        callback("Data hash is incorrect", null, null);
                        return;
                    }
                    callback(null, metaDataBuffer, dataBuffer);
                });
        });
}

module.exports = CircularCacheFile
