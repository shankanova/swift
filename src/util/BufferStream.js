var sys = require('sys'),
    assert = require('assert'),
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
    assert.equal(typeof number, 'number');

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
    assert.equal(typeof size, 'number');

    var fixed = this.buffer.toString('binary', this.offset, this.offset + size);
    this.offset += size;
    return fixed;
}

BufferStream.prototype.encodeFixed = function(fixed)
{
    assert.equal(typeof fixed, 'string');

    this.buffer.write(fixed, this.offset, 'binary');
    this.offset += fixed.length;
}

BufferStream.prototype.decodeHash = function()
{
    return this.decodeFixed(CircularCacheFile.hashLength);
}

BufferStream.prototype.encodeHashOfBuffer = function(buffer)
{
    assert.ok(buffer instanceof Buffer);

    this.encodeFixed(BufferStream.computeHash(buffer));
}

BufferStream.prototype.decodeBuffer = function(size)
{
    assert.equal(typeof size, 'number');

    var result = new Buffer(size);
    this.buffer.copy(result, 0, this.offset, this.offset + size);
    this.offset += size;
    return result;
}

BufferStream.prototype.encodeBuffer = function(buffer)
{
    assert.ok(buffer instanceof Buffer);

    buffer.copy(this.buffer, this.offset, 0, buffer.length);
    this.offset += buffer.length;
}

BufferStream.prototype.getOffset = function()
{
    return this.offset;
}

BufferStream.prototype.setOffset = function(newOffset)
{
    assert.equal(typeof newOffset, 'number');

    this.offset = newOffset;
}

module.exports = BufferStream;
