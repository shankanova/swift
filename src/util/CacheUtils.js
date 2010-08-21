var crypto = require("crypto"),
	url = require("url")
	;

function computeCacheKey(request) {
	var hash = crypto.createHash('md5');
	hash.update(request.url);
	var sig = hash.digest('binary');
	return sig;
}


module.exports = { computeCacheKey : computeCacheKey } 
