### This repo is a copy of https://www.npmjs.com/package/redis-memoizer @ v1.0.2.
It was created due to the original repo being no longer available at the time of creation this repo.

redis-memoizer
===

An asynchronous function memoizer for node.js, using redis as the memo store. Memos expire after a specified timeout. Great as a drop-in performance optimization / caching layer for heavy asynchronous functions.

Wikipedia [explains it best](http://en.wikipedia.org/wiki/Memoization):
> ...memoization is an optimization technique used primarily to speed up computer programs by having function calls avoid repeating the calculation of results for previously processed inputs.

```javascript
var redisClient = require('redis').createClient(port, host, options)
var memoize = require("redis-memoizer")(redisClient);

function someExpensiveOperation(arg1, arg2, done) {
	// later...
	done();
}

var memoized = memoize(someExpensiveOperation);
```

Now, calls to `memoized` will have the same effect as calling `someExpensiveOperation`, except it will be much faster. The results of the first call are stored in redis and then looked up for subsequent calls.

Redis effectively serves as a shared network-available cache for function calls. Thus, the memoization cache is available across processes, so that if the same function call is made from different processes they will reuse the cache.

## Uses

Lets say you are making a DB call that's rather expensive. Let's say you've wrapped the call into a `getUserProfile` function that looks as follows:

```javascript
function getUserProfile(userId, done) {
	// Go over to the DB, perform expensive call, get user's profile
	done(err, userProfile);
}
```

Let's say this call takes 500ms, which is unacceptably high, and you want to make it faster, and don't care about the fact that the value of `userProfile` might be slightly outdated (until the cache timeout is hit in redis). You could simply do the following:

```javascript
var getMemoizedUserProfile = memoize(getUserProfile);

getMemoizedUserProfile("user1", function(err, userProfile) {
	// First call. This will take some time.

	getMemoizedUserProfile("user1", function(err, userProfile) {
		// Second call. This will be blazingly fast.
	});
});

```

This can similarly be used for any network or disk bound async calls where you are tolerant of slightly outdated values.

## Usage

### Initialization
```javascript
// Example configuration:
var memoize = require("redis-memoizer")(redisClient, {
	lookup_timeout: 500, // fast network
	default_ttl: 60 * 1000,
	memoize_key_namespace: require('./package.json').version,
	// Don't memoize server errors.
	memoize_errors_when: function(e) {
	  return !e.code || e.code < 500;
	}
});
```

Creates a memoization function. Requires an existing redis client.

`memoizerOptions` may have the following attributes:

* `lookup_timeout`: Time to wait for Redis to respond. If it times out, the cache get will be abandoned. Default `1000` (ms).
										If the TTL fed to `memoize` is shorter than this, it will be used instead.

* `default_ttl`: TTL to use if none is supplied. Defaults to `120000` (ms).

* `memoize_key_namespace`: Namespace to use under `memos:` in redis. Useful to e.g. invalidate all cache after
													 an application update; simply set this to the current git revision or use the boot timestamp.

* `memoize_errors_when`: A function that receives a parameter (`err`) and returns a boolean. If true, the error will be memoized.
                         By default, all errors are memoized.

### memoize(asyncFunction, [timeout])

Memoizes an async function and returns it.

* `asyncFunction` must be an asynchronous function that needs to be memoized. The last argument that the asyncFunction takes should be a callback in the usual node style.

* `timeout` (Optional) (Default: 120000 (ms)) is the amount of time in milliseconds for which the result of the function call should be cached in redis. Once the timeout is hit, the value is deleted from redis automatically. This is done using the redis [`psetex` command](http://redis.io/commands/psetex). The timeout is only set the first time, so the value expires after the timeout time has expired since the first call. The timeout is not reset with every call to the memoized function. Once the value has expired in redis, this module will treat the function call as though it's called the first time again. `timeout` can alternatively be a function, if you want to dynamically determine the cache time based on the data returned. The returned data will be passed into the timeout function.

	```javascript
	var httpCallMemoized = memoize(makeHttpCall, function(res) {
		// return a number based on say response's expires header
	});

	httpCallMemoized(function(res) { ... });
	```

## Cache Stampedes

This module makes some effort to minimize the effect of a [cache stampede](http://en.wikipedia.org/wiki/Cache_stampede). If multiple calls are made in quick succession before the first (async) call has completed, only the first call is actually really made. Note that redis will not have been populated at this time yet. Subsequent calls are queued up and are responded to as soon as the result of the first call is available.

Once all the calls have been responded to and the result of the computation is stored in redis, the module then switches to using the computed values from redis.

Note, cache stampedes can still happen if the same function is called from different processes, since the queueing logic described above happens in-memory. For the same set of arguments, you are likely to make as many calls as you have processes.

## Types

Note that this module does serialization to JSON. Special affordances are made for Date objects, which will be correctly returned
as Dates, but other, more complex types (like Functions) will not survive the serialization/deserialization.

## Quirks

If you're wrapping a function that you're going to memoize (say, in a promise to callback wrapper),
you will need to set the `_name` property on the function to ensure it is properly memoized. Otherwise,
redis-memoizer will be unable to tell the difference between them.

## Installation

Use npm to install @nlevo/redis-memoizer:
```
npm install @nlevo/redis-memoizer
```

To run the tests, install the dev-dependencies by `cd`'ing into `node_modules/redis-memoizer` and running `npm install` once, and then `npm test`.

## License

(The MIT License)

Copyright (c) 2012 Rakesh Pai <rakeshpai@errorception.com>

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
