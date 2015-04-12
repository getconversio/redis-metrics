redis-metrics
=============

Easy metric tracking and aggregation using Redis.

This module was originally created to provide an easy way of storing and
viewing aggregated counter and trend statistics.

In a sense, the libary tries to provide sugar-coated method calls for storing
and fetching Redis data to report counts and trends. The first design goal is to
make counting simpler.

Usage
----- 

```node
// Create an instance of the me
var RedisMetrics = require('redis-metrics');
var metrics = new RedisMetrics();

// Get a counter for a "pageview" event and increment it three times.
var myCounter = metrics.counter('pageview');
myCounter.incr();
myCounter.incr();
myCounter.incr();

// Fetch the count for myCounter, using a callback.
myCounter.count(function(cnt) {
  console.log(cnt); // Outputs 3 to the console.
});

// Fetch the count for myCounter, using promise.
myCounter.count().then(function(cnt) {
  console.log(cnt); // Outputs 3 to the console.
});
```

Test
----
Run tests including code coverage:

    $ npm test

Documentation
-------------
The internal module documentation is based on [jsdoc](http://usejsdoc.org) and
can be generated with:

    $ npm run-script docs
