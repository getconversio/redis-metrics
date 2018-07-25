# redis-metrics 📈

Easy metric tracking and aggregation using Redis.

This module was originally created to provide an easy way of storing and
viewing aggregated counter and trend statistics.

In a sense, the libary tries to provide sugar-coated method calls for storing
and fetching Redis data to report counts and trends. The first design goal is to
make counting simpler.

Read on below or read more on the [documentation site](http://getconversio.github.io/redis-metrics/).

[![Build Status](https://travis-ci.org/getconversio/redis-metrics.svg?branch=master)](https://travis-ci.org/getconversio/redis-metrics)
[![codecov](https://codecov.io/gh/getconversio/redis-metrics/branch/master/graph/badge.svg)](https://codecov.io/gh/getconversio/redis-metrics)

## Install

```console
$ npm install --save redis-metrics
```

## API

#### `RedisMetrics`

##### `new RedisMetrics({ client, host, port, redisOptions, counterOptions })`

Instances a new RedisMetrics instance with the given redis configuration. You can pass your own redis client or a host & port for a lib-managed connection.

##### `metrics.counter(eventName, { timeGranularity = 'none', expireKeys = true })`

Creates a new `TimestampedCounter`. The default options create a simple counter with no time granularity (basically, a total count only) that expires its keys automatically.

**`timeGranularity`:** can be one of 'none', 'year', 'month', 'day', 'hour', 'minute', 'second'. This indicates the minimum granularity for which to store individual counters. The more granular, the more memory used, and the quicker the expiration (if enabled).

#### `TimestampedCounter`

##### `counter.incr({ eventObj = null } = { })`

Increments a single counter by 1. An optional `eventObj` can be passed to increment a counter
for a specific event attribute. This enables using the `top` and `topRange` functions.

##### `counter.incrby(amount, { eventObj = null } = { })`

Same as `counter.incr` for > 1 increments.

##### `counter.count({ timeGranularity = 'total', eventObj = null })`

Returns the current count for this counter. Returns the total by default, but can return results for the current 'year', 'month', etc. Can optionally return results for a single `eventObj`.

##### `counter.countRange(timeGranularity, { startDate, endDate = new Date() }, { eventObj = null })`

Returns a timeseries of the current counts in the given granularity. Requires a `startDate` from which to start counting.

##### `counter.top({ timeGranularity = 'total', direction = 'desc', startingAt = 0, limit = -1 })`

Returns the list of top `eventObj` and their counts. By default, an all-time toplist is returned in descending order, and all event objects are included.

**`startingAt`:** specify a number of top scorers to skip
**`limit`:** specify to limit how many event objects & counts to return

##### `counter.topRange({ startDate, endDate }, { timeGranularity = 'total', direction = 'desc', startingAt = 0, limit = -1 })`

When given the `total` granularity, acts as `counter.top` but limits the results to the given time range. When given any other granularity, returns a timeseries of toplists for that granularity.

##### `counter.trimEvents({ direction = 'desc', limit = 1000 })`

Keeps only the top `limit` event objects for a given counter.

##### `counter.zero({ eventObj = null })`

Wipes all counters going back 5 years.

## Examples

### Basic counter

```javascript
// Create an instance
const RedisMetrics = require('redis-metrics');
const metrics = new RedisMetrics();

// If you need to catch uncaught exceptions, add an error handler to the client.
metrics.client.on('error', function(err) { /* ... */ });

// Create a counter for a "pageview" event and increment it three times.
const myCounter = metrics.counter('pageview');
myCounter.incr();
myCounter.incr();
myCounter.incr();

// Fetch the count for myCounter, using promise.
myCounter.count().then(function(cnt) {
  console.log(cnt); // Outputs 3 to the console.
});
```

### Time-aware counter

```javascript
const RedisMetrics = require('redis-metrics');
const metrics = new RedisMetrics();

// Use the timeGranularity option to specify how specific the counter should be
// when incrementing.
const myCounter = metrics.counter('pageview', { timeGranularity: 'hour' });
myCounter.incr();
myCounter.incr();
myCounter.incr();

// Fetch the count for myCounter for the current year.
myCounter.count({ timeGranularity: 'year' })
  .then(function(cnt) {
    console.log(cnt); // Outputs 3 to the console.
  });

// Fetch the count for each of the last two hours.
// We are using moment here for convenience.
const moment = require('moment');
const now = moment();
const lastHour = moment(now).subtract(1, 'hours');

myCounter.countRange('hour', { startDate: lastHour })
  .then(function(obj) {
    // "obj" is an object with timestamps as keys and counts as values.
    // For example something like this:
    // {
    //   '2015-04-15T11:00:00+00:00': 2,
    //   '2015-04-15T12:00:00+00:00': 3
    // }
  });

// Fetch the count for each day in the last 30 days
const thirtyDaysAgo = moment(now).subtract(30, 'days');
myCounter.countRange('day', { startDate: thirtyDaysAgo })
  .then(function(obj) {
    // "obj" contains counter information for each of the last thirty days.
    // For example something like this:
    // {
    //   '2015-03-16T00:00:00+00:00': 2,
    //   '2015-03-17T00:00:00+00:00': 3,
    //   ...
    //   '2015-04-15T00:00:00+00:00': 1
    // }
  });

// Fetch the count for the last 60 seconds...
// ... Sorry, you can't do that because the counter is only set up to track by
// the hour.
```

### Event Objects

```javascript
const myCounter = metrics.counter('pageview', { timeGranularity: 'hour' });

// You may want to get specific when counting an event. Use the `eventObj` option
// to store individual counters for different event attributes:
myCounter.incrby(2, { eventObj: '/page1.html' });
myCounter.incrby(5, { eventObj: '/page2.html' });
myCounter.incrby(8, { eventObj: '/page3.html' });

myCounter.count({ eventObj: '/page2.html' })
  .then(count => {
    console.log(count); // Outputs 5 to the console.
  });
```

### Top Event Object Counts

```javascript
const myCounter = metrics.counter('pageview', { timeGranularity: 'hour' });

// You may want to get specific when counting an event. Use the `eventObj` option
// to store individual counters for different event attributes:
myCounter.incrby(2, { eventObj: '/page1.html' });
myCounter.incrby(5, { eventObj: '/page2.html' });
myCounter.incrby(8, { eventObj: '/page3.html' });

myCounter.top()
  .then(toplist => {
    // toplist:
    // [
    //   { '/page3.html': 8 },
    //   { '/page2.html': 5 },
    //   { '/page1.html': 2 }
    // ]
  });

// a few months later...
myCounter.incrby(5, { eventObj: '/page1.html' });
myCounter.incrby(3, { eventObj: '/page2.html' });

myCounter.topRange({ startDate: moment().subtract(1, 'month') })
  .then(toplist => {
    // toplist:
    // [
    //   { '/page1.html': 5 },
    //   { '/page2.html': 3 }
    // ]
  });

```

## Redis Namespace

By default keys are stored in Redis as `c:{name}:{period}`. If you prefer to use a different Redis namespace than `c`, you can pass this in as an option:

```
const myCounter = metrics.counter('pageview', { timeGranularity: 'hour', namespace: 'stats' });`
```

## V2

See the [changelog](CHANGELOG.md).

## Test

Run tests including code coverage:

    $ npm test

## Documentation

The internal module documentation is based on [jsdoc](http://usejsdoc.org) and
can be generated with:

    $ npm run docs
