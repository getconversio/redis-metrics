'use strict';

/**
 * Benchmark to push the memory.
 */

var crypto = require('crypto'),
    sinon = require('sinon'),
    async = require('async'),
    RedisMetrics = require('../lib/metrics');

var metrics = new RedisMetrics();
var counter = metrics.counter('impression', {
  timeGranularity: 'hour'
});

metrics.client.on('error', function(err) {
  console.log('Err', err);
});

var users = [];
for (var i = 0; i < 10000; i++) {
  users.push(crypto.pseudoRandomBytes(16).toString('hex'));
}

var hours = [];
for (var hour = 0; hour < 24; hour++) {
  hours.push(hour);
}

var clock = sinon.useFakeTimers();
async.eachSeries(hours, function(hour, hourDone) {
  console.log('Time is now:', new Date());
  async.eachSeries(users, function(user, userDone) {
    counter.incr(user, userDone);
  }, function(err) {
    clock.tick(1000*60*60);
    hourDone(err);
  });
}, function(err) {
  console.log('All done', err);
  clock.restore();
  process.exit(0);
});
