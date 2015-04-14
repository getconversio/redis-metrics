'use strict';

var util = require('util'),
    crypto = require('crypto'),
    RedisMetrics = require('./lib/metrics');

var metrics = new RedisMetrics();
var simpleCounter = metrics.counter('simple');
var hourCounter = metrics.counter('hour', {
  timeGranularity: 'hour'
});

var eventObjectCounter = metrics.counter('event');
var eventObjectHourCounter = metrics.counter('eventHour', {
  timeGranularity: 'hour'
});

function Test(options) {
  this.name = options.name;
  this.iterations = options.iterations;
  this.testFunction = options.testFunction;
  this.commandsCalled = 0;
}

Test.prototype.run = function(onComplete) {
  var _this = this;
  var start = process.hrtime();

  var testCallback = function() {
    _this.commandsCalled++;

    // If the number of completed commands equal the number of iterations
    // then report the result.
    if (_this.commandsCalled >= _this.iterations) {
      var diff = process.hrtime(start);
      var ms = diff[0] * 1000.0 + diff[1] / 1000000;
      var ops = parseInt(_this.iterations / ms * 1000.0);
      var result = util.format('%s took %d milliseconds, %d ops/sec',
        _this.name, ms, ops);
      onComplete(result);
    }
  };

  for (var i = 0; i < this.iterations; i++) {
    this.testFunction(testCallback, i);
  }
};

// Simple counter increment and count

var tests = [];
tests.push(new Test({
  name: 'incr simple 10000',
  iterations: 10000,
  testFunction: function(callback) {
    simpleCounter.incr(callback);
  }
}));

tests.push(new Test({
  name: 'count simple 10000',
  iterations: 10000,
  testFunction: function(callback) {
    simpleCounter.incr(callback);
  }
}));

// Counter with hourly time granularity

tests.push(new Test({
  name: 'incr hour 10000',
  iterations: 10000,
  testFunction: function(callback) {
    hourCounter.incr(callback);
  }
}));

tests.push(new Test({
  name: 'count hour 10000',
  iterations: 10000,
  testFunction: function(callback) {
    hourCounter.count(callback);
  }
}));

// Counter per user

var users = []
for (var i = 0; i < 10000; i++) {
  users.push(crypto.pseudoRandomBytes(16).toString('hex'));
}

tests.push(new Test({
  name: 'incr user counter simple 10000',
  iterations: 10000,
  testFunction: function(callback, iteration) {
    metrics
      .counter(users[iteration])
      .incr(callback);
  }
}));

tests.push(new Test({
  name: 'incr user counter hour 10000',
  iterations: 10000,
  testFunction: function(callback, iteration) {
    metrics
      .counter(users[iteration], {
        timeGranularity: 'hour'
      })
      .incr(callback);
  }
}));

tests.push(new Test({
  name: 'incr counter sorted user set 10000',
  iterations: 10000,
  testFunction: function(callback, iteration) {
    eventObjectCounter.incr(users[iteration], callback);
  }
}));

tests.push(new Test({
  name: 'incr hour counter sorted user set 10000',
  iterations: 10000,
  testFunction: function(callback, iteration) {
    eventObjectHourCounter.incr(users[iteration], callback);
  }
}));

function nextTest() {
  var test = tests.shift();
  if (test) {
    test.run(function(result) {
      console.log(result);
      nextTest();
    });
  } else {
    metrics.client.info('memory', function(err, res) {
      console.log(res);
      console.log('All done!');
      process.exit(0);
    });
  }
}

metrics.client.flushall(function() {
  nextTest();
});
