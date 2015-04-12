'use strict';

var redis = require('redis'),
    sinon = require('sinon'),
    chai = require('chai'),
    expect = chai.expect,
    RedisMetrics = require('../lib/metrics'),
    TimestampedCounter = require('../lib/counter');

describe('Metric main', function() {

  var sandbox;
  beforeEach(function() {
    sandbox = sinon.sandbox.create();
  });

  afterEach(function() {
    sandbox.restore();
  });

  describe('constructor', function() {
    it('should create an instance with new keyword', function() {
      var metrics = new RedisMetrics();
      expect(metrics).to.be.instanceof(RedisMetrics);
    });

    it('should create an instance without new keyword', function() {
      var metrics = RedisMetrics();
      expect(metrics).to.be.instanceof(RedisMetrics);
    });

    it('should create a redis client', function() {
      var mock = sandbox.mock(redis)
        .expects('createClient')
        .once()
        .withExactArgs();
      RedisMetrics();
      mock.verify();
    });

    it('should create a redis client with hostname and port if provided', function() {
      var mock = sandbox.mock(redis)
        .expects('createClient')
        .once()
        .withExactArgs(1234, 'abcd', {});
      RedisMetrics({ host: 'abcd', port: 1234 });
      mock.verify();
    });

    it('should create a redis client with options if provided', function() {
      var redisOpts = { no_ready_check: true };

      var mock = sandbox.mock(redis)
        .expects('createClient')
        .once()
        .withExactArgs(redisOpts);
      RedisMetrics({ redisOptions: redisOpts });
      mock.verify();
    });
  });

  describe('counter', function() {
    var metrics;
    beforeEach(function() {
      metrics = new RedisMetrics();
    });

    it('should return a counter for a key', function() {
      var counter = metrics.counter('foo');
      expect(counter).to.be.instanceof(TimestampedCounter);
    });

    it('should pass the options object to the counter constructor', function() {
      var options = {
        timeGranularity: 'hour'
      };
      var counter = metrics.counter('foo', options);
      expect(counter.options).to.equal(options);
    });
  });

});
