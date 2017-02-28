'use strict';

var redis = require('redis'),
  sinon = require('sinon'),
  chai = require('chai'),
  RedisMetrics = require('../lib/metrics'),
  TimestampedCounter = require('../lib/counter');

var expect = chai.expect;

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
      // This is not encouraged though :-)
      var metrics = require('../lib/metrics')();
      expect(metrics).to.be.instanceof(RedisMetrics);
    });

    it('should create a redis client', function() {
      var mock = sandbox.mock(redis)
        .expects('createClient')
        .once()
        .withExactArgs();
      new RedisMetrics();
      mock.verify();
    });

    it('should create a redis client with host and port if passed', function() {
      var mock = sandbox.mock(redis)
        .expects('createClient')
        .once()
        .withExactArgs(1234, 'abcd', {});
      new RedisMetrics({ host: 'abcd', port: 1234 });
      mock.verify();
    });

    it('should create a redis client with options if provided', function() {
      // jscs:disable requireCamelCaseOrUpperCaseIdentifiers
      var redisOpts = { no_ready_check: true };

      var mock = sandbox.mock(redis)
        .expects('createClient')
        .once()
        .withExactArgs(redisOpts);
      new RedisMetrics({ redisOptions: redisOpts });
      mock.verify();
    });

    it('should recycle the client if passed as an option', function() {
      var client = redis.createClient();

      var mock = sandbox.mock(redis)
        .expects('createClient')
        .never();

      new RedisMetrics({ client: client });
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
        timeGranularity: 1
      };
      var counter = metrics.counter('foo', options);
      expect(counter.options.timeGranularity).to.equal(1);
    });

    it('should use the default options when none are provided', function() {
      metrics = new RedisMetrics({
        counterOptions: {
          timeGranularity: 2
        }
      });

      var counter = metrics.counter('foo');
      expect(counter.options.timeGranularity).to.equal(2);
    });
  });

});
