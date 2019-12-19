'use strict';

/* eslint-disable no-new */

const redis = require('redis'),
  sinon = require('sinon'),
  chai = require('chai'),
  RedisMetrics = require('../lib/metrics'),
  TimestampedCounter = require('../lib/counter');

const { expect } = chai;

describe('Metric main', () => {
  let sandbox;
  beforeEach(() => {
    sandbox = sinon.sandbox.create();
  });

  afterEach(() => {
    sandbox.restore();
  });

  describe('constructor', () => {
    it('should create an instance with new keyword', () => {
      const metrics = new RedisMetrics();
      expect(metrics).to.be.instanceof(RedisMetrics);
    });

    it('should create a redis client', () => {
      const mock = sandbox.mock(redis)
        .expects('createClient')
        .once()
        .withExactArgs();
      new RedisMetrics();
      mock.verify();
    });

    it('should create a redis client with host and port if passed', () => {
      const mock = sandbox.mock(redis)
        .expects('createClient')
        .once()
        .withExactArgs(1234, 'abcd', {});
      new RedisMetrics({ host: 'abcd', port: 1234 });
      mock.verify();
    });

    it('should create a redis client with options if provided', () => {
      // jscs:disable requireCamelCaseOrUpperCaseIdentifiers
      const redisOpts = { no_ready_check: true };

      const mock = sandbox.mock(redis)
        .expects('createClient')
        .once()
        .withExactArgs(redisOpts);
      new RedisMetrics({ redisOptions: redisOpts });
      mock.verify();
    });

    it('should recycle the client if passed as an option', () => {
      const client = redis.createClient();

      const mock = sandbox.mock(redis)
        .expects('createClient')
        .never();

      new RedisMetrics({ client });
      mock.verify();
    });
  });

  describe('counter', () => {
    let metrics;
    beforeEach(() => metrics = new RedisMetrics());

    it('should return a counter for a key', () => {
      const counter = metrics.counter('foo');
      expect(counter).to.be.instanceof(TimestampedCounter);
    });

    it('should pass the options object to the counter constructor', () => {
      const options = {
        timeGranularity: 1
      };
      const counter = metrics.counter('foo', options);
      expect(counter.options.timeGranularity).to.equal(1);
    });

    it('should use the default options when none are provided', () => {
      metrics = new RedisMetrics({
        counterOptions: {
          timeGranularity: 2
        }
      });

      const counter = metrics.counter('foo');
      expect(counter.options.timeGranularity).to.equal(2);
    });
  });
});
