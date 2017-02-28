'use strict';

var redis = require('redis'),
    _ = require('lodash'),
    TimestampedCounter = require('./counter');

/**
 * A simple metrics utility for Redis. Creates a connection to Redis using the
 * given options or an already-connected Redis client.
 *
 * The `client` does not set up a catch-all error handler. If you rely on
 * handling uncaught exceptions, it's a good idea to create an error handler
 * (see the example).
 *
 * @param {Object} [config] - configuration options
 * @param {Object} [config.client] - An existing redis client.  The only
 * fully-supported client is
 * {@link https://www.npmjs.com/package/redis|node_redis}
 * @param {String} [config.host] - The Redis host to use.
 * @param {String} [config.port] - The Redis port to use.
 * @param {Object} [config.redisOptions] - The Redis options to use.
 * @param {Object} [config.counterOptions] - Default counter options to use if
 * none are provided when creating a counter. See {@link RedisMetrics#counter}
 * @example
 * var metrics = new RedisMetrics();
 * metrics.client.on('error', console.error);
 * metrics.counter('coolCounter').incr(1);
 * @class
 */
function RedisMetrics(config) {
  if (!(this instanceof RedisMetrics))
    return new RedisMetrics(config);

  this.config = config = config || {};

  if (this.config.client) {
    this.client = this.config.client;
  } else if (config.host && config.port) {
    var redisOptions = config.redisOptions || {};
    this.client = redis.createClient(config.port, config.host, redisOptions);
  } else if (config.redisOptions) {
    this.client = redis.createClient(config.redisOptions);
  } else {
    /**
     * The client connection
     * @member {Object}
     * **/
    this.client = redis.createClient();
  }
}

/**
 * Returns a timestamped counter for the given event.
 *
 * If the counter is initialized without options, it works like a normal event
 * counter that tracks a total count for the given event key. If a time
 * granularity option is specified, the counter will be able to report
 * aggregated counters based on time intervals down to the level of granularity
 * that is chosen.
 *
 * @param {string} eventName - The event that we want to count for.
 * @param {Object} [options] - The options to use for the counter
 * @param {number} [options.timeGranularity='none'] - Makes the counter use
 *   timestamps which means that it can be used to measure event metrics based
 *   on time intervals. The default is to not use time granularities.
 * @param {boolean} [options.expireKeys=true] - Automatically expire keys.
 *   Useful for high time granularity levels to preserve memory.
 * @returns {TimestampedCounter}
 * @since 0.1.0
 */
RedisMetrics.prototype.counter = function(eventName, options) {
  if (typeof options === 'undefined') {
    options = _.cloneDeep(this.config.counterOptions);
  }
  return new TimestampedCounter(this, eventName, options);
};

module.exports = RedisMetrics;
