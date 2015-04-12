'use strict';

var redis = require('redis'),
    TimestampedCounter = require('./counter');

/**
 * A simple metrics utility for Redis.
 *
 * @param {Object} [config] - configuration options
 * @param {string} [config.host] - The Redis host to use.
 * @param {string} [config.port] - The Redis port to use.
 * @param {Object} [config.redisOptions] - The Redis options to use.
 * @class
 */
function RedisMetrics(config) {
  if (!(this instanceof RedisMetrics))
    return new RedisMetrics(config);

  this.config = config = config || {};

  if (config.host && config.port) {
    var redisOptions = config.redisOptions || {};
    this.client = redis.createClient(config.port, config.host, redisOptions);
  } else if (config.redisOptions) {
    this.client = redis.createClient(config.redisOptions);
  } else {
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
 * @param {number} [options.timeGranularity] - Makes the counter use timestamps
 * which means that it can be used to measure event metrics based on time
 * intervals.
 * @returns {TimestampedCounter}
 */
RedisMetrics.prototype.counter = function(eventName, options) {
  return new TimestampedCounter(this, eventName, options);
};

module.exports = RedisMetrics;
