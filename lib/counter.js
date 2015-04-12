'use strict';

var _ = require('lodash'),
    Q = require('q'),
    moment = require('moment'),
    timeGranularities = require('./constants').timeGranularities,
    utils = require('./utils');

var defaults = {
  timeGranularity: timeGranularities.none
};

var momentFormat = 'YYYYMMDDHHmmss';

var parseTimeGranularity = function(timeGranularity) {
  timeGranularity = timeGranularities[timeGranularity];
  if (timeGranularity) return timeGranularity;
  else return timeGranularities.none;
};

/**
 * A timestamped event counter.
 *
 * This constructor is usually not called directly but through the
 * {@link RedisMetrics#counter} function.
 *
 * The timestamped counter stores one or more Redis keys based on the given
 * event name and time granularity appends a timestamp to the key before
 * storing the key in Redis. The counter can then report an overall aggregated
 * count or a specific count for a time range, depending on the chosen
 * granularity of the timestamp.
 *
 * If no time granularity is chosen at creation time, the counter will work
 * just like a global counter for the given key, i.e. events will not be
 * timestamped.
 *
 * @param {RedisMetrics} metrics - An instance of a RedisMetrics client.
 * @param {string} key - The base key to use for this counter.
 * @class
 */
function TimestampedCounter(metrics, key, options) {
  this.metrics = metrics;
  this.key = key;
  this.options = options || {};
  _.defaults(this.options, defaults);

  this.options.timeGranularity =
    parseTimeGranularity(this.options.timeGranularity);
}

/**
 * Return a list of Redis keys that are associated with this counter at the
 * current point in time and will be written.
 * @returns {Array}
 */
TimestampedCounter.prototype.getKeys = function() {
  var keys = [this.key]; // Always add the key itself.

  // If no time granularity is chosen, the timestamped keys will not be used so
  // just return the default key.
  if (this.options.timeGranularity === timeGranularities.none) {
    return keys;
  }

  var now = moment.utc().format(momentFormat);
  for (var i = 1; i <= this.options.timeGranularity; i++) {
    keys.push(this.key + ':' + now.slice(0, i*2+2));
  }
  return keys;
};

/**
 * Increments this counter with the given value.
 *
 * @param {function} [callback] - Optional callback.
 * @returns {Promise} A promise that resolves to the results from Redis. Can
 * be used instead of the callback function.
 */
TimestampedCounter.prototype.incr = function(callback) {
  var deferred = Q.defer();
  var cb = utils.createRedisCallback(deferred, callback);

  var keysToIncrement = this.getKeys();
  if (keysToIncrement.length === 1) {
    this.metrics.client.incr(keysToIncrement[0], cb);
  } else {
    var multi = this.metrics.client.multi();
    keysToIncrement.forEach(function(key) {
      multi.incr(key);
    });
    multi.exec(cb);
  }

  return deferred.promise;
};

/**
 * Returns the current count for this counter.
 *
 * If a specific time granularity is given, the value returned is the current
 * value at the given granularity level. Effectively, this provides a single
 * answer to questions such as "what is the count for the current day".
 *
 * @param {module:constants~timeGranularities} [timeGranularity] - The
 * granularity level to report the count for.
 * @param {function} [callback] - Optional callback.
 * @returns {Promise} A promise that resolves to the result from Redis. Can
 * be used instead of the callback function.
 */
TimestampedCounter.prototype.count = function(timeGranularity, callback) {
  if (typeof timeGranularity === 'function') {
    callback = timeGranularity;
    timeGranularity = timeGranularities.none;
  } else {
    timeGranularity = parseTimeGranularity(timeGranularity);
  }

  var deferred = Q.defer();
  var cb = utils.createRedisCallback(deferred, callback, utils.parseInt);
  this.metrics.client.get(this.getKeys()[timeGranularity], cb);
  return deferred.promise;
};

/**
 * Returns a list of counts in the given time range at a specific granularity
 * level.
 *
 * Notice: This function does not make sense for the "none" time granularity.
 *
 * @param {module:constants~timeGranularities} [timeGranularity] - The
 * granularity level to report the count for.
 * @param {Date} startDate - Start date for the range (inclusive)
 * @param {Date} [endDate=new Date()] - End date for the range (inclusive).
 * @param {function} [callback] - Optional callback.
 * @returns {Promise} A promise that resolves to the result from Redis. Can
 * be used instead of the callback function.
 */
TimestampedCounter.prototype.countRange =
    function(timeGranularity, startDate, endDate, callback) {
  timeGranularity = parseTimeGranularity(timeGranularity);
  endDate = endDate || moment.utc();
  var deferred = Q.defer();
  var cb = utils.createRedisCallback(deferred, callback, utils.parseIntArray);

  var momentRange = utils.momentRange(startDate, endDate, timeGranularity);
  var _this = this;
  var keyRange = momentRange.map(function(m) {
    return _this.key + ':' +
      m.format(momentFormat).slice(0, timeGranularity*2+2);
  });

  this.metrics.client.mget(keyRange, cb);

  return deferred.promise;
};

module.exports = TimestampedCounter;
