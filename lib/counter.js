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
 * Creates a function that parses a list of Redis results and matches them up
 * with the given keyRange
 * @param {array} keyRange - The list of keys to match with the results.
 * @returns {function}
 * @private
 */
var createRangeParser = function(keyRange) {
  return function(results) {
    return _.zipObject(keyRange, utils.parseIntArray(results));
  };
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
 * @param {Object} options - The options to use for this counter. The available
 * options are specified in {@link RedisMetrics#counter}.
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
 * current point in time and will be written to Redis.
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
 * Increments this counter with 1.
 *
 * For some use cases, it makes sense to pass in an event object to get more
 * precise statistics for a specific event. For example, when counting page
 * views on a page, it makes sense to increment a counter per specific page.
 * For this use case, the eventObj parameter is a good fit.
 *
 * @param {Object|string} [eventObj] - Extra event information used to
 * determine what counter to increment.
 * @param {function} [callback] - Optional callback.
 * @returns {Promise} A promise that resolves to the results from Redis. Can
 * be used instead of the callback function.
 */
TimestampedCounter.prototype.incr = function(eventObj, callback) {
  if (typeof eventObj === 'function') {
    callback = eventObj;
    eventObj = null;
  }

  var deferred = Q.defer();
  var cb = utils.createRedisCallback(deferred, callback);

  var keys = this.getKeys();
  // Optimize for the case where there is only a single key to increment.
  if (keys.length === 1) {
    if (eventObj) {
      this.metrics.client.zincrby('cz:' + keys[0], 1, eventObj, cb);
    } else {
      this.metrics.client.incr('c:' + keys[0], cb);
    }
  } else {
    var multi = this.metrics.client.multi();
    keys.forEach(function(key) {
      if (eventObj) {
        multi.zincrby('cz:' + key, 1, eventObj);
      } else {
        multi.incr('c:' + key);
      }
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
 * Notice that counts cannot be returned for a given time granularity if it was
 * not incremented at this granularity in the first place.
 *
 * @example
 * myCounter.count(function(err, result) {
 *   console.log(result); // Outputs the global count
 * });
 * @example
 * myCounter.count('year', function(err, result) {
 *   console.log(result); // Outputs the count for the current year
 * });
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
  this.metrics.client.get('c:' + this.getKeys()[timeGranularity], cb);
  return deferred.promise;
};

/**
 * Returns a object mapping timestamps to counts in the given time range at a
 * specific time granularity level.
 *
 * Notice: This function does not make sense for the "none" time granularity.
 *
 * @param {module:constants~timeGranularities} timeGranularity - The
 *   granularity level to report the count for.
 * @param {Date|Object|string|number} startDate - Start date for the range
 *   (inclusive). Accepts the same argument as the constructor of a
 *   {@link http://momentjs.com/|moment} date.
 * @param {Date|Object|string|number} [endDate=new Date()] - End date for the
 *   range (inclusive). Accepts the same arguments as the constructor of a
 *   {@link http://momentjs.com/|moment} date.
 * @param {function} [callback] - Optional callback.
 * @returns {Promise} A promise that resolves to the result from Redis. Can
 *   be used instead of the callback function.
 */
TimestampedCounter.prototype.countRange =
    function(timeGranularity, startDate, endDate, callback) {
  timeGranularity = parseTimeGranularity(timeGranularity);
  if (typeof endDate === 'function') {
    callback = endDate;
    endDate = moment.utc();
  } else {
    endDate = endDate || moment.utc();
  }

  var momentRange = utils.momentRange(startDate, endDate, timeGranularity);
  var _this = this;
  var keyRange = [];
  var momentKeyRange = [];

  // Create the range of keys to fetch from Redis as well as the keys to use in
  // the returned data object.
  momentRange.forEach(function(m) {
    // Redis key range
    keyRange.push(
      'c:' +
      _this.key + ':' +
      m.format(momentFormat).slice(0, timeGranularity*2+2));

    // Timestamp range. Use ISO format for easy parsing back to a timestamp.
    momentKeyRange.push(m.format());
  });

  var deferred = Q.defer();
  var rangeParser = createRangeParser(momentKeyRange);
  var cb = utils.createRedisCallback(deferred, callback, rangeParser);

  this.metrics.client.mget(keyRange, cb);

  return deferred.promise;
};

module.exports = TimestampedCounter;
