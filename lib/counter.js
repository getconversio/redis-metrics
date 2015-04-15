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
  this.key = 'c:' + key; // Pre-prend c to indicate it's a counter.
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
  // Always add the key itself.
  var keys = [this.key];

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
 *   determine what counter to increment.
 * @param {function} [callback] - Optional callback.
 * @returns {Promise} A promise that resolves to the results from Redis. Can
 *   be used instead of the callback function.
 * @since 0.1.0
 */
TimestampedCounter.prototype.incr = function(eventObj, callback) {
  return this.incrby(1, eventObj, callback);
};

/**
 * Increments this counter with the given amount.
 *
 * @param {number} amount - The amount to increment with.
 * @param {Object|string} [eventObj] - Extra event information used to
 *   determine what counter to increment. See {@link TimestampedCounter#incr}.
 * @param {function} [callback] - Optional callback.
 * @returns {Promise} A promise that resolves to the results from Redis. Can
 *   be used instead of the callback function.
 * @see {@link TimestampedCounter#incr}
 * @since 0.2.0
 */
TimestampedCounter.prototype.incrby = function(amount, eventObj, callback) {
  // The event object is optional so it might be a callback.
  if (_.isFunction(eventObj)) {
    callback = eventObj;
    eventObj = null;
  }
  if (eventObj) eventObj = String(eventObj);
  var deferred = Q.defer();
  var cb = utils.createRedisCallback(deferred, callback);
  this._incrby(amount, eventObj, cb);
  return deferred.promise;
};

TimestampedCounter.prototype._incrby = function(amount, eventObj, cb) {
  var keys = this.getKeys();
  // Optimize for the case where there is only a single key to increment.
  if (keys.length === 1) {
    if (eventObj) {
      this.metrics.client.zincrby(keys[0] + ':z', amount, eventObj, cb);
    } else {
      this.metrics.client.incrby(keys[0], amount, cb);
    }
  } else {
    var multi = this.metrics.client.multi();
    keys.forEach(function(key) {
      if (eventObj) {
        multi.zincrby(key + ':z', amount, eventObj);
      } else {
        multi.incrby(key, amount);
      }
    });
    multi.exec(cb);
  }
};

/**
 * Returns the current count for this counter.
 *
 * If a specific time granularity is given, the value returned is the current
 * value at the given granularity level. Effectively, this provides a single
 * answer to questions such as "what is the count for the current day".
 *
 * Notice that counts cannot be returned for a given time granularity if it was
 * not incremented at this granularity level in the first place.
 *
 * @example
 * myCounter.count(function(err, result) {
 *   console.log(result); // Outputs the global count
 * });
 * @example
 * myCounter.count('year', function(err, result) {
 *   console.log(result); // Outputs the count for the current year
 * });
 * @example
 * myCounter.count('year', '/foo.html', function(err, result) {
 *   // Outputs the count for the current year for the event object '/foo.html'
 *   console.log(result);
 * });
 *
 * @param {module:constants~timeGranularities} [timeGranularity='total'] - The
 *   granularity level to report the count for.
 * @param {string|object} [eventObj] - The event object. See
 *   {@link TimestampedCounter#incr} for more info on event objects.
 * @param {function} [callback] - Optional callback.
 * @returns {Promise} A promise that resolves to the result from Redis. Can
 *   be used instead of the callback function.
 * @since 0.1.0
 */
TimestampedCounter.prototype.count = function(
    timeGranularity, eventObj, callback) {
  var args = Array.prototype.slice.call(arguments);

  // Last argument is callback;
  callback = typeof args[args.length - 1] === 'function' ? args.pop() : null;

  // Event object requires that the time granularity is specified, otherwise we
  // can't reliably distinguish between them because both the eventObj and time
  // granularity can be strings. I miss Python.
  eventObj = args.length > 1 ? args.pop() : null;

  // Still any arguments left? That's a time granularity.
  timeGranularity = args.length > 0 ? args.pop() : 'none';
  timeGranularity = parseTimeGranularity(timeGranularity);

  var deferred = Q.defer();
  var cb = utils.createRedisCallback(deferred, callback, utils.parseInt);
  this._count(timeGranularity, eventObj, cb);
  return deferred.promise;
};

TimestampedCounter.prototype._count = function(timeGranularity, eventObj, cb) {
  var theKey = this.getKeys()[timeGranularity];
  if (eventObj) {
    this.metrics.client.zscore(theKey + ':z', eventObj, cb);
  } else {
    this.metrics.client.get(theKey, cb);
  }
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
 * @param {string|object} [eventObj] - The event object. See
 *   {@link TimestampedCounter#incr} for more info on event objects.
 * @param {function} [callback] - Optional callback.
 * @returns {Promise} A promise that resolves to the result from Redis. Can
 *   be used instead of the callback function.
 * @since 0.1.0
 */
TimestampedCounter.prototype.countRange = function(
    timeGranularity, startDate, endDate, eventObj, callback) {
  timeGranularity = parseTimeGranularity(timeGranularity);
  if (_.isFunction(eventObj)) {
    callback = eventObj;
    eventObj = null;
  }
  else if (_.isFunction(endDate)) {
    callback = endDate;
    endDate = moment.utc();
  } else {
    endDate = endDate || moment.utc();
  }
  if (eventObj) eventObj = String(eventObj);

  var momentRange = utils.momentRange(startDate, endDate, timeGranularity);
  var _this = this;
  var keyRange = [];
  var momentKeyRange = [];

  // Create the range of keys to fetch from Redis as well as the keys to use in
  // the returned data object.
  momentRange.forEach(function(m) {
    // Redis key range
    var mKeyFormat = m.format(momentFormat).slice(0, timeGranularity*2+2);
    keyRange.push(_this.key + ':' + mKeyFormat);

    // Timestamp range. Use ISO format for easy parsing back to a timestamp.
    momentKeyRange.push(m.format());
  });

  var deferred = Q.defer();
  var rangeParser = createRangeParser(momentKeyRange);
  var cb = utils.createRedisCallback(deferred, callback, rangeParser);

  this._countRange(keyRange, eventObj, cb);

  return deferred.promise;
};

TimestampedCounter.prototype._countRange = function(keys, eventObj, cb) {
  if (eventObj) {
    var multi = this.metrics.client.multi();
    keys.forEach(function(key) {
      multi.zscore(key + ':z', eventObj);
    });
    multi.exec(cb);
  } else {
    this.metrics.client.mget(keys, cb);
  }
};

module.exports = TimestampedCounter;
