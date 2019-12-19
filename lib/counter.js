'use strict';

/* eslint-disable no-restricted-syntax */
/* eslint-disable prefer-rest-params */

const _ = require('lodash'),
  moment = require('moment'),
  { timeGranularities } = require('./constants'),
  utils = require('./utils'),
  lua = require('./lua');

// The default expiration times aim at having less than 800 counters for a
// given counter key (event) since the counter keys are stored with a specific
// timestamp. For example, second-based counters expire after 10 minutes which
// means that there will be 600 counter keys in the worst-case for a single
// event.
const defaultExpiration = {
  total: -1,
  year: -1,
  month: 10 * 365 * 24 * 60 * 60, // 10 years = 120 counters worst-case
  day: 2 * 365 * 24 * 60 * 60,    // 2 years = 730 counters worst-case
  hour: 31 * 24 * 60 * 60,        // 31 days = 744 counters worst-case
  minute: 12 * 60 * 60,           // 12 hours = 720 counters worst-case
  second: 10 * 60                 // 10 minutes = 600 counters worst-case
};
// BTW, good luck keeping your Redis server around for 10 years :-)

// Translate the "nice looking expiration times above to "real" keys that
// correspond to time granularities.
_.keys(defaultExpiration).forEach(key => {
  const newKey = timeGranularities[key];
  const value = defaultExpiration[key];
  delete defaultExpiration[key];
  defaultExpiration[newKey] = value;
});

const defaults = {
  namespace: 'c', // Short for counter.
  timeGranularity: timeGranularities.none,
  expireKeys: true,
  expiration: defaultExpiration
};

const momentFormat = 'YYYYMMDDHHmmss';

/**
 * Convert the given granularity level into the internal representation (a
 * number).
 * @returns {module:constants~timeGranularities}
 * @private
 */
const parseTimeGranularity = timeGranularity => {
  timeGranularity = timeGranularities[timeGranularity];
  if (timeGranularity) return timeGranularity;
  return timeGranularities.none;
};

/**
 * Return a list of Redis keys that are associated with this counter at the
 * current point in time and will be written to Redis.
 * @param {String} baseKey - The base key to use.
 * @param {String} formattedTimestamp - A formatted timestamp string.
 * @param {module:constants~timeGranularities} timeGranularity - The
 *   granularity level to return keys for.
 * @returns {Array} Contains the counter keys for the given timestamp.
 * @private
 */
const getKeys = (baseKey, formattedTimestamp, timeGranularity) => {
  // Always add the baseKey itself.
  const keys = [baseKey];

  // If no time granularity is chosen, the timestamped keys will not be used so
  // just return the default key.
  if (timeGranularity === timeGranularities.none) {
    return keys;
  }

  for (let i = 1; i <= timeGranularity; i++) {
    keys.push(`${baseKey}:${formattedTimestamp.slice(0, i * 2 + 2)}`);
  }

  return keys;
};

/**
 * Creates a function that parses a list of Redis results and matches them up
 * with the given keyRange
 * @param {array} keyRange - The list of keys to match with the results.
 * @returns {function}
 * @private
 */
const createRangeParser = keyRange => results => (
  _.zipObject(keyRange, utils.parseIntArray(results))
);

/**
 * Creates a function that parses a list of Redis results and returns the total.
 * @returns {function}
 * @private
 */
const createRangeTotalParser = () => results => _.sum(utils.parseIntArray(results));

const incrSingle = (client, key, amount, eventObj, ttl, cb) => {
  if (utils.isNil(eventObj)) {
    if (ttl > 0) {
      if (cb) client.eval(lua.incrbyExpire, 1, key, amount, ttl, cb);
      else client.eval(lua.incrbyExpire, 1, key, amount, ttl);
    } else if (cb) client.incrby(key, amount, cb);
    else client.incrby(key, amount);
  } else { // No event object
    key += ':z';

    if (ttl > 0) {
      if (cb) client.eval(lua.zincrbyExpire, 1, key, amount, eventObj, ttl, cb);
      else client.eval(lua.zincrbyExpire, 1, key, amount, eventObj, ttl);
    } else if (cb) client.zincrby(key, amount, eventObj, cb);
    else client.zincrby(key, amount, eventObj);
  }
};

const zero = (client, key, eventObj, cb) => {
  if (utils.isNil(eventObj)) {
    client.del(key, cb);
  } else {
    key += ':z';
    client.zrem(key, eventObj, cb);
  }
};

/**
 * Parse a rank result from redis
 * @param  {array} rank In this format: [ 'foo', '39', 'bar', '13' ]
 * @return {object} In this format: [ { foo: 39 }, { bar: 13 } ]
 * @private
 */
const rankParser = rank => {
  return _.chain(rank)
    .chunk(2)
    .map(([key, count]) => ({ [key]: Number(count) }))
    .value();
};

const createRankRangeParser = keyRange => results => _.zipObject(keyRange, results.map(rankParser));

/**
 * Parse & merge multiple rank results from redis.
 * @param  {array}  ranks In this format: [['foo', '39', 'bar', '13'], ['foo', '11']]
 * @return {object}       In this format: [{ foo: 50 }, { bar: 13 }]
 */
const createRankTotalParser = (direction, startingAt, limit) => ranks => {
  // TODO-V4 destructure here
  return _.chain(ranks)
    .flatten()
    .chunk(2)
    .reduce((acc, metric) => {
      acc[metric[0]] = (acc[metric[0]] || 0) + parseInt(metric[1], 10);
      return acc;
    }, { })
    .toPairs()
    .sortBy(metric => direction === 'asc' ? metric[1] : -metric[1])
    .drop(startingAt)
    .takeWhile((value, index) => limit <= 0 || index < limit)
    .map(metric => {
      const obj = {};
      [, obj[metric[0]]] = metric;
      return obj;
    })
    .value();
};

/**
 * Parse a granularity and return it as a report granularity - that which was
 * requested for the final result - and as a query granularity - that which will
 * be used to generate the query timestamps.
 *
 * @param  {String} timeGranularity The requested granularity for the result.
 * @return {Object}                 An object { reportTimeGranularity, rangeTimeGranularity }
 */
const parseRangeTimeGranularities = (timeGranularity, counterGranularity) => {
  timeGranularity = parseTimeGranularity(timeGranularity);

  // Save the report time granularity because it might change for the query.
  const reportTimeGranularity = timeGranularity;

  // If the range granularity is total, fall back to the granularity specified
  // at the counter level and then add the numbers together when parsing the
  // result.
  if (timeGranularity === timeGranularities.total) {
    timeGranularity = counterGranularity;

    // If the rangeGranularity is still total, it does not make sense to report
    // a range for the counter and we throw an error.
    if (timeGranularity === timeGranularities.total) {
      throw new Error('total granularity not supported for this counter');
    }
  }

  return { reportTimeGranularity, rangeTimeGranularity: timeGranularity };
};

/**
 * A timestamped event counter.
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
 * **Notice**: The constructor for this class is usually not called directly
 * but through the {@link RedisMetrics#counter} function.
 *
 * @param {RedisMetrics} metrics - An instance of a RedisMetrics client.
 * @param {string} key - The base key to use for this counter.
 * @param {Object} options - The options to use for this counter. The available
 *   options are specified in {@link RedisMetrics#counter}.
 * @class
 */
class TimestampedCounter {
  constructor(metrics, key, options) {
    this.options = options || {};
    _.defaults(this.options, _.cloneDeep(defaults));

    this.metrics = metrics;
    this.key = this.options.namespace + ':' + key;

    // Translate the expiration keys of the options.
    _.keys(this.options.expiration).forEach(key => {
      const newKey = timeGranularities[key];
      const value = this.options.expiration[key];
      delete this.options.expiration[key];
      this.options.expiration[newKey] = value;
    });

    this.options.timeGranularity = parseTimeGranularity(this.options.timeGranularity);
  }

  /**
   * Return a list of Redis keys that are associated with this counter at the
   * current point in time (default) and will be written to Redis.
   * @param {Moment} [time=now] - A specific time to get keys for.
   * @param {module:constants~timeGranularities} [timeGranularity] - The
   *   granularity level to return keys for. The default is the granularity from
   *   the options.
   * @returns {Array}
   */
  getKeys(time, timeGranularity) {
    return getKeys(
      this.key,
      (time || moment.utc()).format(momentFormat),
      parseTimeGranularity(timeGranularity) || this.options.timeGranularity
    );
  }

  /**
   * Finds the configured time to live for the given key.
   * @param {string} key - The full key (including timestamp) for the key to
   *   determine the ttl for.
   * @returns {number} Number of seconds that the key should live.
   */
  getKeyTTL(key) {
    if (!this.options.expireKeys) return -1;

    const timePart = key.replace(this.key, '').split(':')[1] || '';
    let timeGranularity = timeGranularities.none;
    switch (timePart.length) {
      case 4:
        timeGranularity = timeGranularities.year;
        break;
      case 6:
        timeGranularity = timeGranularities.month;
        break;
      case 8:
        timeGranularity = timeGranularities.day;
        break;
      case 10:
        timeGranularity = timeGranularities.hour;
        break;
      case 12:
        timeGranularity = timeGranularities.minute;
        break;
      case 14:
        timeGranularity = timeGranularities.second;
        break;
    }
    let ttl = this.options.expiration[timeGranularity];
    if (typeof ttl === 'undefined') ttl = defaultExpiration[timeGranularity];
    return ttl;
  }

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
  incr(eventObj, callback) {
    return this.incrby(1, eventObj, callback);
  }

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
  incrby(amount, eventObj, callback) {
    // The event object is optional so it might be a callback.
    if (_.isFunction(eventObj)) {
      callback = eventObj;
      eventObj = null;
    }
    if (!utils.isNil(eventObj)) eventObj = String(eventObj);
    const deferred = utils.defer();
    const cb = utils.createRedisCallback(deferred, callback);
    this._incrby(amount, eventObj, cb);
    return deferred.promise;
  }

  _incrby(amount, eventObj, cb) {
    const keys = this.getKeys();
    // Optimize for the case where there is only a single key to increment.
    if (keys.length === 1) {
      incrSingle(this.metrics.client, keys[0], amount, eventObj, this.getKeyTTL(keys[0]), cb);
    } else {
      const multi = this.metrics.client.multi();
      keys.forEach(key => {
        incrSingle(multi, key, amount, eventObj, this.getKeyTTL(key));
      });
      multi.exec(cb);
    }
  }

  /**
   * Returns the current count for this counter.
   *
   * If a specific time granularity is given, the value returned is the current
   * value at the given granularity level. Effectively, this provides a single
   * answer to questions such as "what is the count for the current day".
   *
   * **Notice**: Counts cannot be returned for a given time granularity if it was
   * not incremented at this granularity level in the first place.
   *
   * @example
   * myCounter.count((err, result) => {
   *   console.log(result); // Outputs the global count
   * });
   * @example
   * myCounter.count('year', (err, result) => {
   *   console.log(result); // Outputs the count for the current year
   * });
   * @example
   * myCounter.count('year', '/foo.html', (err, result) => {
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
  count(timeGranularity, eventObj, callback) {
    const args = Array.prototype.slice.call(arguments);

    // Last argument is callback;
    callback = typeof args[args.length - 1] === 'function' ? args.pop() : null;

    // Event object requires that the time granularity is specified, otherwise we
    // can't reliably distinguish between them because both the eventObj and time
    // granularity can be strings. I miss Python.
    eventObj = args.length > 1 ? args.pop() : null;

    // Still any arguments left? That's a time granularity.
    timeGranularity = args.length > 0 ? args.pop() : 'none';
    timeGranularity = parseTimeGranularity(timeGranularity);

    const deferred = utils.defer();
    const cb = utils.createRedisCallback(deferred, callback, utils.parseInt);
    this._count(timeGranularity, eventObj, cb);
    return deferred.promise;
  }

  _count(timeGranularity, eventObj, cb) {
    const theKey = this.getKeys()[timeGranularity];
    if (utils.isNil(eventObj)) {
      this.metrics.client.get(theKey, cb);
    } else {
      this.metrics.client.zscore(theKey + ':z', eventObj, cb);
    }
  }

  /**
   * Returns an object mapping timestamps to counts in the given time range at a
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
  countRange(timeGranularity, startDate, endDate, eventObj, callback) {
    if (_.isFunction(eventObj)) {
      callback = eventObj;
      eventObj = null;
    } else if (_.isFunction(endDate)) {
      callback = endDate;
      endDate = moment.utc();
    } else {
      endDate = endDate || moment.utc();
    }
    if (!utils.isNil(eventObj)) eventObj = String(eventObj);

    // TODO-V4 destructure here
    const grans = parseRangeTimeGranularities(timeGranularity, this.options.timeGranularity);
    const { reportTimeGranularity } = grans;
    const { rangeTimeGranularity } = grans;

    const momentRange = utils.momentRange(startDate, endDate, rangeTimeGranularity);
    const keyRange = [];
    const momentKeyRange = [];

    // Create the range of keys to fetch from Redis as well as the keys to use in
    // the returned data object.
    momentRange.forEach(m => {
      // Redis key range
      const mKeyFormat = m.format(momentFormat).slice(0, rangeTimeGranularity * 2 + 2);
      keyRange.push(`${this.key}:${mKeyFormat}`);

      // Timestamp range. Use ISO format for easy parsing back to a timestamp.
      momentKeyRange.push(m.format());
    });

    const deferred = utils.defer();
    const parser = reportTimeGranularity === timeGranularities.total ?
      createRangeTotalParser() : createRangeParser(momentKeyRange);
    const cb = utils.createRedisCallback(deferred, callback, parser);

    this._countRange(keyRange, eventObj, cb);

    return deferred.promise;
  }

  _countRange(keys, eventObj, cb) {
    if (utils.isNil(eventObj)) {
      this.metrics.client.mget(keys, cb);
    } else {
      const multi = this.metrics.client.multi();
      keys.forEach(key => multi.zscore(key + ':z', eventObj));
      multi.exec(cb);
    }
  }

  /**
   * Returns the current top elements for this counter. This only makes sense
   * for counters with event objects.
   *
   * If a specific time granularity is given, the value returned is the current
   * value at the given granularity level. Effectively, this provides a single
   * answer to questions such as "what is the rank for the current day".
   *
   * @example
   * myCounter.top((err, result) => {
   *   console.log(result); // Outputs the global rank
   * });
   * @example
   * myCounter.top('year', (err, result) => {
   *   console.log(result); // Outputs the rank for the current year
   * });
   *
   * @param {module:constants~timeGranularities} [timeGranularity='total'] - The
   *   granularity level to report the rank for.
   * @param {string} [direction=desc] - Optional sort direction, can be "asc" or "desc"
   * @param {integer} [startingAt=0] - Optional starting row.
   * @param {integer} [limit=-1] - Optional number of results to return.
   * @param {function} [callback] - Optional callback.
   * @returns {Promise} A promise that resolves to the result from Redis. Can
   *   be used instead of the callback function.
   * @since 0.1.1
   */
  top(timeGranularity, direction, startingAt, limit, callback) {
    const args = Array.prototype.slice.call(arguments);

    // Last argument is callback;
    callback = typeof args[args.length - 1] === 'function' ? args.pop() : null;

    limit = args.length > 3 ? args.pop() : -1;
    startingAt = args.length > 2 ? args.pop() : 0;
    direction = args.length > 1 ? args.pop() : 'desc';

    if (['asc', 'desc'].indexOf(direction) === -1) {
      throw new Error(
        'The direction parameter is expected to be one between ' +
        '"asc" or "desc", got "' + direction + '".'
      );
    }

    timeGranularity = parseTimeGranularity(timeGranularity);

    const deferred = utils.defer();
    const cb = utils.createRedisCallback(deferred, callback, rankParser);
    this._top(timeGranularity, direction, startingAt, limit, cb);
    return deferred.promise;
  }

  _top(timeGranularity, direction, startingAt, limit, cb) {
    const theKey = this.getKeys()[timeGranularity];

    if (direction === 'asc') {
      return this.metrics.client.zrange(
        theKey + ':z',
        startingAt,
        limit,
        'WITHSCORES',
        cb
      );
    }

    this.metrics.client.zrevrange(
      theKey + ':z',
      startingAt,
      limit,
      'WITHSCORES',
      cb
    );
  }

  /**
   * Returns the top elements for this counter for a given time range. Like
   * `top`, this only makes sense for counters with event objects. Like
   * `countRange`, this does not make sense for counters with the 'none'
   * granularity.
   *
   * Options are the same as `top`. Result is an object of timestamps to
   * toplist mappings. When the 'total' granularity is requested, an toplist is
   * returned instead.
   *
   * @param {Date|Object|string|number} startDate - Start date for the range
   *   (inclusive). Accepts the same argument as the constructor of a
   *   {@link http://momentjs.com/|moment} date.
   * @param {Date|Object|string|number} [endDate=new Date()] - End date for the
   *   range (inclusive). Accepts the same arguments as the constructor of a
   *   {@link http://momentjs.com/|moment} date.
   * @param {module:constants~timeGranularities} timeGranularity - The
   *   granularity level to report the count for.
   * @param {string} [direction=desc] - Optional sort direction, can be "asc" or "desc"
   * @param {integer} [startingAt=0] - Optional starting row.
   * @param {integer} [limit=-1] - Optional number of results to return.
   * @param {function} [callback] - Optional callback.
   * @returns {Promise} A promise that resolves to the result from Redis. Can
   *   be used instead of the callback function.
   * @since 1.3.0
   */
  topRange(startDate, endDate, timeGranularity, direction, startingAt, limit, callback) {
    const args = Array.prototype.slice.call(arguments);

    // Last argument is callback;
    callback = typeof args[args.length - 1] === 'function' ? args.pop() : null;

    limit = args.length > 5 ? args.pop() : -1;
    startingAt = args.length > 4 ? args.pop() : 0;
    direction = args.length > 3 ? args.pop() : 'desc';
    if (args.length > 2) args.pop(); // default handled in parseTimeGranularity;
    endDate = args.length > 1 ? args.pop() : moment.utc();

    if (!startDate) {
      throw new Error('The startDate parameter is required.');
    }

    if (['asc', 'desc'].indexOf(direction) === -1) {
      throw new Error(
        'The direction parameter is expected to be one between ' +
        '"asc" or "desc", got "' + direction + '".'
      );
    }

    // TODO-V4 destructure here
    const grans = parseRangeTimeGranularities(timeGranularity, this.options.timeGranularity);
    const { reportTimeGranularity } = grans;
    const { rangeTimeGranularity } = grans;

    const momentRange = utils.momentRange(startDate, endDate, rangeTimeGranularity);
    const keyRange = [];
    const momentKeyRange = [];

    // Create the range of keys to fetch from Redis as well as the keys to use in
    // the returned data object.
    momentRange.forEach(m => {
      // Redis key range
      const mKeyFormat = m.format(momentFormat).slice(0, rangeTimeGranularity * 2 + 2);
      keyRange.push(`${this.key}:${mKeyFormat}`);

      // Timestamp range. Use ISO format for easy parsing back to a timestamp.
      momentKeyRange.push(m.format());
    });

    const deferred = utils.defer();

    if (reportTimeGranularity === timeGranularities.total) {
      const cb = utils.createRedisCallback(
        deferred,
        callback,
        createRankTotalParser(direction, startingAt, limit)
      );

      this._topRange(keyRange, 'asc', 0, -1, cb); // get everything and re-count on parser
    } else {
      const cb = utils.createRedisCallback(
        deferred,
        callback,
        createRankRangeParser(momentKeyRange)
      );

      this._topRange(keyRange, direction, startingAt, limit, cb);
    }

    return deferred.promise;
  }

  _topRange(keys, direction, startingAt, limit, cb) {
    const multi = this.metrics.client.multi();

    keys.forEach(key => {
      const redisFn = direction === 'asc' ? 'zrange' : 'zrevrange';
      multi[redisFn](
        key + ':z',
        startingAt,
        limit,
        'WITHSCORES'
      );
    });

    multi.exec(cb);
  }

  /**
   * Permanently remove event objects for this counter so only the top-N
   * elements remain in either descending (the default) or ascending order.
   *
   * The event objects are removed with Redis' removal by rank for sorted sets
   * ({@link https://redis.io/commands/zremrangebyrank|ZREMRANGEBYRANK}).
   *
   * The removal happens at each granularity level and only supports daily
   * granularity and above (month, year, total) to optimize for the number of
   * Redis keys to consider. It removes data going 5 years back in time.
   *
   * If the function is used too often, there is a risk that the top-N elements
   * will stay the same forever, because lower-ranking elements get removed
   * before their scores have a change to increase.
   *
   * Therefore, the function should only be used to try and reclaim space for
   * big counters with a lot of event objects, and only rarely used.
   *
   * @param {string} [keepDirection=desc] - Sort direction for the top-N
   * elements to keep, can be "asc" or "desc".
   * @param {integer} [limit=1000] - Number of results to keep.
   * @param {function} [callback] - Callback
   * @returns {Promise} Resolves when the values have been removed.
   * @example
   * // Keeps the top-5 elements with highest score
   * myCounter.trimEvents('desc', 5)
   * @example
   * // Keeps the top-5 elements with lowest score
   * myCounter.trimEvents('asc', 5)
   * @since 1.1.0
   */
  trimEvents(keepDirection, limit, callback) {
    const args = Array.prototype.slice.call(arguments);
    callback = typeof args[args.length - 1] === 'function' ? args.pop() : null;
    limit = args.length > 1 ? args.pop() : 1000;
    keepDirection = args.length > 0 ? args.pop() : 'desc';

    if (['asc', 'desc'].indexOf(keepDirection) === -1) {
      throw new Error(
        'The keepDirection parameter is expected to be one between ' +
        '"asc" or "desc", got "' + keepDirection + '".'
      );
    }

    // If we want to keep top-5 lowest scores, remove rank 5 to -1
    // If we want to keep top-5 highest scores, remove rank 0 to -(5 + 1)
    const startIndex = keepDirection === 'asc' ? limit : 0;
    const endIndex = keepDirection === 'asc' ? -1 : -(limit + 1);

    const deferred = utils.defer();
    const cb = utils.createRedisCallback(deferred, callback);

    // If the counter does not have a time granularity, our job is easy.
    if (this.options.timeGranularity === timeGranularities.none) {
      this.metrics.client.zremrangebyrank(`${this.key}:z`, startIndex, endIndex, cb);
      return deferred.promise;
    }

    // Otherwise trim keys for the last five years.
    const currentTime = moment.utc().subtract(5, 'year')
      .startOf('year')
      .startOf('day');
    const end = moment.utc();
    const keySet = new Set();
    while (currentTime.isBefore(end)) {
      for (const key of this.getKeys(currentTime, timeGranularities.day)) {
        keySet.add(key);
      }

      // Mutates the time.
      currentTime.add(1, 'day');
    }

    // Each key is mapped to a function that returns a Promise.
    const mappedPromiseFunctions = Array.from(keySet).map(key => {
      return () => {
        const subDeferred = utils.defer();
        const subCallback = utils.createRedisCallback(subDeferred);
        this.metrics.client.zremrangebyrank(`${key}:z`, startIndex, endIndex, subCallback);
        return subDeferred.promise;
      };
    });

    // Each function is executed sequentially using reduce.
    mappedPromiseFunctions.reduce((promise, f) => {
      return promise.then(totalRemoved => f().then(removed => totalRemoved + removed));
    }, Promise.resolve(0))
    .then(totalRemoved => cb(null, totalRemoved))
    .catch(err => cb(err));

    return deferred.promise;
  }

  /**
   * Resets all counters to 0, going back 5 years in time.
   *
   * If an `eventObj` is passed, only the counters for that object are reset.
   *
   * @param  {String|Object}  [eventObj] Optional event object.
   * @param  {Function}       [callback] Optional callback.
   * @return {Promise}                   Resolves when done.
   */
  zero(eventObj, callback) {
    if (_.isFunction(eventObj)) {
      callback = eventObj;
      eventObj = null;
    }
    if (!utils.isNil(eventObj)) eventObj = String(eventObj);

    const keySet = new Set();
    const end = moment.utc();
    const addKeys = time => {
      for (const key of this.getKeys(time)) keySet.add(key);
    };

    // Get all keys with values for the 5-year range. We're limiting on the
    // expiration because 5 years of second granularity can easily blow up in
    // memory requirements.
    for (let granularity = 0; granularity <= this.options.timeGranularity; granularity++) {
      const start = this.options.expiration[granularity] > 0 ?
        moment.utc().subtract(this.options.expiration[granularity], 'seconds') :
        moment.utc().subtract(5, 'years'); // 5 years, like `trimEvents`.

      utils.momentRange(start, end, granularity)
        .forEach(addKeys);
    }

    const deferred = utils.defer();
    const cb = utils.createRedisCallback(deferred, callback);

    const mappedPromiseFunctions = Array.from(keySet).map(key => {
      return () => {
        const subDeferred = utils.defer();
        const subCallback = utils.createRedisCallback(subDeferred);
        zero(this.metrics.client, key, eventObj, subCallback);
        return subDeferred.promise;
      };
    });

    mappedPromiseFunctions.reduce((promise, f) => {
      return promise.then(() => f());
    }, Promise.resolve())
    .then(() => cb())
    .catch(err => cb(err));

    return deferred.promise;
  }
}

module.exports = TimestampedCounter;
