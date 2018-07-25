'use strict';

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
    keys.push(`${baseKey}:${formattedTimestamp.slice(0, (i * 2) + 2)}`);
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
const createRangeParser = keyRange =>
  results => _.zipObject(keyRange, utils.parseIntArray(results));

/**
 * Creates a function that parses a list of Redis results and returns the total.
 * @returns {function}
 * @private
 */
const createRangeTotalParser = () =>
  results => _.sum(utils.parseIntArray(results));

const incrSingle = (client, key, amount, eventObj, ttl) => {
  if (eventObj) {
    key += ':z';

    if (ttl > 0) {
      return utils.ninvoke(client, 'eval', lua.zincrbyExpire, 1, key, amount, eventObj, ttl);
    }

    return utils.ninvoke(client, 'zincrby', key, amount, eventObj);
  }

  if (ttl > 0) { // No event object
    return utils.ninvoke(client, 'eval', lua.incrbyExpire, 1, key, amount, ttl);
  }

  return utils.ninvoke(client, 'incrby', key, amount);
};

const zero = (client, key, eventObj) => {
  if (eventObj) {
    key += ':z';
    return utils.ninvoke(client, 'zrem', key, eventObj);
  }

  return utils.ninvoke(client, 'del', key);
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

const createRankRangeParser = keyRange =>
  results => _.zipObject(keyRange, results.map(rankParser));

/**
 * Parse & merge multiple rank results from redis.
 * @param  {array}  ranks In this format: [['foo', '39', 'bar', '13'], ['foo', '11']]
 * @return {object}       In this format: [{ foo: 50 }, { bar: 13 }]
 */
const createRankTotalParser = (direction, startingAt, limit) => ranks => {
  return _.chain(ranks)
    .flatten()
    .chunk(2)
    .reduce((acc, [key, count]) => _.tap(acc, acc => {
      acc[key] = (acc[key] || 0) + Number(count);
    }), { })
    .toPairs()
    .sortBy(([, count]) => direction === 'asc' ? count : -count)
    .drop(startingAt)
    .takeWhile((value, index) => limit <= 0 || index < limit)
    .map(([key, count]) => ({ [key]: count }))
    .value();
};

/**
 * Parse a granularity and return it as a report granularity - that which was
 * requested for the final result - and as a query granularity - that which will
 * be used to generate the query timestamps.
 *
 * @param  {String} timeGranularity               The requested granularity for the result.
 * @param  {Number} defaultCounterTimeGranularity The counter's default granularity.
 * @return {Object}                 An object { reportTimeGranularity, rangeTimeGranularity }
 */
const parseRangeTimeGranularities = (timeGranularity, defaultCounterTimeGranularity) => {
  const reportTimeGranularity = parseTimeGranularity(timeGranularity);
  let rangeTimeGranularity = reportTimeGranularity;

  // If the range granularity is total, fall back to the granularity specified
  // at the counter level and then add the numbers together when parsing the
  // result.
  if (rangeTimeGranularity === timeGranularities.total) {
    rangeTimeGranularity = defaultCounterTimeGranularity;

    // If the rangeTimeGranularity is still total, it does not make sense to report
    // a range for the counter and we throw an error.
    if (rangeTimeGranularity === timeGranularities.total) {
      throw new Error('total granularity not supported for this counter');
    }
  }

  return { reportTimeGranularity, rangeTimeGranularity };
};

/**
 * Creates the range of keys to fetch from Redis as well as the keys to use in
 * the returned data object.
 *
 * Implementation notes:
 * `_.over` calls all functions in the array with each element in an array. This
 * means both key arrays are generated in one pass of the `momentRange`. Then,
 * `_.wrap` just converts the two-element array return into an object.
 *
 * @param  {moment[]} momentRange     An array of moment objects for mapping
 *                                    into redis keys and times.
 * @param  {String}   key             The base redis key.
 * @param  {Number}   timeGranularity The time granularity for the moments.
 * @return {Object}                   Both ranges in an object as `keyRange` and
 *                                    `momentKeyRange`.
 */
const keyAndMomentRange = _.wrap(
  _.over([
    utils.momentToKeyRange,
    momentRange => momentRange.map(m => m.format())
  ]),
  (func, ...args) => {
    const [keyRange, momentKeyRange] = func(...args);
    return { keyRange, momentKeyRange };
  }
);

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
  constructor(metrics, key, options = {}) {
    this.options = options;
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
      (time || moment.utc()).format(utils.REDIS_MOMENT_FORMAT),
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
   * @param   {Object}  [opts] @see `#incrby`
   * @returns {Promise}        A promise that resolves to the results from Redis.
   * @since 2.0.0
   */
  incr(opts = {}) {
    return this.incrby(1, opts);
  }

  /**
   * Increments this counter with the given amount.
   *
   * For some use cases, it makes sense to pass in an event object to get more
   * precise statistics for a specific event. For example, when counting page
   * views on a page, it makes sense to increment a counter per specific page.
   * For this use case, the eventObj parameter is a good fit.
   *
   * @param {number}        amount           The amount to increment with.
   * @param {Object}        [opts]           Optional config for this call.
   * @param {Object|string} [opts.eventObj]  Extra event information to
   *                                         determine which counter total
   *                                         increment.
   * @returns {Promise}                      A promise that resolves to the
   *                                         results from Redis.
   * @since 2.0.0
   */
  incrby(amount, { eventObj = null } = {}) {
    if (eventObj) eventObj = String(eventObj);

    const keys = this.getKeys();

    // Optimize for the case where there is only a single key to increment.
    if (keys.length === 1) {
      return incrSingle(
        this.metrics.client,
        keys[0],
        amount,
        eventObj,
        this.getKeyTTL(keys[0])
      );
    }

    const multi = this.metrics.client.multi();
    keys.forEach(key => {
      incrSingle(multi, key, amount, eventObj, this.getKeyTTL(key));
    });
    return utils.ninvoke(multi, 'exec');
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
   * myCounter.count().then(result => {
   *   console.log(result); // Outputs the global count
   * });
   * @example
   * myCounter.count({ granularity: 'year' }).then(result => {
   *   console.log(result); // Outputs the count for the current year
   * });
   * @example
   * myCounter.count({ granularity: 'year', eventObj: '/foo.html' }).then(result => {
   *   // Outputs the count for the current year for the event object '/foo.html'
   *   console.log(result);
   * });
   *
   * @param {module:constants~timeGranularities} [timeGranularity='total'] - The
   *   granularity level to report the count for.
   * @param {string|object} [eventObj] - The event object. See
   *   {@link TimestampedCounter#incr} for more info on event objects.
   * @returns {Promise} A promise that resolves to the result from Redis
   * @since 0.1.0
   */
  count({ timeGranularity = 'total', eventObj = null } = {}) {
    timeGranularity = parseTimeGranularity(timeGranularity);

    const key = this.getKeys()[timeGranularity];
    let resultPromise;

    if (eventObj) {
      resultPromise = utils.ninvoke(this.metrics.client, 'zscore', `${key}:z`, eventObj);
    } else {
      resultPromise = utils.ninvoke(this.metrics.client, 'get', key);
    }

    return resultPromise.then(utils.parseInt);
  }

  /**
   * Returns an object mapping timestamps to counts in the given time range at a
   * specific time granularity level.
   *
   * Notice: This function does not make sense for the "none"  or "total" time
   * granularities, and will throw an error accordingly.
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
   * @returns {Promise} A promise that resolves to the result from Redis.
   * @since 0.1.0
   */
  countRange(timeGranularity, { startDate, endDate = moment.utc() }, { eventObj = null } = {}) {
    if (eventObj) eventObj = String(eventObj);

    let reportTimeGranularity;
    let rangeTimeGranularity;
    let momentRange;

    try {
      ({
        reportTimeGranularity,
        rangeTimeGranularity
      } = parseRangeTimeGranularities(timeGranularity, this.options.timeGranularity));

      momentRange = utils.momentRange(startDate, endDate, rangeTimeGranularity);
    } catch (err) {
      return Promise.reject(err);
    }

    const { keyRange, momentKeyRange } = keyAndMomentRange(
      momentRange,
      this.key,
      rangeTimeGranularity
    );

    const parser = reportTimeGranularity === timeGranularities.total
      ? createRangeTotalParser()
      : createRangeParser(momentKeyRange);

    let resultPromise;

    if (eventObj) {
      const multi = this.metrics.client.multi();
      keyRange.forEach(key => multi.zscore(key + ':z', eventObj));
      resultPromise = utils.ninvoke(multi, 'exec');
    } else {
      resultPromise = utils.ninvoke(this.metrics.client, 'mget', keyRange);
    }

    return resultPromise.then(parser);
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
   * myCounter.top().then(result => {
   *   console.log(result); // Outputs the global rank
   * });
   * @example
   * myCounter.top({ timeGranularity: 'year' }).then(result => {
   *   console.log(result); // Outputs the rank for the current year
   * });
   *
   * @param {module:constants~timeGranularities} [timeGranularity='total'] - The
   *   granularity level to report the rank for.
   * @param {string} [direction=desc] - Optional sort direction, can be "asc" or "desc"
   * @param {integer} [startingAt=0] - Optional starting row.
   * @param {integer} [limit=-1] - Optional number of results to return.
   * @returns {Promise} A promise that resolves to the result from Redis.
   * @since 2.0.0
   */
  top({ timeGranularity = 'total', direction = 'desc', startingAt = 0, limit = -1 } = {}) {
    if (['asc', 'desc'].indexOf(direction) === -1) {
      return Promise.reject(new Error(
        'The direction option is expected to be one of ' +
        '"asc" or "desc", got "' + direction + '".'
      ));
    }

    timeGranularity = parseTimeGranularity(timeGranularity);

    const key = this.getKeys()[timeGranularity];
    const redisFn = direction === 'asc' ? 'zrange' : 'zrevrange';

    return utils.ninvoke(
      this.metrics.client,
      redisFn,
      key + ':z',
      startingAt,
      limit,
      'WITHSCORES'
    )
    .then(rankParser);
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
   * @returns {Promise} A promise that resolves to the result from Redis.
   * @since 2.0.0
   */
  topRange(
    { startDate, endDate = moment.utc() },
    {
      timeGranularity = 'total',
      direction = 'desc',
      startingAt = 0,
      limit = -1
    } = {}
  ) {
    if (['asc', 'desc'].indexOf(direction) === -1) {
      return Promise.reject(new Error(
        'The direction option is expected to be one between ' +
        '"asc" or "desc", got "' + direction + '".'
      ));
    }

    let reportTimeGranularity;
    let rangeTimeGranularity;
    let momentRange;

    try {
      ({
        reportTimeGranularity,
        rangeTimeGranularity
      } = parseRangeTimeGranularities(timeGranularity, this.options.timeGranularity));

      momentRange = utils.momentRange(startDate, endDate, rangeTimeGranularity);
    } catch (err) {
      return Promise.reject(err);
    }

    const { keyRange, momentKeyRange } = keyAndMomentRange(
      momentRange,
      this.key,
      rangeTimeGranularity
    );

    const multi = this.metrics.client.multi();
    let parser;

    if (reportTimeGranularity === timeGranularities.total) {
      parser = createRankTotalParser(direction, startingAt, limit);

      // we need to get every value so that we can re-aggregate in the parser
      keyRange.forEach(key => multi.zrange(key + ':z', 0, -1, 'WITHSCORES'));
    } else {
      parser = createRankRangeParser(momentKeyRange);

      const redisFn = direction === 'asc' ? 'zrange' : 'zrevrange';

      keyRange.forEach(key => multi[redisFn](
        key + ':z',
        startingAt,
        limit,
        'WITHSCORES'
      ));
    }

    return utils.ninvoke(multi, 'exec')
      .then(parser);
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
   * @param {string} [direction=desc] - Sort direction for the top-N
   * elements to keep, can be "asc" or "desc".
   * @param {integer} [limit=1000] - Number of results to keep.
   * @returns {Promise} Resolves when the values have been removed.
   * @example
   * // Keeps the top-5 elements with highest score
   * myCounter.trimEvents('desc', 5)
   * @example
   * // Keeps the top-5 elements with lowest score
   * myCounter.trimEvents('asc', 5)
   * @since 2.0.0
   */
  trimEvents({ direction = 'desc', limit = 1000 } = {}) {
    if (['asc', 'desc'].indexOf(direction) === -1) {
      return Promise.reject(new Error(
        'The direction parameter is expected to be one between ' +
        '"asc" or "desc", got "' + direction + '".'
      ));
    }

    // If we want to keep top-5 lowest scores, remove rank 5 to -1
    // If we want to keep top-5 highest scores, remove rank 0 to -(5 + 1)
    const startIndex = direction === 'asc' ? limit : 0;
    const endIndex = direction === 'asc' ? -1 : -(limit + 1);

    // If the counter does not have a time granularity, our job is easy.
    if (this.options.timeGranularity === timeGranularities.none) {
      return utils.ninvoke(
        this.metrics.client, 'zremrangebyrank', `${this.key}:z`, startIndex, endIndex
      );
    }

    // Otherwise trim keys for the last five years.
    const currentTime = moment.utc().subtract(5, 'year')
      .startOf('year')
      .startOf('day');
    const end = moment.utc();
    const keySet = new Set();
    while (currentTime.isBefore(end)) {
      this.getKeys(currentTime, timeGranularities.day)
        .forEach(key => keySet.add(key));

      // Mutates the time.
      currentTime.add(1, 'day');
    }

    // Each key is mapped to a function that returns a Promise.
    const mappedPromiseFunctions = Array.from(keySet).map(key => {
      return () => utils.ninvoke(
        this.metrics.client,
        'zremrangebyrank',
        `${key}:z`,
        startIndex,
        endIndex
      );
    });

    // Each function is executed sequentially using reduce.
    return mappedPromiseFunctions.reduce((promise, f) => {
      return promise.then(totalRemoved => f().then(removed => totalRemoved + removed));
    }, Promise.resolve(0));
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
  zero({ eventObj = null } = {}) {
    if (eventObj) eventObj = String(eventObj);

    const keySet = new Set();
    const end = moment.utc();
    const addKeys = time => this.getKeys(time).forEach(key => keySet.add(key));

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

    const mappedPromiseFunctions = Array.from(keySet).map(key => {
      return () => zero(this.metrics.client, key, eventObj);
    });

    return mappedPromiseFunctions.reduce((promise, f) => {
      return promise.then(() => f());
    }, Promise.resolve());
  }
}

module.exports = TimestampedCounter;
