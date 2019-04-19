'use strict';

/**
 * @module utils
 * @private
 */

const _ = require('lodash'),
  moment = require('moment'),
  constants = require('./constants');

/**
 * Creates a general callback for a redis client function that both resolves
 * the given promise as well as executing the callback function.
 * @param {Promise} deferred - A deferred promise object.
 * @param {function} [callback] - Optional callback to call.
 * @param {function} [resultParser] - Optional function for parsing the result.
 */
const createRedisCallback = (deferred, callback, resultParser) => (err, result) => {
  // (Maybe) parse the result.
  if (typeof resultParser === 'function') result = resultParser(result);

  // Handle the deferred.
  if (err) deferred.reject(err);
  else deferred.resolve(result);

  // (Maybe call the callback)
  if (typeof callback === 'function') callback(err, result);
};

/**
 * A simple parser that always returns an integer, no matter what value is
 * thrown at it.
 * @param i - The integer to parse.
 * @returns {number} An integer. If the provided value cannot be parsed, the
 * function returns 0.
 */
const parseIntSingle = i => {
  i = parseInt(i, 10);
  return _.isFinite(i) ? i : 0;
};

/**
 * Map all values of the given array to integers.
 * @param arrayOfInts - The array of integers to parse.
 * @returns {array} An array of integers. If any value in the original array
 * cannot be parsed, the integer value will be 0 for this index.
 */
const parseIntArray = arrayOfInts => arrayOfInts.map(i => parseIntSingle(i));

const momentRange = (startDate, endDate, timeGranularity) => {
  timeGranularity = constants.timeGranularities[timeGranularity];
  startDate = moment.utc(startDate);
  endDate = moment.utc(endDate);

  // To avoid edge case issues when adding timestamp information, the time
  // information below the current granularity level is reset.
  startDate.millisecond(0);
  endDate.millisecond(0);

  // It presumably doesn't matter what we return here as any date will represent
  // the only existing counter.
  if (timeGranularity === constants.timeGranularities.none) return [endDate];

  if (timeGranularity < constants.timeGranularities.second) {
    startDate.second(0);
    endDate.second(0);
  }
  if (timeGranularity < constants.timeGranularities.minute) {
    startDate.minute(0);
    endDate.minute(0);
  }
  if (timeGranularity < constants.timeGranularities.hour) {
    startDate.hour(0);
    endDate.hour(0);
  }
  if (timeGranularity < constants.timeGranularities.day) {
    startDate.date(1);
    endDate.date(1);
  }
  if (timeGranularity < constants.timeGranularities.month) {
    startDate.month(0);
    endDate.month(0);
  }

  const momentGranularity = constants.momentGranularities[timeGranularity];
  const timestamps = [];

  endDate.add(1, 'millisecond');
  for (let m = startDate; m.isBefore(endDate); m.add(1, momentGranularity)) {
    timestamps.push(moment(m));
  }

  return timestamps;
};

const defer = () => {
  const deferred = {};
  deferred.promise = new Promise((resolve, reject) => {
    deferred.resolve = resolve;
    deferred.reject = reject;
  });
  return deferred;
};

const isNil = val => val === null || val === undefined;

module.exports = {
  createRedisCallback,
  parseIntArray,
  momentRange,
  defer,
  isNil,
  parseInt: parseIntSingle
};
