'use strict';

/**
 * @module utils
 * @private
 */

const _ = require('lodash'),
  moment = require('moment'),
  constants = require('./constants');

const REDIS_MOMENT_FORMAT = 'YYYYMMDDHHmmss';

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
  if (!startDate) throw new TypeError('No startDate provided');

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

/**
 * Converts a moment object array (returned from `momentRange`) into its
 * corresponding redis keys.
 *
 * @param  {moment[]} momentRange     A range of moment objects.
 * @param  {string}   baseKey         The base redis key, used to generate the
 *                                    timestamped keys.
 * @param  {number}   timeGranularity The granularity we're generating keys for.
 * @return {string[]}                 The array of redis keys.
 */
const momentToKeyRange = (momentRange, baseKey, timeGranularity) => {
  return momentRange.map(m => {
    const mKeyFormat = m.format(REDIS_MOMENT_FORMAT).slice(0, (timeGranularity * 2) + 2);
    return `${baseKey}:${mKeyFormat}`;
  });
};

/**
 * Invoke a callback style function in a way such that it returns a promise
 * and can thus be used in a promise-chain.
 *
 * @param {Object} moduleObject - A module object or simple object.
 * @param {String} functionName - A function
 *
 * @example
 *
 * const promise = require('./lib/promise');
 * const redis = require('./lib/services/redis');
 * const { exec } = require('child_process')
 *
 * promise.ninvoke(redis, 'get', 'mykey').then(val => doSomething(val));
 * promise.ninvoke(null, exec, 'mkdir tmp');
 */
const ninvoke = (moduleObject, functionName, ...args) => {
  return new Promise((resolve, reject) => {
    // Create a callback as the last argument for the function call.
    args.push((err, res) => {
      if (err) return reject(err);
      resolve(res);
    });

    if (moduleObject === null) {
      return functionName(...args);
    }

    moduleObject[functionName](...args);
  });
};

module.exports = {
  REDIS_MOMENT_FORMAT,
  parseIntArray,
  momentRange,
  momentToKeyRange,
  ninvoke,
  parseInt: parseIntSingle
};
