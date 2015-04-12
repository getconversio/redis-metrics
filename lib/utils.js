'use strict';

/**
 * @module utils
 */

var _ = require('lodash'),
    moment = require('moment'),
    constants = require('./constants');

/**
 * Creates a general callback for a redis client function that both resolves
 * the given promise as well as executing the callback function.
 * @param {Promise} deferred - A Q deferred.
 * @param {function} [callback] - Optional callback to call.
 * @param {function} [resultParser] - Optional function for parsing the result.
 */
var createRedisCallback = function(deferred, callback, resultParser) {
  return function(err, result) {
    // (Maybe) parse the result.
    if (typeof resultParser === 'function') result = resultParser(result);

    // Handle the deferred.
    if (err) deferred.reject(err);
    else deferred.resolve(result);

    // (Maybe call the callback)
    if (typeof callback === 'function') callback(err, result);
  };
};

/**
 * A simple parser that always returns an integer, no matter what value is
 * thrown at it.
 * @param i - The integer to parse.
 * @returns {number} An integer. If the provided value cannot be parsed, the
 * function returns 0.
 */
var parseIntSingle = function(i) {
  i = parseInt(i, 10);
  return _.isFinite(i) ? i : 0;
};

/**
 * Map all values of the given array to integers.
 * @param arrayOfInts - The array of integers to parse.
 * @returns {array} An array of integers. If any value in the original array
 * cannot be parsed, the integer value will be 0 for this index.
 */
var parseIntArray = function(arrayOfInts) {
  return arrayOfInts.map(function(i) {
    return parseIntSingle(i);
  });
};

var momentRange = function(startDate, endDate, timeGranularity) {
  timeGranularity = constants.timeGranularities[timeGranularity];
  startDate = moment.utc(startDate);
  endDate = moment.utc(endDate).add(1, 'millisecond');

  var momentGranularity = constants.momentGranularities[timeGranularity];
  var timestamps = [];

  for (var m = startDate; m.isBefore(endDate); m.add(1, momentGranularity)) {
    timestamps.push(moment(m));
  }

  return timestamps;
};

module.exports = {
  createRedisCallback: createRedisCallback,
  parseInt: parseIntSingle,
  parseIntArray: parseIntArray,
  momentRange: momentRange
};
