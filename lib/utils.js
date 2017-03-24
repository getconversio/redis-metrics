'use strict';

/**
 * @module utils
 * @private
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
  // -- problem with meteor and moment, invalid moment objects
  startDate = moment.utc(startDate.toISOString());
  endDate = moment.utc(endDate.toISOString());

  // To avoid edge case issues when adding timestamp information, the time
  // information below the current granularity level is reset.
  startDate.millisecond(0);
  endDate.millisecond(0);
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

  var momentGranularity = constants.momentGranularities[timeGranularity];
  var timestamps = [];

  endDate.add(1, 'millisecond');
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
