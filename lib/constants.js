'use strict';

/**
 * @module constants
 */

/**
 * Supported time granularities. Each granularity exist in different versions
 * to accomodate different function calls for the same granularity level. See
 * the example.
 *
 * @example
 * // All these are equivalent
 * myCounter.count('hour');
 * myCounter.count('h');
 * myCounter.count('4');
 * myCounter.count(4);
 *
 * @readonly
 * @enum {number}
 */
const timeGranularities = {
  // Long string form
  total: 0,
  none: 0,
  year: 1,
  month: 2,
  day: 3,
  hour: 4,
  minute: 5,
  second: 6,

  // Short string form

  /** Short form of "total" */
  T: 0,
  /** Short form of "none" */
  N: 0,
  /** Short form of "year" */
  Y: 1,
  /** Short form of "month" */
  M: 2,
  /** Short form of "day" */
  D: 3,
  /** Short form of "hour" */
  h: 4,
  /** Short form of "minute" */
  m: 5,
  /** Short form of "second" */
  s: 6,

  // Integer to integer form (a bit silly maybe), but it allows for both
  // timeGranularities[0] and timeGranularities['0'] which is not always such a
  // bad thing when different system components are speaking with each other
  // :-)

  /** Integer form of "none" */
  0: 0,
  /** Integer form of "year" */
  1: 1,
  /** Integer form of "month" */
  2: 2,
  /** Integer form of "day" */
  3: 3,
  /** Integer form of "hour" */
  4: 4,
  /** Integer form of "minute" */
  5: 5,
  /** Integer form of "second" */
  6: 6
};

const momentGranularities = [
  '', 'years', 'months', 'days', 'hours', 'minutes', 'seconds'
];

module.exports = {
  timeGranularities,
  momentGranularities
};
