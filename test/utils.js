'use strict';

const ensureError = (watchedFn, matcherFn) => {
  return new Promise((res, rej) => {
    watchedFn()
      .then(() => rej(new Error('should have rejected!')))
      .catch(err => {
        if (matcherFn) matcherFn(err);
        res();
      })
      .catch(rej);
  });
};

module.exports = { ensureError };
