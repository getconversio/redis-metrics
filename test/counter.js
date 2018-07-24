'use strict';

const { expect } = require('chai'),
  sinon = require('sinon'),
  moment = require('moment'),
  IORedis = require('ioredis'),
  RedisMetrics = require('../lib/metrics'),
  TimestampedCounter = require('../lib/counter'),
  constants = require('../lib/constants'),
  utils = require('../lib/utils');

describe('Counter', () => {
  let metrics;
  let sandbox;

  beforeEach(done => {
    sandbox = sinon.createSandbox();
    const host = process.env.REDIS_HOST || 'localhost';
    const port = process.env.REDIS_PORT || 6379;
    if (process.env.USE_IOREDIS) {
      const client = new IORedis(port, host);
      metrics = new RedisMetrics({ client });
    } else {
      metrics = new RedisMetrics({ host, port });
    }
    metrics.client.flushall(done);
  });

  afterEach(done => {
    sandbox.restore();
    metrics.client.quit(done);
  });

  describe('constructor', () => {
    it('should set some defaults', () => {
      const counter = new TimestampedCounter(metrics, 'foo');
      expect(counter.metrics).to.equal(metrics);
      expect(counter.key).to.equal('c:foo');
      expect(counter.options.timeGranularity).to.equal(0);
      expect(counter.options.expireKeys).to.equal(true);
      expect(counter.options.namespace).to.equal('c');
    });

    it('should respect the passed namespace in the key', () => {
      const counter = new TimestampedCounter(metrics, 'foo', {
        namespace: 'stats'
      });
      expect(counter.key).to.equal('stats:foo');
      expect(counter.options.namespace).to.equal('stats');
    });

    it('should reset an incorrect time granularity to "none"', () => {
      let counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: -10
      });
      expect(counter.options.timeGranularity).to.equal(0);

      counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 10
      });
      expect(counter.options.timeGranularity).to.equal(0);
    });
  });

  describe('getKeys', () => {
    it('should return an array of keys', () => {
      const counter = new TimestampedCounter(metrics, 'foo');
      const keys = counter.getKeys();
      expect(keys).to.be.instanceof(Array);
      expect(keys).to.have.length.above(0);
      expect(keys).to.include('c:foo');
    });

    it('should return keys based on the granularity level', () => {
      sandbox.useFakeTimers(new Date('2015-01-02T03:04:05Z').getTime());

      let counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'none'
      });
      let keys = counter.getKeys();
      expect(keys).to.be.instanceof(Array);
      expect(keys).to.have.length(1);
      expect(keys).to.include('c:foo');

      counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'year'
      });
      keys = counter.getKeys();
      expect(keys).to.be.instanceof(Array);
      expect(keys).to.have.length(2);
      expect(keys).to.include('c:foo');
      expect(keys).to.include('c:foo:2015');

      counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'month'
      });
      keys = counter.getKeys();
      expect(keys).to.be.instanceof(Array);
      expect(keys).to.have.length(3);
      expect(keys).to.include('c:foo');
      expect(keys).to.include('c:foo:2015');
      expect(keys).to.include('c:foo:201501');

      counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'day'
      });
      keys = counter.getKeys();
      expect(keys).to.be.instanceof(Array);
      expect(keys).to.have.length(4);
      expect(keys).to.include('c:foo');
      expect(keys).to.include('c:foo:2015');
      expect(keys).to.include('c:foo:201501');
      expect(keys).to.include('c:foo:20150102');

      counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'hour'
      });
      keys = counter.getKeys();
      expect(keys).to.be.instanceof(Array);
      expect(keys).to.have.length(5);
      expect(keys).to.include('c:foo');
      expect(keys).to.include('c:foo:2015');
      expect(keys).to.include('c:foo:201501');
      expect(keys).to.include('c:foo:2015010203');

      counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'minute'
      });
      keys = counter.getKeys();
      expect(keys).to.be.instanceof(Array);
      expect(keys).to.have.length(6);
      expect(keys).to.include('c:foo');
      expect(keys).to.include('c:foo:2015');
      expect(keys).to.include('c:foo:201501');
      expect(keys).to.include('c:foo:2015010203');
      expect(keys).to.include('c:foo:201501020304');

      counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'second'
      });
      keys = counter.getKeys();
      expect(keys).to.be.instanceof(Array);
      expect(keys).to.have.length(7);
      expect(keys).to.include('c:foo');
      expect(keys).to.include('c:foo:2015');
      expect(keys).to.include('c:foo:201501');
      expect(keys).to.include('c:foo:2015010203');
      expect(keys).to.include('c:foo:201501020304');
      expect(keys).to.include('c:foo:20150102030405');
    });

    it('should support a custom time', () => {
      const counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'second'
      });
      const keys = counter.getKeys(moment.utc('2017-12-13T14:15:16Z'));
      expect(keys).to.be.instanceof(Array);
      expect(keys).to.have.length(7);
      expect(keys).to.include('c:foo');
      expect(keys).to.include('c:foo:2017');
      expect(keys).to.include('c:foo:201712');
      expect(keys).to.include('c:foo:2017121314');
      expect(keys).to.include('c:foo:201712131415');
      expect(keys).to.include('c:foo:20171213141516');
    });

    it('should support a custom time and granularity', () => {
      const counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'second'
      });
      const keys = counter.getKeys(moment.utc('2017-12-13T14:15:16Z'), 'month');
      // Note the counter granularity is set to second, but we only want months.
      expect(keys).to.be.instanceof(Array);
      expect(keys).to.have.length(3);
      expect(keys).to.include('c:foo');
      expect(keys).to.include('c:foo:2017');
      expect(keys).to.include('c:foo:201712');
    });
  });

  describe('getKeyTTL', () => {
    it('should the default for seconds', () => {
      const counter = new TimestampedCounter(metrics, 'foo');
      const key = 'c:foo:20150102030405';
      const ttl = counter.getKeyTTL(key);
      expect(ttl).to.equal(10 * 60);
    });

    it('should the default for minutes', () => {
      const counter = new TimestampedCounter(metrics, 'foo');
      const key = 'c:foo:201501020304';
      const ttl = counter.getKeyTTL(key);
      expect(ttl).to.equal(12 * 60 * 60);
    });

    it('should the default for hours', () => {
      const counter = new TimestampedCounter(metrics, 'foo');
      const key = 'c:foo:2015010203';
      const ttl = counter.getKeyTTL(key);
      expect(ttl).to.equal(31 * 24 * 60 * 60);
    });

    it('should the default for days', () => {
      const counter = new TimestampedCounter(metrics, 'foo');
      const key = 'c:foo:20150102';
      const ttl = counter.getKeyTTL(key);
      expect(ttl).to.equal(2 * 365 * 24 * 60 * 60);
    });

    it('should the default for months', () => {
      const counter = new TimestampedCounter(metrics, 'foo');
      const key = 'c:foo:201501';
      const ttl = counter.getKeyTTL(key);
      expect(ttl).to.equal(10 * 365 * 24 * 60 * 60);
    });

    it('should the default for years', () => {
      const counter = new TimestampedCounter(metrics, 'foo');
      const key = 'c:foo:2015';
      const ttl = counter.getKeyTTL(key);
      expect(ttl).to.equal(-1);
    });

    it('should the default for none', () => {
      const counter = new TimestampedCounter(metrics, 'foo');
      const key = 'c:foo';
      const ttl = counter.getKeyTTL(key);
      expect(ttl).to.equal(-1);
    });

    it('should allow the expiration to be configured', () => {
      let counter = new TimestampedCounter(metrics, 'foo', {
        expiration: {
          total: 1
        }
      });
      const key = 'c:foo';
      let ttl = counter.getKeyTTL(key);
      expect(ttl).to.equal(1);

      counter = new TimestampedCounter(metrics, 'foo', {
        expiration: {
          0: 2
        }
      });

      ttl = counter.getKeyTTL(key);
      expect(ttl).to.equal(2);

      counter = new TimestampedCounter(metrics, 'foo', {
        expiration: {
          total: 3
        }
      });

      ttl = counter.getKeyTTL(key);
      expect(ttl).to.equal(3);

      counter = new TimestampedCounter(metrics, 'foo', {
        expiration: {
          T: 4
        }
      });

      ttl = counter.getKeyTTL(key);
      expect(ttl).to.equal(4);
    });

    it('should return the default value if conf is missing', () => {
      const counter = new TimestampedCounter(metrics, 'foo', {
        expiration: {
          total: 10
        }
      });
      // Only total is set, yearly is not, use default of -1
      const key = 'c:foo:2015';
      const ttl = counter.getKeyTTL(key);
      expect(ttl).to.equal(-1);
    });
  });

  describe('incr', () => {
    it('should call redis without a trans when keys dont expire', () => {
      const multiSpy = sandbox.spy(metrics.client, 'multi');
      const incrSpy = sandbox.spy(metrics.client, 'incrby');

      const counter = new TimestampedCounter(
        metrics,
        'foo',
        { expireKeys: false }
      );
      return counter.incr().then(() => {
        sinon.assert.calledOnce(incrSpy);
        sinon.assert.notCalled(multiSpy);
      });
    });

    it('should call redis without a trans when counters expire', () => {
      const multiSpy = sandbox.spy(metrics.client, 'multi');
      const evalSpy = sandbox.spy(metrics.client, 'eval');

      const counter = new TimestampedCounter(metrics, 'foo', {
        expireKeys: true,
        expiration: {
          total: 10
        }
      });
      return counter.incr().then(() => {
        sinon.assert.calledOnce(evalSpy);
        sinon.assert.notCalled(multiSpy);
      });
    });

    it('should call redis when a time granularity is chosen', () => {
      const multiSpy = sandbox.spy(metrics.client, 'multi');

      const counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'year'
      });

      return counter.incr().then(() => {
        sinon.assert.calledOnce(multiSpy);
      });
    });

    it('should call the callback on success', done => {
      const counter = new TimestampedCounter(metrics, 'foo');
      counter.incr((err, result) => {
        expect(err).to.equal(null);
        expect(result).to.equal(1);
        done();
      });
    });

    it('should resolve a promise on success', done => {
      const counter = new TimestampedCounter(metrics, 'foo');
      counter.incr().then(result => {
        expect(result).to.equal(1);
        done();
      })
      .catch(done);
    });

    it('should call the callback on error', done => {
      sandbox.stub(metrics.client, 'incrby')
        .yields(new Error('oh no'), null);

      const counter = new TimestampedCounter(
        metrics,
        'foo',
        { expireKeys: false }
      );
      counter.incr((err, result) => {
        expect(err).to.not.equal(null);
        expect(result).to.equal(null);
        done();
      });
    });

    it('should reject the promise on error', done => {
      sandbox.stub(metrics.client, 'incrby')
        .yields(new Error('oh no'), null);

      const counter = new TimestampedCounter(
        metrics,
        'foo',
        { expireKeys: false }
      );
      counter.incr().then(() => {
        done(new Error('Should not be here'));
      })
      .catch(err => {
        expect(err).to.not.equal(null);
        done();
      });
    });

    it('should return a list of results', done => {
      const counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'year'
      });

      counter.incr().then(results => {
        expect(results).to.be.instanceof(Array);
        expect(results).to.deep.equal([1, 1]);
        done();
      })
      .catch(done);
    });

    it('should return a list of results for non-expiring keys', done => {
      const counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'year',
        expireKeys: false
      });

      counter.incr().then(results => {
        expect(results).to.be.instanceof(Array);
        expect(results).to.deep.equal([1, 1]);
        done();
      })
      .catch(done);
    });
  });

  describe('incr with event object', () => {
    it('should work with an event object when a counter expires', () => {
      const counter = new TimestampedCounter(metrics, 'foo', {
        expireKeys: true,
        expiration: {
          total: 10
        }
      });

      return counter.incr('bar').then(result => {
        expect(Number(result)).to.equal(1);
      });
    });

    it('should work with an event object and non-expiring keys', () => {
      const counter = new TimestampedCounter(
        metrics,
        'foo',
        { expireKeys: false }
      );

      return counter.incr('bar').then(result => {
        expect(Number(result)).to.equal(1);
      });
    });

    it('should work with an event, time gran, a counter that exp', () => {
      const counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'year',
        expireKeys: true,
        expiration: {
          year: 10
        }
      });

      return counter.incr('bar').then(results => {
        expect(utils.parseIntArray(results)).to.deep.equal([1, 1]);
      });
    });

    it('should work with an event, time gran and non-exp keys', () => {
      const counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'year',
        expireKeys: false
      });

      return counter.incr('bar').then(results => {
        expect(utils.parseIntArray(results)).to.deep.equal([1, 1]);
      });
    });
  });

  describe('incrby', () => {
    it('should call redis without a tran when keys do not exp', done => {
      const multiSpy = sandbox.spy(metrics.client, 'multi');
      const incrSpy = sandbox.spy(metrics.client, 'incrby');

      const counter = new TimestampedCounter(
        metrics,
        'foo',
        { expireKeys: false }
      );
      counter.incrby(2).then(() => {
        sinon.assert.calledOnce(incrSpy);
        sinon.assert.notCalled(multiSpy);
        done();
      })
      .catch(done);
    });

    it('should call redis without a trans when counters exp', done => {
      const multiSpy = sandbox.spy(metrics.client, 'multi');
      const evalSpy = sandbox.spy(metrics.client, 'eval');

      const counter = new TimestampedCounter(metrics, 'foo', {
        expireKeys: true,
        expiration: {
          total: 10
        }
      });
      counter.incrby(2).then(() => {
        sinon.assert.calledOnce(evalSpy);
        sinon.assert.notCalled(multiSpy);
        done();
      })
      .catch(done);
    });

    it('should call redis when a time granularity is chosen', done => {
      const multiSpy = sandbox.spy(metrics.client, 'multi');

      const counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'year'
      });

      counter.incrby(3).then(() => {
        sinon.assert.calledOnce(multiSpy);
        done();
      })
      .catch(done);
    });

    it('should call the callback on success', done => {
      const counter = new TimestampedCounter(metrics, 'foo');
      counter.incrby(4, (err, result) => {
        expect(err).to.equal(null);
        expect(result).to.equal(4);
        done();
      });
    });

    it('should resolve a promise on success', done => {
      const counter = new TimestampedCounter(metrics, 'foo');
      counter.incrby(5).then(result => {
        expect(result).to.equal(5);
        done();
      })
      .catch(done);
    });

    it('should call the callback on error from incrby', done => {
      sandbox.stub(metrics.client, 'incrby')
        .yields(new Error('oh no'), null);

      const counter = new TimestampedCounter(
        metrics,
        'foo',
        { expireKeys: false }
      );
      counter.incrby(6, (err, result) => {
        expect(err).to.not.equal(null);
        expect(result).to.equal(null);
        done();
      });
    });

    it('should call the callback on error from eval', done => {
      sandbox.stub(metrics.client, 'eval')
        .yields(new Error('oh no'), null);

      const counter = new TimestampedCounter(metrics, 'foo', {
        expireKeys: true,
        expiration: {
          total: 10
        }
      });
      counter.incrby(6, (err, result) => {
        expect(err).to.not.equal(null);
        expect(result).to.equal(null);
        done();
      });
    });

    it('should reject the promise on redis error from incrby', done => {
      sandbox.stub(metrics.client, 'incrby')
        .yields(new Error('oh no'), null);

      const counter = new TimestampedCounter(
        metrics,
        'foo',
        { expireKeys: false }
      );
      counter.incrby(7).then(() => {
        done(new Error('Should not be here'));
      })
      .catch(err => {
        expect(err).to.not.equal(null);
        done();
      });
    });

    it('should reject the promise on redis error with eval', done => {
      sandbox.stub(metrics.client, 'eval')
        .yields(new Error('oh no'), null);

      const counter = new TimestampedCounter(metrics, 'foo', {
        expireKeys: true,
        expiration: {
          total: 10
        }
      });
      counter.incrby(7).then(() => {
        done(new Error('Should not be here'));
      })
      .catch(err => {
        expect(err).to.not.equal(null);
        done();
      });
    });

    it('should return a list of results from the operation', done => {
      const counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'year'
      });

      counter.incrby(8).then(results => {
        expect(results).to.be.instanceof(Array);
        expect(results).to.deep.equal([8, 8]);
        done();
      })
      .catch(done);
    });
  });

  describe('incrby with event object', () => {
    it('should work with an event object', done => {
      const counter = new TimestampedCounter(metrics, 'foo');

      counter.incrby(9, 'bar').then(result => {
        expect(Number(result)).to.equal(9);
        done();
      })
      .catch(done);
    });

    it('should work with an event object and time granularity', done => {
      const counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'year'
      });

      counter.incrby(10, 'bar').then(results => {
        expect(utils.parseIntArray(results)).to.deep.equal([10, 10]);
        done();
      })
      .catch(done);
    });
  });

  describe('count', () => {
    it('should work with callback', done => {
      const mock = sandbox.mock(metrics.client)
        .expects('get')
        .once()
        .yields(null, '10');

      const counter = new TimestampedCounter(metrics, 'foo');
      counter.count((err, result) => {
        mock.verify();
        expect(result).to.equal(10);
        done(err);
      });
    });

    it('should work with time granularity and callback', done => {
      const mock = sandbox.mock(metrics.client)
        .expects('get')
        .once()
        .yields(null, '10');

      const counter = new TimestampedCounter(metrics, 'foo');
      counter.count('none', (err, result) => {
        mock.verify();
        expect(result).to.equal(10);
        done(err);
      });
    });

    it('should work with time gran, event object and callback', done => {
      const mock = sandbox.mock(metrics.client)
        .expects('zscore')
        .once()
        .yields(null, '10');

      const counter = new TimestampedCounter(metrics, 'foo');
      counter.count('none', 'bar', (err, result) => {
        mock.verify();
        expect(result).to.equal(10);
        done(err);
      });
    });

    it('should return a single res when no arg are given', () => {
      const counter = new TimestampedCounter(metrics, 'foo');
      return counter.incr()
        .then(() => counter.incr())
        .then(() => counter.incr())
        .then(() => counter.count())
        .then(result => {
          // Counter has been incremented 3 times.
          expect(result).to.equal(3);
        });
    });

    it('should return a single result for an event object', () => {
      const counter = new TimestampedCounter(metrics, 'foo');
      return counter.incr('bar')
        .then(() => counter.incr('bar'))
        .then(() => counter.incr('bar'))
        .then(() => counter.count('total', 'bar'))
        .then(result => {
          // Counter has been incremented 3 times.
          expect(result).to.equal(3);
        });
    });

    it('should return 0 when the key does not exist (cb)', done => {
      const mock = sandbox.mock(metrics.client)
        .expects('get')
        .once()
        .yields(null, null);

      const counter = new TimestampedCounter(metrics, 'foo');
      counter.count((err, result) => {
        mock.verify();
        expect(result).to.equal(0);
        done(err);
      });
    });

    it('should return 0 when the key does not exist (promise)', done => {
      const mock = sandbox.mock(metrics.client)
        .expects('get')
        .once()
        .yields(null, null);

      const counter = new TimestampedCounter(metrics, 'foo');
      counter.count()
        .then(result => {
          mock.verify();
          expect(result).to.equal(0);
          done();
        })
        .catch(done);
    });

    it('should return a count for a specific time granularity', () => {
      const counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'year'
      });

      // Increment in 2014 and 2015
      // Total should be 2 but year should be 1.

      const clock = sandbox.useFakeTimers(new Date('2014-02-01').getTime());
      return counter.incr()
        .then(() => {
          clock.tick(1000 * 60 * 60 * 24 * 365);
          return counter.incr();
        })
        .then(() => counter.count('none')) // same as counter.count();
        .then(result => {
          expect(result).to.equal(2);
          return counter.count('year');
        })
        .then(result => expect(result).to.equal(1));
    });

    it('should return a count for a specific time gran and event', () => {
      const counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'year'
      });

      // Increment in 2014 and 2015
      // Total should be 2 but year should be 1.

      const clock = sandbox.useFakeTimers(new Date('2014-02-01').getTime());
      return counter.incr('bar')
        .then(() => {
          clock.tick(1000 * 60 * 60 * 24 * 365);
          return counter.incr('bar');
        })
        .then(() => counter.count('none', 'bar'))
        .then(result => {
          expect(result).to.equal(2);
          return counter.count('year', 'bar');
        })
        .then(result => expect(result).to.equal(1));
    });
  });

  describe('countRange', () => {
    it('should return a range of counts', done => {
      const counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'year'
      });

      // Increment 2014 once and 2015 twice

      const start = moment.utc({ year: 2014 });
      const end = moment.utc({ year: 2015 });
      const expected = {};
      expected[start.format()] = 1;
      expected[end.format()] = 2;

      const clock = sandbox.useFakeTimers(new Date('2014-02-01').getTime());
      counter.incr()
        .then(() => {
          clock.tick(1000 * 60 * 60 * 24 * 365);
          return counter.incr();
        })
        .then(() => counter.incr())
        .then(() => counter.countRange('year', start, end))
        .then(result => {
          // Check promise
          expect(result).to.deep.equal(expected);

          // Check callback
          counter.countRange('year', start, end, (err, res) => {
            expect(res).to.deep.equal(expected);
            done();
          });
        })
        .catch(done);
    });

    it('should return a range of counts at the second level', done => {
      const counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'second'
      });

      const start = moment.utc({ year: 2015, second: 0 });
      const end = moment.utc({ year: 2015, second: 1 });
      const expected = {};
      expected[start.format()] = 1;
      expected[end.format()] = 2;

      const clock = sandbox.useFakeTimers(new Date('2015-01-01').getTime());
      counter.incr()
        .then(() => {
          clock.tick(1000);
          return counter.incr();
        })
        .then(() => counter.incr())
        .then(() => counter.countRange('second', start, end))
        .then(result => {
          // Check promise
          expect(result).to.deep.equal(expected);

          // Check callback
          counter.countRange('second', start, end, (err, res) => {
            expect(res).to.deep.equal(expected);
            done();
          });
        })
        .catch(done);
    });

    it('should return a range of count for an event object', done => {
      const counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'year'
      });

      // Increment 2014 once and 2015 twice

      const start = moment.utc({ year: 2014 });
      const end = moment.utc({ year: 2015 });
      const expected = {};
      expected[start.format()] = 1;
      expected[end.format()] = 2;

      const clock = sandbox.useFakeTimers(new Date('2014-02-01').getTime());
      counter.incr('bar')
        .then(() => {
          clock.tick(1000 * 60 * 60 * 24 * 365);
          return counter.incr('bar');
        })
        .then(() => counter.incr('bar'))
        .then(() => counter.countRange('year', start, end, 'bar'))
        .then(result => {
          // Check promise
          expect(result).to.deep.equal(expected);

          // Check callback
          counter.countRange('year', start, end, 'bar', (err, res) => {
            expect(res).to.deep.equal(expected);
            done();
          });
        })
        .catch(done);
    });

    it('should use current date if no end date is provided', done => {
      const counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'year'
      });

      // Increment 2014 once and 2015 twice

      const start = moment.utc({ year: 2014 });
      const expected = {};
      expected[start.format()] = 1;
      expected[moment.utc(start).add(1, 'years').format()] = 2;

      const clock = sandbox.useFakeTimers(new Date('2014-02-01').getTime());
      counter.incr()
        .then(() => {
          clock.tick(1000 * 60 * 60 * 24 * 365);
          return counter.incr();
        })
        .then(() => counter.incr())
        .then(() => counter.countRange('year', start))
        .then(result => {
          // Check promise
          expect(result).to.deep.equal(expected);

          // Check callback
          counter.countRange('year', start, (err, res) => {
            expect(res).to.deep.equal(expected);
            done();
          });
        })
        .catch(done);
    });

    it('should return 0 for counters where no data registed', done => {
      const counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'year'
      });

      // Increment 2014 once.

      const start = moment.utc({ year: 2014 });
      const end = moment.utc({ year: 2015 });
      const expected = {};
      expected[start.format()] = 1;
      expected[end.format()] = 0;

      sandbox.useFakeTimers(new Date('2014-02-01').getTime());
      counter.incr()
        .then(() => counter.countRange('year', start, end))
        .then(result => {
          // Check promise
          expect(result).to.deep.equal(expected);

          // Check callback
          counter.countRange('year', start, end, (err, res) => {
            expect(res).to.deep.equal(expected);
            done();
          });
        })
        .catch(done);
    });

    it('should return 0 for counters where no data is reg', done => {
      const counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'year'
      });

      // Increment 2014 once.

      const start = moment.utc({ year: 2014 });
      const end = moment.utc({ year: 2015 });
      const expected = {};
      expected[start.format()] = 1;
      expected[end.format()] = 0;

      sandbox.useFakeTimers(new Date('2014-02-01').getTime());
      counter.incr('bar')
        .then(() => counter.countRange('year', start, end, 'bar'))
        .then(result => {
          // Check promise
          expect(result).to.deep.equal(expected);

          // Check callback
          counter.countRange('year', start, end, 'bar', (err, res) => {
            expect(res).to.deep.equal(expected);
            done();
          });
        })
        .catch(done);
    });

    it('should accept strings for date range', done => {
      const counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'year'
      });

      // Increment 2014 once and 2015 twice

      const start = moment.utc({ year: 2014 });
      const end = moment.utc({ year: 2015 });
      const expected = {};
      expected[start.format()] = 1;
      expected[end.format()] = 2;

      const startStr = '2014-01-01T00:00:00Z';
      const endStr = '2015-01-01T00:00:00Z';

      const clock = sandbox.useFakeTimers(new Date('2014-02-01').getTime());
      counter.incr()
        .then(() => {
          clock.tick(1000 * 60 * 60 * 24 * 365);
          return counter.incr();
        })
        .then(() => counter.incr())
        .then(() => counter.countRange('year', startStr, endStr))
        .then(result => {
          // Check promise
          expect(result).to.deep.equal(expected);

          // Check callback
          counter.countRange('year', startStr, endStr, (err, res) => {
            expect(res).to.deep.equal(expected);
            done();
          });
        })
        .catch(done);
    });

    it('should accept numbers for date range', done => {
      const counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'year'
      });

      // Increment 2014 once and 2015 twice

      const start = moment.utc({ year: 2014 });
      const end = moment.utc({ year: 2015 });
      const expected = {};
      expected[start.format()] = 1;
      expected[end.format()] = 2;

      const startNum = start.valueOf();
      const endNum = end.valueOf();

      const clock = sandbox.useFakeTimers(new Date('2014-02-01').getTime());
      counter.incr()
        .then(() => {
          clock.tick(1000 * 60 * 60 * 24 * 365);
          return counter.incr();
        })
        .then(() => counter.incr())
        .then(() => counter.countRange('year', startNum, endNum))
        .then(result => {
          // Check promise
          expect(result).to.deep.equal(expected);

          // Check callback
          counter.countRange('year', startNum, endNum, (err, res) => {
            expect(res).to.deep.equal(expected);
            done();
          });
        })
        .catch(done);
    });

    it('should return a single num if "total" granularity is selected', done => {
      const counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'year'
      });

      // Increment 2014 once and 2015 twice => total is 3

      const start = moment.utc({ year: 2014 });
      const end = moment.utc({ year: 2015 });

      const clock = sandbox.useFakeTimers(new Date('2014-02-01').getTime());
      counter.incr()
        .then(() => {
          clock.tick(1000 * 60 * 60 * 24 * 365);
          return counter.incr();
        })
        .then(() => counter.incr())
        .then(() => counter.countRange('total', start, end))
        .then(result => {
          // Check promise
          expect(result).to.equal(3);

          // Check callback
          counter.countRange('total', start, end, (err, res) => {
            expect(res).to.equal(3);
            done();
          });
        })
        .catch(done);
    });

    it('should throw an exc if countRange is used on a counter', () => {
      const counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'total'
      });

      const throwClosure = () => counter.countRange('total', '2015', '2016');
      expect(throwClosure).to.throw(Error);
    });
  });

  describe('incr with expiration', () => {
    it('should set a ttl for a key', done => {
      const counter = new TimestampedCounter(metrics, 'foo', {
        expireKeys: true,
        expiration: {
          total: 100
        }
      });

      counter.incr().then(() => {
        const key = counter.getKeys()[0];
        metrics.client.ttl(key, (err, ttl) => {
          expect(err).to.equal(null);
          expect(ttl).to.be.above(0);
          expect(ttl).to.be.most(100);
          done();
        });
      })
      .catch(done);
    });

    it('should set a ttl for a key with event objects', done => {
      const counter = new TimestampedCounter(metrics, 'foo', {
        expireKeys: true,
        expiration: {
          total: 100
        }
      });

      counter.incr('bar').then(() => {
        const key = counter.getKeys()[0] + ':z';
        metrics.client.ttl(key, (err, ttl) => {
          expect(err).to.equal(null);
          expect(ttl).to.be.above(0);
          expect(ttl).to.be.most(100);
          done();
        });
      })
      .catch(done);
    });

    it('should not renew the ttl on the second call', done => {
      const counter = new TimestampedCounter(metrics, 'foo', {
        expireKeys: true,
        expiration: {
          total: 100
        }
      });

      counter.incr().then(() => {
        const key = counter.getKeys()[0];
        metrics.client.ttl(key, (err, ttl) => {
          setTimeout(() => {
            counter.incr().then(() => {
              metrics.client.ttl(key, (err2, ttl2) => {
                // Expect that ttl has decreased.
                expect(ttl2).to.be.below(ttl);
                expect(ttl2).to.be.within(ttl - 2, ttl);
                done();
              });
            });
          }, 1100);
        });
      })
      .catch(done);
    });

    it('should not renew the ttl on the second call', done => {
      const counter = new TimestampedCounter(metrics, 'foo', {
        expireKeys: true,
        expiration: {
          total: 100
        }
      });

      counter.incr('bar').then(() => {
        const key = counter.getKeys()[0] + ':z';
        metrics.client.ttl(key, (err, ttl) => {
          setTimeout(() => {
            counter.incr('bar').then(() => {
              metrics.client.ttl(key, (err2, ttl2) => {
                // Expect that ttl has decreased.
                expect(ttl2).to.be.below(ttl);
                expect(ttl2).to.be.within(ttl - 2, ttl);
                done();
              });
            });
          }, 1100);
        });
      })
      .catch(done);
    });
  });

  describe('incrby with expiration', () => {
    it('should set a ttl for a key', done => {
      const counter = new TimestampedCounter(metrics, 'foo', {
        expireKeys: true,
        expiration: {
          total: 60 // Gone in 60 seconds :-)
        }
      });

      counter.incrby(10).then(() => {
        const key = counter.getKeys()[0];
        metrics.client.ttl(key, (err, ttl) => {
          expect(err).to.equal(null);
          expect(ttl).to.be.above(0);
          expect(ttl).to.be.most(60);
          done();
        });
      })
      .catch(done);
    });

    it('should set a ttl for a key with event objects', done => {
      const counter = new TimestampedCounter(metrics, 'foo', {
        expireKeys: true,
        expiration: {
          total: 100
        }
      });

      counter.incrby(10, 'bar').then(() => {
        const key = counter.getKeys()[0] + ':z';
        metrics.client.ttl(key, (err, ttl) => {
          expect(err).to.equal(null);
          expect(ttl).to.be.above(0);
          expect(ttl).to.be.most(100);
          done();
        });
      })
      .catch(done);
    });
  });

  describe('top', () => {
    it('should work with callback', done => {
      const mock = sandbox.mock(metrics.client)
        .expects('zrevrange')
        .once()
        .withArgs('c:foo:z', 0, -1, 'WITHSCORES')
        .yields(null, ['foo', '39', 'bar', '13']);

      const counter = new TimestampedCounter(metrics, 'foo');
      counter.top('foo', (err, results) => {
        mock.verify();
        expect(results).to.have.length(2);
        expect(results[0]).to.have.property('foo');
        expect(results[0].foo).to.equal(39);
        done(err);
      });
    });

    it('should work with promises', done => {
      const mock = sandbox.mock(metrics.client)
        .expects('zrevrange')
        .once()
        .yields(null, ['foo', '39', 'bar', '13']);

      const counter = new TimestampedCounter(metrics, 'foo');
      counter.top('foo')
        .then(results => {
          mock.verify();
          expect(results).to.have.length(2);
          expect(results[0]).to.have.property('foo');
          expect(results[0].foo).to.equal(39);
          done();
        })
        .catch(done);
    });

    it('should accept a startingAt argument', done => {
      const mock = sandbox.mock(metrics.client)
        .expects('zrevrange')
        .once()
        .withArgs('c:foo:z', 10, -1, 'WITHSCORES')
        .yields(null, ['foo', '39', 'bar', '13']);

      const counter = new TimestampedCounter(metrics, 'foo');
      counter.top('foo', 'desc', 10, err => {
        mock.verify();
        done(err);
      });
    });

    it('should accept a startingAt and a limit argument', done => {
      const mock = sandbox.mock(metrics.client)
        .expects('zrevrange')
        .once()
        .withArgs('c:foo:z', 10, 15, 'WITHSCORES')
        .yields(null, ['foo', '39', 'bar', '13']);

      const counter = new TimestampedCounter(metrics, 'foo');
      counter.top('foo', 'desc', 10, 15, err => {
        mock.verify();
        done(err);
      });
    });

    it('should accept a direction argument with asc value', done => {
      const mock = sandbox.mock(metrics.client)
        .expects('zrange')
        .once()
        .withArgs('c:foo:z', 0, -1, 'WITHSCORES')
        .yields(null, ['foo', '39', 'bar', '13']);

      const counter = new TimestampedCounter(metrics, 'foo');
      counter.top('foo', 'asc', err => {
        mock.verify();
        done(err);
      });
    });

    it('should throw an exception if the direction argument is not correct', done => {
      const counter = new TimestampedCounter(metrics, 'foo');
      try {
        counter.top('foo', 'dummy');
      } catch (e) {
        return done();
      }

      throw new Error('This should never be called.');
    });

    it('should work with a real Redis connection', () => {
      const counter = new TimestampedCounter(metrics, 'foo');

      return counter.incrby(5, '/page1')
        .then(() => counter.incrby(3, '/page2'))
        .then(() => counter.top())
        .then(results => expect(results).to.eql([
          { '/page1': 5 },
          { '/page2': 3 }
        ]));
    });
  });

  describe('topRange', () => {
    it('should return toplists within the given range', done => {
      const counter = new TimestampedCounter(metrics, 'foo', { timeGranularity: 'year' });

      const start = moment.utc({ year: 2014 });
      const end = moment.utc({ year: 2015 });
      const expected = {};
      expected[start.format()] = [{ two: 2 }, { one: 1 }];
      expected[end.format()] = [{ two: 4 }, { one: 2 }];

      const clock = sandbox.useFakeTimers(new Date('2014-02-01').getTime());

      Promise.all([
        counter.incr('one'),
        counter.incr('two'),
        counter.incr('two')
      ])
      .then(() => {
        clock.tick(1000 * 60 * 60 * 24 * 365);
        return Promise.all([
          counter.incr('one'),
          counter.incr('one'),
          counter.incr('two'),
          counter.incr('two'),
          counter.incr('two'),
          counter.incr('two')
        ]);
      })
      .then(() => counter.topRange(start, end, 'year'))
      .then(result => {
        // Check promise
        expect(result).to.deep.equal(expected);

        // Check callback
        counter.topRange(start, end, 'year', (err, res) => {
          expect(res).to.deep.equal(expected);
          done();
        });
      })
      .catch(done);
    });

    it('should merge toplists within the given range if given "total" granularity', done => {
      const counter = new TimestampedCounter(metrics, 'foo', { timeGranularity: 'year' });

      const start = moment.utc({ year: 2014 });
      const end = moment.utc({ year: 2015 });
      const expected = [{ two: 6 }, { one: 3 }];

      const clock = sandbox.useFakeTimers(new Date('2014-02-01').getTime());

      Promise.all([
        counter.incr('one'),
        counter.incr('two'),
        counter.incr('two')
      ])
      .then(() => {
        clock.tick(1000 * 60 * 60 * 24 * 365);
        return Promise.all([
          counter.incr('one'),
          counter.incr('one'),
          counter.incr('two'),
          counter.incr('two'),
          counter.incr('two'),
          counter.incr('two')
        ]);
      })
      .then(() => {
        clock.tick(1000 * 60 * 60 * 24 * 365);
        return Promise.all([
          counter.incr('one'),
          counter.incr('two')
        ]);
      })
      .then(() => counter.topRange(start, end, 'total'))
      .then(result => {
        // Check promise
        expect(result).to.deep.equal(expected);

        // Check callback
        counter.topRange(start, end, 'total', (err, res) => {
          expect(res).to.deep.equal(expected);
          done();
        });
      })
      .catch(done);
    });

    it('can be customized with direction, startingAt and limit', done => {
      const counter = new TimestampedCounter(metrics, 'foo', { timeGranularity: 'year' });

      const start = moment.utc({ year: 2014 });
      const end = moment.utc({ year: 2015 });
      const expected = {};
      expected[start.format()] = [{ two: 2 }, { three: 3 }];
      expected[end.format()] = [{ two: 4 }, { three: 6 }];

      const clock = sandbox.useFakeTimers(new Date('2014-02-01').getTime());

      Promise.all([
        counter.incr('one'),
        counter.incr('two'),
        counter.incr('two'),
        counter.incr('three'),
        counter.incr('three'),
        counter.incr('three'),
        counter.incr('four'),
        counter.incr('four'),
        counter.incr('four'),
        counter.incr('four')
      ])
      .then(() => {
        clock.tick(1000 * 60 * 60 * 24 * 365);
        return Promise.all([
          counter.incr('one'),
          counter.incr('one'),
          counter.incr('two'),
          counter.incr('two'),
          counter.incr('two'),
          counter.incr('two'),
          counter.incr('three'),
          counter.incr('three'),
          counter.incr('three'),
          counter.incr('three'),
          counter.incr('three'),
          counter.incr('three'),
          counter.incr('four'),
          counter.incr('four'),
          counter.incr('four'),
          counter.incr('four'),
          counter.incr('four'),
          counter.incr('four'),
          counter.incr('four'),
          counter.incr('four')
        ]);
      })
      .then(() => counter.topRange(start, end, 'year', 'asc', 1, 2))
      .then(result => {
        // Check promise
        expect(result).to.deep.equal(expected);

        // Check callback
        counter.topRange(start, end, 'year', 'asc', 1, 2, (err, res) => {
          expect(res).to.deep.equal(expected);
          done();
        });
      })
      .catch(done);
    });

    it('can be customized with direction, startingAt and limit, on "total" granularity', done => {
      const counter = new TimestampedCounter(metrics, 'foo', { timeGranularity: 'year' });

      const start = moment.utc({ year: 2014 });
      const end = moment.utc({ year: 2015 });
      const expected = [{ two: 7 }, { three: 8 }];

      const clock = sandbox.useFakeTimers(new Date('2014-02-01').getTime());

      // orders are inverted in this first call to make sure that results
      // are not trimmed in the initial zscore calls
      Promise.all([
        counter.incr('one'),
        counter.incr('one'),
        counter.incr('one'),
        counter.incr('one'),
        counter.incr('two'),
        counter.incr('two'),
        counter.incr('two'),
        counter.incr('three'),
        counter.incr('three'),
        counter.incr('four')
      ])
      .then(() => {
        clock.tick(1000 * 60 * 60 * 24 * 365);
        return Promise.all([
          counter.incr('one'),
          counter.incr('one'),
          counter.incr('two'),
          counter.incr('two'),
          counter.incr('two'),
          counter.incr('two'),
          counter.incr('three'),
          counter.incr('three'),
          counter.incr('three'),
          counter.incr('three'),
          counter.incr('three'),
          counter.incr('three'),
          counter.incr('four'),
          counter.incr('four'),
          counter.incr('four'),
          counter.incr('four'),
          counter.incr('four'),
          counter.incr('four'),
          counter.incr('four'),
          counter.incr('four')
        ]);
      })
      .then(() => {
        clock.tick(1000 * 60 * 60 * 24 * 365);
        return Promise.all([
          counter.incr('one'),
          counter.incr('two')
        ]);
      })
      .then(() => counter.topRange(start, end, 'total', 'asc', 1, 2))
      .then(result => {
        // Check promise
        expect(result).to.deep.equal(expected);

        // Check callback
        counter.topRange(start, end, 'total', 'asc', 1, 2, (err, res) => {
          expect(res).to.deep.equal(expected);
          done();
        });
      })
      .catch(done);
    });

    it('should throw an exc if no from date is given', () => {
      const counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'total'
      });

      const throwClosure = () => counter.topRange();
      expect(throwClosure).to.throw(Error);
    });

    it('should throw an exc if an unsupported direction is given', () => {
      const counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'total'
      });

      const throwClosure = () => counter.topRange(new Date(), new Date(), 'total', 1);
      expect(throwClosure).to.throw(Error);
    });
  });

  describe('trimEvents', () => {
    let counter;
    beforeEach(() => {
      counter = new TimestampedCounter(metrics, 'fruits', {
        timeGranularity: 'hour'
      });
      return Promise.all([
        counter.incrby(10, 'apples'),
        counter.incrby(8, 'oranges'),
        counter.incrby(7, 'pears'),
        counter.incrby(5, 'peaches'),
        counter.incrby(2, 'mangos')
      ]);
    });

    it('should throw an exception if the direction argument is not correct', done => {
      try {
        counter.trimEvents('dummy');
      } catch (e) {
        return done();
      }

      throw new Error('This should never be called.');
    });

    it('should resolve to a failure if a subcommand fails', done => {
      sandbox.stub(metrics.client, 'zremrangebyrank')
        .yields(new Error('oh no'));
      counter.trimEvents()
        .then(() => done(new Error('should not be here')))
        .catch(err => {
          expect(err).to.be.an('error');
          expect(err.message).to.equal('oh no');
          done();
        });
    });

    it('should call the callback with an error if a subcommand fails', done => {
      sandbox.stub(metrics.client, 'zremrangebyrank')
        .yields(new Error('oh no'));
      counter.trimEvents(err => {
        try {
          expect(err).to.be.an('error');
          expect(err.message).to.equal('oh no');
          done();
        } catch (e) {
          done(e);
        }
      });
    });

    it('should trim to 1000 elements by default', () => {
      // This means it keeps all of them, because there's only 5 elements.
      return counter.trimEvents()
        .then(removed => {
          expect(removed).to.equal(0);
          return counter.top();
        })
        .then(topN => expect(topN.length).to.equal(5));
    });

    it('should support descending trim', () => {
      return counter.trimEvents('desc', 3)
        .then(removed => {
          // It removed 2 from total, 2 from year, 2 from month and 2 from day.
          expect(removed).to.equal(8);
          return counter.top();
        })
        .then(topN => {
          expect(topN.length).to.equal(3);
          expect(topN).to.eql([
            { apples: 10 },
            { oranges: 8 },
            { pears: 7 }
          ]);
        });
    });

    it('should support ascending trim', () => {
      return counter.trimEvents('asc', 3)
        .then(removed => {
          // It removed 2 from total, 2 from year, 2 from month and 2 from day.
          expect(removed).to.equal(8);
          return counter.top();
        })
        .then(topN => {
          expect(topN.length).to.equal(3);
          // It kept the lowest scores, but the top() function returns
          // descending sort by default.
          expect(topN).to.eql([
            { pears: 7 },
            { peaches: 5 },
            { mangos: 2 }
          ]);
        });
    });

    it('should support a callback', done => {
      counter.trimEvents('desc', 3, (err, removed) => {
        try {
          expect(err).to.equal(null);
          expect(removed).to.equal(8);
          done();
        } catch (e) {
          done(e);
        }
      });
    });

    context('given a counter with only total granularity', () => {
      beforeEach(() => {
        // This is cheating a bit since the counter has already been incremented
        // at the hourly level.
        counter.options.timeGranularity = constants.timeGranularities.none;
      });

      it('should trim to 1000 elements by default', () => {
        // This means it keeps all of them, because there's only 5 elements.
        return counter.trimEvents()
          .then(removed => {
            expect(removed).to.equal(0);
            return counter.top();
          })
          .then(topN => expect(topN.length).to.equal(5));
      });

      it('should support descending trim', () => {
        return counter.trimEvents('desc', 3)
          .then(removed => {
            expect(removed).to.equal(2);
            return counter.top();
          })
          .then(topN => {
            expect(topN.length).to.equal(3);
            expect(topN).to.eql([
              { apples: 10 },
              { oranges: 8 },
              { pears: 7 }
            ]);
          });
      });

      it('should support ascending trim', () => {
        return counter.trimEvents('asc', 3)
          .then(removed => {
            expect(removed).to.equal(2);
            return counter.top();
          })
          .then(topN => {
            expect(topN.length).to.equal(3);
            // It kept the lowest scores, but the top() function returns
            // descending sort by default.
            expect(topN).to.eql([
              { pears: 7 },
              { peaches: 5 },
              { mangos: 2 }
            ]);
          });
      });
    });
  });

  describe('zero', () => {
    it('clears counts at the second level', () => {
      const counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'second'
      });

      const clock = sandbox.useFakeTimers(new Date('2015-01-01').getTime());
      return counter.incr()
        .then(() => {
          clock.tick(1000);
          return counter.incr();
        })
        .then(() => counter.incr())
        .then(() => counter.count('total'))
        .then(count => {
          expect(count).to.equal(3);
          return counter.zero();
        })
        .then(() => counter.count('total'))
        .then(count => expect(count).to.equal(0));
    });

    it('clears counts with no granularity', () => {
      const counter = new TimestampedCounter(metrics, 'foo');

      return Promise.all([counter.incr(), counter.incr()])
        .then(() => counter.count('total'))
        .then(count => {
          expect(count).to.equal(2);
          return counter.zero();
        })
        .then(() => counter.count('total'))
        .then(count => expect(count).to.equal(0));
    });

    it('clears counts for an event object', () => {
      const counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'year'
      });

      const clock = sandbox.useFakeTimers(new Date('2014-02-01').getTime());
      return Promise.all([
        counter.incrby(2, 'five'),
        counter.incr('two')
      ])
      .then(() => {
        clock.tick(1000 * 60 * 60 * 24 * 365);
        return Promise.all([
          counter.incrby(3, 'five'),
          counter.incr('two')
        ]);
      })
      .then(() => Promise.all([counter.count('total', 'five'), counter.count('total', 'two')]))
      .then(res => {
        expect(res[0]).to.equal(5);
        expect(res[1]).to.equal(2);
        return counter.zero('five');
      })
      .then(() => Promise.all([counter.count('total', 'five'), counter.count('total', 'two')]))
      .then(res => {
        expect(res[0]).to.equal(0);
        expect(res[1]).to.equal(2);
      });
    });

    it('clears the ranges as well', () => {
      const counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'day'
      });

      const start = moment.utc({ year: 2015, day: 1 });
      const end = moment.utc({ year: 2015, day: 2 });
      const expectedBefore = {};
      expectedBefore[start.format()] = 1;
      expectedBefore[end.format()] = 2;

      const expectedAfter = {};
      expectedAfter[start.format()] = 0;
      expectedAfter[end.format()] = 0;

      const clock = sandbox.useFakeTimers(new Date('2015-01-01').getTime());
      return counter.incr()
        .then(() => {
          clock.tick(1000 * 60 * 60 * 24);
          return counter.incrby(2);
        })
        .then(() => counter.countRange('day', start, end))
        .then(result => {
          expect(result).to.deep.equal(expectedBefore);
          return counter.zero();
        })
        .then(() => counter.countRange('day', start, end))
        .then(result => expect(result).to.deep.equal(expectedAfter));
    });
  });
});
