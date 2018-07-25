'use strict';

const { expect } = require('chai'),
  sinon = require('sinon'),
  moment = require('moment'),
  IORedis = require('ioredis'),
  { ensureError } = require('./utils'),
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

    it('should resolve a promise on success', () => {
      const counter = new TimestampedCounter(metrics, 'foo');

      return counter.incr().then(result => {
        expect(result).to.equal(1);
      });
    });

    it('should reject on error', () => {
      sandbox.stub(metrics.client, 'incrby')
        .yields(new Error('oh no'), null);

      const counter = new TimestampedCounter(
        metrics,
        'foo',
        { expireKeys: false }
      );

      return ensureError(() => counter.incr());
    });

    it('should return a list of results', () => {
      const counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'year'
      });

      return counter.incr()
        .then(results => {
          expect(results).to.be.instanceof(Array);
          expect(results).to.deep.equal([1, 1]);
        });
    });

    it('should return a list of results for non-expiring keys', () => {
      const counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'year',
        expireKeys: false
      });

      return counter.incr()
        .then(results => {
          expect(results).to.be.instanceof(Array);
          expect(results).to.deep.equal([1, 1]);
        });
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

      return counter.incr({ eventObj: 'bar' }).then(result => {
        expect(Number(result)).to.equal(1);
      });
    });

    it('should work with an event object and non-expiring keys', () => {
      const counter = new TimestampedCounter(
        metrics,
        'foo',
        { expireKeys: false }
      );

      return counter.incr({ eventObj: 'bar' }).then(result => {
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

      return counter.incr({ eventObj: 'bar' }).then(results => {
        expect(utils.parseIntArray(results)).to.deep.equal([1, 1]);
      });
    });

    it('should work with an event, time gran and non-exp keys', () => {
      const counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'year',
        expireKeys: false
      });

      return counter.incr({ eventObj: 'bar' }).then(results => {
        expect(utils.parseIntArray(results)).to.deep.equal([1, 1]);
      });
    });
  });

  describe('incr with expiration', () => {
    it('should set a ttl for a key', () => {
      const counter = new TimestampedCounter(metrics, 'foo', {
        expireKeys: true,
        expiration: {
          total: 100
        }
      });

      return counter.incr()
        .then(() => {
          const [key] = counter.getKeys();
          return utils.ninvoke(metrics.client, 'ttl', key);
        })
        .then(ttl => {
          expect(ttl).to.be.above(0);
          expect(ttl).to.be.most(100);
        });
    });

    it('should set a ttl for a key with event objects', () => {
      const counter = new TimestampedCounter(metrics, 'foo', {
        expireKeys: true,
        expiration: {
          total: 100
        }
      });

      return counter.incr({ eventObj: 'bar' })
        .then(() => {
          const key = counter.getKeys()[0] + ':z';
          return utils.ninvoke(metrics.client, 'ttl', key);
        })
        .then(ttl => {
          expect(ttl).to.be.above(0);
          expect(ttl).to.be.most(100);
        });
    });

    it('should not renew the ttl on the second call', () => {
      const counter = new TimestampedCounter(metrics, 'foo', {
        expireKeys: true,
        expiration: {
          total: 100
        }
      });

      let key;

      return counter.incr()
        .then(() => {
          [key] = counter.getKeys();
          return utils.ninvoke(metrics.client, 'ttl', key);
        })
        .then(ttl => new Promise((resolve, reject) => {
          setTimeout(() => {
            counter.incr()
              .then(() => utils.ninvoke(metrics.client, 'ttl', key))
              .then(ttl2 => {
                // Expect that ttl has decreased.
                expect(ttl2).to.be.below(ttl);
                expect(ttl2).to.be.within(ttl - 2, ttl);
                resolve();
              })
              .catch(reject);
          }, 1100);
        }));
    });

    it('should not renew the ttl on the second call for eventObj', () => {
      const counter = new TimestampedCounter(metrics, 'foo', {
        expireKeys: true,
        expiration: {
          total: 100
        }
      });

      let key;

      return counter.incr({ eventObj: 'bar' })
        .then(() => {
          key = counter.getKeys()[0] + ':z';
          return utils.ninvoke(metrics.client, 'ttl', key);
        })
        .then(ttl => new Promise((resolve, reject) => {
          setTimeout(() => {
            counter.incr({ eventObj: 'bar' })
              .then(() => utils.ninvoke(metrics.client, 'ttl', key))
              .then(ttl2 => {
                // Expect that ttl has decreased.
                expect(ttl2).to.be.below(ttl);
                expect(ttl2).to.be.within(ttl - 2, ttl);
                resolve();
              })
              .catch(reject);
          }, 1100);
        }));
    });
  });

  describe('incrby', () => {
    it('should call redis without a tran when keys do not exp', () => {
      const multiSpy = sandbox.spy(metrics.client, 'multi');
      const incrSpy = sandbox.spy(metrics.client, 'incrby');

      const counter = new TimestampedCounter(
        metrics,
        'foo',
        { expireKeys: false }
      );

      return counter.incrby(2).then(() => {
        sinon.assert.calledOnce(incrSpy);
        sinon.assert.notCalled(multiSpy);
      });
    });

    it('should call redis without a trans when counters exp', () => {
      const multiSpy = sandbox.spy(metrics.client, 'multi');
      const evalSpy = sandbox.spy(metrics.client, 'eval');

      const counter = new TimestampedCounter(metrics, 'foo', {
        expireKeys: true,
        expiration: {
          total: 10
        }
      });

      return counter.incrby(2).then(() => {
        sinon.assert.calledOnce(evalSpy);
        sinon.assert.notCalled(multiSpy);
      });
    });

    it('should call redis when a time granularity is chosen', () => {
      const multiSpy = sandbox.spy(metrics.client, 'multi');

      const counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'year'
      });

      return counter.incrby(3).then(() => {
        sinon.assert.calledOnce(multiSpy);
      });
    });

    it('should resolve on success', () => {
      const counter = new TimestampedCounter(metrics, 'foo');

      return counter.incrby(5).then(result => {
        expect(result).to.equal(5);
      });
    });

    it('should reject the promise on redis error from incrby', () => {
      sandbox.stub(metrics.client, 'incrby')
        .yields(new Error('oh no'), null);

      const counter = new TimestampedCounter(
        metrics,
        'foo',
        { expireKeys: false }
      );

      return ensureError(() => counter.incrby(7));
    });

    it('should reject the promise on redis error with eval', () => {
      sandbox.stub(metrics.client, 'eval')
        .yields(new Error('oh no'), null);

      const counter = new TimestampedCounter(metrics, 'foo', {
        expireKeys: true,
        expiration: {
          total: 10
        }
      });

      return ensureError(() => counter.incrby(7));
    });

    it('should resolve a list of results from the operation', () => {
      const counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'year'
      });

      return counter.incrby(8).then(results => {
        expect(results).to.be.instanceof(Array);
        expect(results).to.deep.equal([8, 8]);
      });
    });
  });

  describe('incrby with event object', () => {
    it('should work with an event object', () => {
      const counter = new TimestampedCounter(metrics, 'foo');

      return counter.incrby(9, { eventObj: 'bar' }).then(result => {
        expect(Number(result)).to.equal(9);
      });
    });

    it('should work with an event object and time granularity', () => {
      const counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'year'
      });

      return counter.incrby(10, { eventObj: 'bar' }).then(results => {
        expect(utils.parseIntArray(results)).to.deep.equal([10, 10]);
      });
    });
  });

  describe('incrby with expiration', () => {
    it('should set a ttl for a key', () => {
      const counter = new TimestampedCounter(metrics, 'foo', {
        expireKeys: true,
        expiration: {
          total: 60 // Gone in 60 seconds :-)
        }
      });

      return counter.incrby(10)
        .then(() => {
          const [key] = counter.getKeys();
          return utils.ninvoke(metrics.client, 'ttl', key);
        })
        .then(ttl => {
          expect(ttl).to.be.above(0);
          expect(ttl).to.be.most(60);
        });
    });

    it('should set a ttl for a key with event objects', () => {
      const counter = new TimestampedCounter(metrics, 'foo', {
        expireKeys: true,
        expiration: {
          total: 100
        }
      });

      return counter.incrby(10, { eventObj: 'bar' })
        .then(() => {
          const key = counter.getKeys()[0] + ':z';
          return utils.ninvoke(metrics.client, 'ttl', key);
        })
        .then(ttl => {
          expect(ttl).to.be.above(0);
          expect(ttl).to.be.most(100);
        });
    });
  });

  describe('count', () => {
    it('should return a single res when no arg are given', () => {
      const counter = new TimestampedCounter(metrics, 'foo');
      return Promise.all([
        counter.incr(),
        counter.incr(),
        counter.incr()
      ])
      .then(() => counter.count())
      .then(result => {
        expect(result).to.equal(3);
      });
    });

    it('should return a single result for an event object', () => {
      const counter = new TimestampedCounter(metrics, 'foo');
      return Promise.all([
        counter.incr({ eventObj: 'bar' }),
        counter.incr({ eventObj: 'bar' }),
        counter.incr({ eventObj: 'bar' })
      ])
      .then(() => counter.count({ eventObj: 'bar' }))
      .then(result => {
        expect(result).to.equal(3);
      });
    });

    it('should return 0 when the key does not exist', () => {
      const mock = sandbox.mock(metrics.client)
        .expects('get')
        .once()
        .yields(null, null);

      const counter = new TimestampedCounter(metrics, 'foo');

      return counter.count()
        .then(result => {
          mock.verify();
          expect(result).to.equal(0);
        });
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
        .then(() => counter.count()) // same as counter.count();
        .then(result => {
          expect(result).to.equal(2);
          return counter.count({ timeGranularity: 'year' });
        })
        .then(result => expect(result).to.equal(1));
    });

    it('should return a count for a specific granularity and event', () => {
      const counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'year'
      });

      // Increment in 2014 and 2015
      // Total should be 2 but year should be 1.

      const clock = sandbox.useFakeTimers(new Date('2014-02-01').getTime());
      return counter.incr({ eventObj: 'bar' })
        .then(() => {
          clock.tick(1000 * 60 * 60 * 24 * 365);
          return counter.incr({ eventObj: 'bar' });
        })
        .then(() => counter.count({ eventObj: 'bar' }))
        .then(result => {
          expect(result).to.equal(2);
          return counter.count({ timeGranularity: 'year', eventObj: 'bar' });
        })
        .then(result => expect(result).to.equal(1));
    });
  });

  describe('countRange', () => {
    it('should return a range of counts', () => {
      const counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'year'
      });

      // Increment 2014 once and 2015 twice
      const startDate = moment.utc({ year: 2014 });
      const endDate = moment.utc({ year: 2015 });

      const clock = sandbox.useFakeTimers(new Date('2014-02-01').getTime());
      return counter.incr()
        .then(() => {
          clock.tick(1000 * 60 * 60 * 24 * 365);
          return Promise.all([
            counter.incr(),
            counter.incr()
          ]);
        })
        .then(() => counter.countRange('year', { startDate, endDate }))
        .then(result => {
          expect(result).to.deep.equal({
            [startDate.format()]: 1,
            [endDate.format()]: 2
          });
        });
    });

    it('should return a range of counts at the second level', () => {
      const counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'second'
      });

      const startDate = moment.utc({ year: 2015, second: 0 });
      const endDate = moment.utc({ year: 2015, second: 1 });

      const clock = sandbox.useFakeTimers(new Date('2015-01-01').getTime());
      return counter.incr()
        .then(() => {
          clock.tick(1000);
          return Promise.all([
            counter.incr(),
            counter.incr()
          ]);
        })
        .then(() => counter.countRange('second', { startDate, endDate }))
        .then(result => {
          expect(result).to.deep.equal({
            [startDate.format()]: 1,
            [endDate.format()]: 2
          });
        });
    });

    it('should return a range of count for an event object', () => {
      const counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'year'
      });

      const startDate = moment.utc({ year: 2014 });
      const endDate = moment.utc({ year: 2015 });

      const clock = sandbox.useFakeTimers(new Date('2014-02-01').getTime());
      return counter.incr({ eventObj: 'bar' })
        .then(() => {
          clock.tick(1000 * 60 * 60 * 24 * 365);
          return Promise.all([
            counter.incr({ eventObj: 'bar' }),
            counter.incr({ eventObj: 'bar' })
          ]);
        })
        .then(() => counter.countRange('year', { startDate, endDate }, { eventObj: 'bar' }))
        .then(result => {
          expect(result).to.deep.equal({
            [startDate.format()]: 1,
            [endDate.format()]: 2
          });
        });
    });

    it('should use current date if no end date is provided', () => {
      const counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'year'
      });

      const startDate = moment.utc({ year: 2014 });

      const clock = sandbox.useFakeTimers(new Date('2014-02-01').getTime());
      return counter.incr()
        .then(() => {
          clock.tick(1000 * 60 * 60 * 24 * 365);
          return Promise.all([
            counter.incr(),
            counter.incr()
          ]);
        })
        .then(() => counter.countRange('year', { startDate }))
        .then(result => {
          expect(result).to.deep.equal({
            [startDate.format()]: 1,
            [moment.utc(startDate).add(1, 'years').format()]: 2
          });
        });
    });

    it('should return 0 for counters where no data registed', () => {
      const counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'year'
      });

      // Increment 2014 once.
      const startDate = moment.utc({ year: 2014 });
      const endDate = moment.utc({ year: 2015 });

      sandbox.useFakeTimers(new Date('2014-02-01').getTime());
      return counter.incr()
        .then(() => counter.countRange('year', { startDate, endDate }))
        .then(result => {
          expect(result).to.deep.equal({
            [startDate.format()]: 1,
            [endDate.format()]: 0
          });
        });
    });

    it('should return 0 for eventObj counters where no data is registered', () => {
      const counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'year'
      });

      // Increment 2014 once.
      const startDate = moment.utc({ year: 2014 });
      const endDate = moment.utc({ year: 2015 });

      sandbox.useFakeTimers(new Date('2014-02-01').getTime());
      return counter.incr({ eventObj: 'bar' })
        .then(() => counter.countRange('year', { startDate, endDate }, { eventObj: 'bar' }))
        .then(result => {
          expect(result).to.deep.equal({
            [startDate.format()]: 1,
            [endDate.format()]: 0
          });
        });
    });

    it('should accept strings for date range', () => {
      const counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'year'
      });

      const startDate = moment.utc({ year: 2014 });
      const endDate = moment.utc({ year: 2015 });

      const startStr = '2014-01-01T00:00:00Z';
      const endStr = '2015-01-01T00:00:00Z';

      const clock = sandbox.useFakeTimers(new Date('2014-02-01').getTime());
      return counter.incr()
        .then(() => {
          clock.tick(1000 * 60 * 60 * 24 * 365);
          return Promise.all([
            counter.incr(),
            counter.incr()
          ]);
        })
        .then(() => counter.countRange('year', { startDate: startStr, endDate: endStr }))
        .then(result => {
          expect(result).to.deep.equal({
            [startDate.format()]: 1,
            [endDate.format()]: 2
          });
        });
    });

    it('should accept numbers for date range', () => {
      const counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'year'
      });

      const start = moment.utc({ year: 2014 });
      const end = moment.utc({ year: 2015 });

      const startNum = start.valueOf();
      const endNum = end.valueOf();

      const clock = sandbox.useFakeTimers(new Date('2014-02-01').getTime());
      return counter.incr()
        .then(() => {
          clock.tick(1000 * 60 * 60 * 24 * 365);
          return Promise.all([
            counter.incr(),
            counter.incr()
          ]);
        })
        .then(() => counter.countRange('year', { startDate: startNum, endDate: endNum }))
        .then(result => {
          expect(result).to.deep.equal({
            [start.format()]: 1,
            [end.format()]: 2
          });
        });
    });

    it('should return a single num if "total" granularity is selected', () => {
      const counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'year'
      });

      // Increment 2014 once and 2015 twice => total is 3
      const startDate = moment.utc({ year: 2014 });
      const endDate = moment.utc({ year: 2015 });

      const clock = sandbox.useFakeTimers(new Date('2014-02-01').getTime());
      return counter.incr()
        .then(() => {
          clock.tick(1000 * 60 * 60 * 24 * 365);
          return Promise.all([
            counter.incr(),
            counter.incr()
          ]);
        })
        .then(() => counter.countRange('total', { startDate, endDate }))
        .then(result => {
          expect(result).to.equal(3);
        });
    });

    it('should reject if countRange is used on a counter', () => {
      const counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'total'
      });

      return ensureError(
        () => counter.countRange('total', { startDate: '2015', endDate: '2016' }),
        err => expect(/total granularity/.test(err.message)).to.be.true
      );
    });
  });

  describe('top', () => {
    it('should resolve the toplist', () => {
      const mock = sandbox.mock(metrics.client)
        .expects('zrevrange')
        .once()
        .yields(null, ['foo', '39', 'bar', '13']);

      const counter = new TimestampedCounter(metrics, 'foo');
      return counter.top()
        .then(results => {
          mock.verify();
          expect(results).to.have.length(2);
          expect(results[0]).to.have.property('foo');
          expect(results[0].foo).to.equal(39);
        });
    });

    it('should accept a startingAt option', () => {
      const mock = sandbox.mock(metrics.client)
        .expects('zrevrange')
        .once()
        .withArgs('c:foo:z', 10, -1, 'WITHSCORES')
        .yields(null, ['foo', '39', 'bar', '13']);

      const counter = new TimestampedCounter(metrics, 'foo');
      return counter.top({ startingAt: 10 })
        .then(() => mock.verify());
    });

    it('should accept startingAt and limit options', () => {
      const mock = sandbox.mock(metrics.client)
        .expects('zrevrange')
        .once()
        .withArgs('c:foo:z', 10, 15, 'WITHSCORES')
        .yields(null, ['foo', '39', 'bar', '13']);

      const counter = new TimestampedCounter(metrics, 'foo');
      return counter.top({ startingAt: 10, limit: 15 })
        .then(() => mock.verify());
    });

    it('should accept a direction option with asc value', () => {
      const mock = sandbox.mock(metrics.client)
        .expects('zrange')
        .once()
        .withArgs('c:foo:z', 0, -1, 'WITHSCORES')
        .yields(null, ['foo', '39', 'bar', '13']);

      const counter = new TimestampedCounter(metrics, 'foo');
      return counter.top({ direction: 'asc' })
        .then(() => mock.verify());
    });

    it('should reject if the direction option is invalid', () => {
      const counter = new TimestampedCounter(metrics, 'foo');

      return ensureError(
        () => counter.top({ direction: 'dummy' }),
        err => expect(err.message).to.match(/direction/)
      );
    });

    it('should work with a real Redis connection', () => {
      const counter = new TimestampedCounter(metrics, 'foo');

      return counter.incrby(5, { eventObj: '/page1' })
        .then(() => counter.incrby(3, { eventObj: '/page2' }))
        .then(() => counter.top())
        .then(results => expect(results).to.eql([
          { '/page1': 5 },
          { '/page2': 3 }
        ]));
    });
  });

  describe('topRange', () => {
    it('should return toplists within the given range', () => {
      const counter = new TimestampedCounter(metrics, 'foo', { timeGranularity: 'year' });

      const startDate = moment.utc({ year: 2014 });
      const endDate = moment.utc({ year: 2015 });

      const clock = sandbox.useFakeTimers(new Date('2014-02-01').getTime());

      return Promise.all([
        counter.incr({ eventObj: 'one' }),
        counter.incrby(2, { eventObj: 'two' })
      ])
      .then(() => {
        clock.tick(1000 * 60 * 60 * 24 * 365);
        return Promise.all([
          counter.incrby(2, { eventObj: 'one' }),
          counter.incrby(4, { eventObj: 'two' })
        ]);
      })
      .then(() => counter.topRange({ startDate, endDate }, { timeGranularity: 'year' }))
      .then(result => {
        expect(result).to.deep.equal({
          [startDate.format()]: [{ two: 2 }, { one: 1 }],
          [endDate.format()]: [{ two: 4 }, { one: 2 }]
        });
      });
    });

    it('should merge toplists within the given range if given "total" granularity', () => {
      const counter = new TimestampedCounter(metrics, 'foo', { timeGranularity: 'year' });

      const startDate = moment.utc({ year: 2014 });
      const endDate = moment.utc({ year: 2015 });

      const clock = sandbox.useFakeTimers(new Date('2014-02-01').getTime());

      return Promise.all([
        counter.incr({ eventObj: 'one' }),
        counter.incrby(2, { eventObj: 'two' })
      ])
      .then(() => {
        clock.tick(1000 * 60 * 60 * 24 * 365);
        return Promise.all([
          counter.incrby(2, { eventObj: 'one' }),
          counter.incrby(4, { eventObj: 'two' })
        ]);
      })
      .then(() => {
        clock.tick(1000 * 60 * 60 * 24 * 365);
        return Promise.all([
          counter.incr({ eventObj: 'one' }),
          counter.incr({ eventObj: 'two' })
        ]);
      })
      .then(() => counter.topRange({ startDate, endDate }, { timeGranularity: 'total' }))
      .then(result => {
        expect(result).to.deep.equal([{ two: 6 }, { one: 3 }]);
      });
    });

    it('can be customized with direction, startingAt and limit', () => {
      const counter = new TimestampedCounter(metrics, 'foo', { timeGranularity: 'year' });

      const startDate = moment.utc({ year: 2014 });
      const endDate = moment.utc({ year: 2015 });

      const clock = sandbox.useFakeTimers(new Date('2014-02-01').getTime());
      return Promise.all([
        counter.incr({ eventObj: 'one' }),
        counter.incrby(2, { eventObj: 'two' }),
        counter.incrby(3, { eventObj: 'three' }),
        counter.incrby(4, { eventObj: 'four' })
      ])
      .then(() => {
        clock.tick(1000 * 60 * 60 * 24 * 365);
        return Promise.all([
          counter.incrby(2, { eventObj: 'one' }),
          counter.incrby(4, { eventObj: 'two' }),
          counter.incrby(6, { eventObj: 'three' }),
          counter.incrby(8, { eventObj: 'four' })
        ]);
      })
      .then(() => counter.topRange(
        { startDate, endDate },
        { timeGranularity: 'year', direction: 'asc', startingAt: 1, limit: 2 }
      ))
      .then(result => {
        expect(result).to.deep.equal({
          [startDate.format()]: [{ two: 2 }, { three: 3 }],
          [endDate.format()]: [{ two: 4 }, { three: 6 }]
        });
      });
    });

    it('can be customized with direction, startingAt and limit, on "total" granularity', () => {
      const counter = new TimestampedCounter(metrics, 'foo', { timeGranularity: 'year' });

      const startDate = moment.utc({ year: 2014 });
      const endDate = moment.utc({ year: 2015 });

      const clock = sandbox.useFakeTimers(new Date('2014-02-01').getTime());

      // orders are inverted in this first call to make sure that results
      // are not trimmed in the initial zscore calls
      return Promise.all([
        counter.incrby(4, { eventObj: 'one' }),
        counter.incrby(3, { eventObj: 'two' }),
        counter.incrby(2, { eventObj: 'three' }),
        counter.incr({ eventObj: 'four' })
      ])
      .then(() => {
        clock.tick(1000 * 60 * 60 * 24 * 365);
        return Promise.all([
          counter.incrby(2, { eventObj: 'one' }),
          counter.incrby(4, { eventObj: 'two' }),
          counter.incrby(6, { eventObj: 'three' }),
          counter.incrby(8, { eventObj: 'four' })
        ]);
      })
      .then(() => {
        clock.tick(1000 * 60 * 60 * 24 * 365);
        return Promise.all([
          counter.incr({ eventObj: 'one' }),
          counter.incr({ eventObj: 'two' })
        ]);
      })
      .then(() => counter.topRange(
        { startDate, endDate },
        { timeGranularity: 'total', direction: 'asc', startingAt: 1, limit: 2 }
      ))
      .then(result => {
        expect(result).to.deep.equal([{ two: 7 }, { three: 8 }]);
      });
    });

    it('should reject if no start date is given', () => {
      const counter = new TimestampedCounter(metrics, 'foo', { timeGranularity: 'year' });

      return ensureError(
        () => counter.topRange({}),
        err => expect(err.message).to.match(/startDate/)
      );
    });

    it('should throw an exc if an unsupported direction is given', () => {
      const counter = new TimestampedCounter(metrics, 'foo', { timeGranularity: 'year' });

      return ensureError(
        () => counter.topRange(new Date(), { direction: 1 }),
        err => expect(err.message).to.match(/direction option/)
      );
    });
  });

  describe('trimEvents', () => {
    let counter;

    beforeEach(() => {
      counter = new TimestampedCounter(metrics, 'fruits', {
        timeGranularity: 'hour'
      });
      return Promise.all([
        counter.incrby(10, { eventObj: 'apples' }),
        counter.incrby(8, { eventObj: 'oranges' }),
        counter.incrby(7, { eventObj: 'pears' }),
        counter.incrby(5, { eventObj: 'peaches' }),
        counter.incrby(2, { eventObj: 'mangos' })
      ]);
    });

    it('should reject if the direction argument is not correct', () => {
      return ensureError(
        () => counter.trimEvents({ direction: 'dummy' }),
        err => expect(err.message).to.match(/direction/)
      );
    });

    it('should reject if a subcommand fails', () => {
      sandbox.stub(metrics.client, 'zremrangebyrank')
        .yields(new Error('oh no'));

      return ensureError(
        () => counter.trimEvents(),
        err => {
          expect(err).to.be.an('error');
          expect(err.message).to.equal('oh no');
        }
      );
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
      return counter.trimEvents({ direction: 'desc', limit: 3 })
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
      return counter.trimEvents({ direction: 'asc', limit: 3 })
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
        return counter.trimEvents({ direction: 'desc', limit: 3 })
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
        return counter.trimEvents({ direction: 'asc', limit: 3 })
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
        .then(() => counter.count())
        .then(count => {
          expect(count).to.equal(3);
          return counter.zero();
        })
        .then(() => counter.count())
        .then(count => expect(count).to.equal(0));
    });

    it('clears counts with no granularity', () => {
      const counter = new TimestampedCounter(metrics, 'foo');

      return Promise.all([counter.incr(), counter.incr()])
        .then(() => counter.count())
        .then(count => {
          expect(count).to.equal(2);
          return counter.zero();
        })
        .then(() => counter.count())
        .then(count => expect(count).to.equal(0));
    });

    it('clears counts for an event object', () => {
      const counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'year'
      });

      const clock = sandbox.useFakeTimers(new Date('2014-02-01').getTime());
      return Promise.all([
        counter.incrby(2, { eventObj: 'five' }),
        counter.incr({ eventObj: 'two' })
      ])
      .then(() => {
        clock.tick(1000 * 60 * 60 * 24 * 365);
        return Promise.all([
          counter.incrby(3, { eventObj: 'five' }),
          counter.incr({ eventObj: 'two' })
        ]);
      })
      .then(() => Promise.all([
        counter.count({ eventObj: 'five' }),
        counter.count({ eventObj: 'two' })
      ]))
      .then(([fiveCount, twoCount]) => {
        expect(fiveCount).to.equal(5);
        expect(twoCount).to.equal(2);
        return counter.zero({ eventObj: 'five' });
      })
      .then(() => Promise.all([
        counter.count({ eventObj: 'five' }),
        counter.count({ eventObj: 'two' })
      ]))
      .then(([fiveCount, twoCount]) => {
        expect(fiveCount).to.equal(0);
        expect(twoCount).to.equal(2);
      });
    });

    it('clears the ranges as well', () => {
      const counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'day'
      });

      const startDate = moment.utc({ year: 2015, day: 1 });
      const endDate = moment.utc({ year: 2015, day: 2 });

      const clock = sandbox.useFakeTimers(new Date('2015-01-01').getTime());
      return counter.incr()
        .then(() => {
          clock.tick(1000 * 60 * 60 * 24);
          return counter.incrby(2);
        })
        .then(() => counter.countRange('day', { startDate, endDate }))
        .then(result => {
          expect(result).to.deep.equal({
            [startDate.format()]: 1,
            [endDate.format()]: 2
          });
          return counter.zero();
        })
        .then(() => counter.countRange('day', { startDate, endDate }))
        .then(result => expect(result).to.deep.equal({
          [startDate.format()]: 0,
          [endDate.format()]: 0
        }));
    });
  });
});
