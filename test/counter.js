'use strict';

var chai = require('chai'),
  sinon = require('sinon'),
  moment = require('moment'),
  RedisMetrics = require('../lib/metrics'),
  TimestampedCounter = require('../lib/counter'),
  utils = require('../lib/utils');

var expect = chai.expect;

describe('Counter', function() {

  var metrics;
  var sandbox;
  beforeEach(function(done) {
    sandbox = sinon.sandbox.create();
    metrics = new RedisMetrics();
    metrics.client.flushall(done);
  });

  afterEach(function(done) {
    sandbox.restore();
    metrics.client.quit(done);
  });

  describe('constructor', function() {

    it('should set some defaults', function() {
      var counter = new TimestampedCounter(metrics, 'foo');
      expect(counter.metrics).to.equal(metrics);
      expect(counter.key).to.equal('c:foo');
      expect(counter.options.timeGranularity).to.equal(0);
      expect(counter.options.expireKeys).to.equal(true);
      expect(counter.options.namespace).to.equal('c');
    });

    it('should respect the passed namespace in the key', function() {
      var counter = new TimestampedCounter(metrics, 'foo', {
        namespace: 'stats'
      });
      expect(counter.key).to.equal('stats:foo');
      expect(counter.options.namespace).to.equal('stats');
    });

    it('should reset an incorrect time granularity to "none"', function() {
      var counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: -10
      });
      expect(counter.options.timeGranularity).to.equal(0);

      counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 10
      });
      expect(counter.options.timeGranularity).to.equal(0);
    });
  });

  describe('getKeys', function() {
    it('should return an array of keys', function() {
      var counter = new TimestampedCounter(metrics, 'foo');
      var keys = counter.getKeys();
      expect(keys).to.be.instanceof(Array);
      expect(keys).to.have.length.above(0);
      expect(keys).to.include('c:foo');
    });

    it('should return keys based on the granularity level', function() {
      sandbox.useFakeTimers(new Date('2015-01-02T03:04:05Z').getTime());

      var counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'none'
      });
      var keys = counter.getKeys();
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
  });

  describe('getKeyTTL', function() {
    it('should the default for seconds', function() {
      var counter = new TimestampedCounter(metrics, 'foo');
      var key = 'c:foo:20150102030405';
      var ttl = counter.getKeyTTL(key);
      expect(ttl).to.equal(10 * 60);
    });

    it('should the default for minutes', function() {
      var counter = new TimestampedCounter(metrics, 'foo');
      var key = 'c:foo:201501020304';
      var ttl = counter.getKeyTTL(key);
      expect(ttl).to.equal(12 * 60 * 60);
    });

    it('should the default for hours', function() {
      var counter = new TimestampedCounter(metrics, 'foo');
      var key = 'c:foo:2015010203';
      var ttl = counter.getKeyTTL(key);
      expect(ttl).to.equal(31 * 24 * 60 * 60);
    });

    it('should the default for days', function() {
      var counter = new TimestampedCounter(metrics, 'foo');
      var key = 'c:foo:20150102';
      var ttl = counter.getKeyTTL(key);
      expect(ttl).to.equal(2 * 365 * 24 * 60 * 60);
    });

    it('should the default for months', function() {
      var counter = new TimestampedCounter(metrics, 'foo');
      var key = 'c:foo:201501';
      var ttl = counter.getKeyTTL(key);
      expect(ttl).to.equal(10 * 365 * 24 * 60 * 60);
    });

    it('should the default for years', function() {
      var counter = new TimestampedCounter(metrics, 'foo');
      var key = 'c:foo:2015';
      var ttl = counter.getKeyTTL(key);
      expect(ttl).to.equal(-1);
    });

    it('should the default for none', function() {
      var counter = new TimestampedCounter(metrics, 'foo');
      var key = 'c:foo';
      var ttl = counter.getKeyTTL(key);
      expect(ttl).to.equal(-1);
    });

    it('should allow the expiration to be configured', function() {
      var counter = new TimestampedCounter(metrics, 'foo', {
        expiration: {
          total: 1
        }
      });
      var key = 'c:foo';
      var ttl = counter.getKeyTTL(key);
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

    it('should return the default value if conf is missing', function() {
      var counter = new TimestampedCounter(metrics, 'foo', {
        expiration: {
          total: 10
        }
      });
      // Only total is set, yearly is not, use default of -1
      var key = 'c:foo:2015';
      var ttl = counter.getKeyTTL(key);
      expect(ttl).to.equal(-1);
    });
  });

  describe('incr', function() {
    it('should call redis without a trans when keys dont expire', function() {
      var multiSpy = sandbox.spy(metrics.client, 'multi');
      var incrSpy = sandbox.spy(metrics.client, 'incrby');

      var counter = new TimestampedCounter(
        metrics,
        'foo',
        { expireKeys: false }
      );
      return counter.incr().then(function() {
        sinon.assert.calledOnce(incrSpy);
        sinon.assert.notCalled(multiSpy);
      });
    });

    it('should call redis without a trans when counters expire', function() {
      var multiSpy = sandbox.spy(metrics.client, 'multi');
      var evalSpy = sandbox.spy(metrics.client, 'eval');

      var counter = new TimestampedCounter(metrics, 'foo', {
        expireKeys: true,
        expiration: {
          total: 10
        }
      });
      return counter.incr().then(function() {
        sinon.assert.calledOnce(evalSpy);
        sinon.assert.notCalled(multiSpy);
      });
    });

    it('should call redis when a time granularity is chosen', function() {
      var multiSpy = sandbox.spy(metrics.client, 'multi');

      var counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'year'
      });

      return counter.incr().then(function() {
        sinon.assert.calledOnce(multiSpy);
      });
    });

    it('should call the callback on success', function(done) {
      var counter = new TimestampedCounter(metrics, 'foo');
      counter.incr(function(err, result) {
        expect(err).to.equal(null);
        expect(result).to.equal(1);
        done();
      });
    });

    it('should resolve a promise on success', function(done) {
      var counter = new TimestampedCounter(metrics, 'foo');
      counter.incr().then(function(result) {
        expect(result).to.equal(1);
        done();
      })
      .catch(done);
    });

    it('should call the callback on error', function(done) {
      sandbox.stub(metrics.client, 'incrby')
        .yields(new Error('oh no'), null);

      var counter = new TimestampedCounter(
        metrics,
        'foo',
        { expireKeys: false }
      );
      counter.incr(function(err, result) {
        expect(err).to.not.equal(null);
        expect(result).to.equal(null);
        done();
      });
    });

    it('should reject the promise on error', function(done) {
      sandbox.stub(metrics.client, 'incrby')
        .yields(new Error('oh no'), null);

      var counter = new TimestampedCounter(
        metrics,
        'foo',
        { expireKeys: false }
      );
      counter.incr().then(function() {
        done(new Error('Should not be here'));
      })
      .catch(function(err) {
        expect(err).to.not.equal(null);
        done();
      });
    });

    it('should return a list of results', function(done) {
      var counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'year'
      });

      counter.incr().then(function(results) {
        expect(results).to.be.instanceof(Array);
        expect(results).to.deep.equal([1, 1]);
        done();
      })
      .catch(done);
    });

    it('should return a list of results for non-expiring keys', function(done) {
      var counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'year',
        expireKeys: false
      });

      counter.incr().then(function(results) {
        expect(results).to.be.instanceof(Array);
        expect(results).to.deep.equal([1, 1]);
        done();
      })
      .catch(done);
    });
  });

  describe('incr with event object', function() {

    it('should work with an event object when a counter expires', function() {
      var counter = new TimestampedCounter(metrics, 'foo', {
        expireKeys: true,
        expiration: {
          total: 10
        }
      });

      return counter.incr('bar').then(function(result) {
        expect(parseInt(result)).to.equal(1);
      });
    });

    it('should work with an event object and non-expiring keys', function() {
      var counter = new TimestampedCounter(
        metrics,
        'foo',
        { expireKeys: false }
      );

      return counter.incr('bar').then(function(result) {
        expect(parseInt(result)).to.equal(1);
      });
    });

    it('should work with an event, time gran, a counter that exp', function() {
      var counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'year',
        expireKeys: true,
        expiration: {
          year: 10
        }
      });

      return counter.incr('bar').then(function(results) {
        expect(utils.parseIntArray(results)).to.deep.equal([1, 1]);
      });
    });

    it('should work with an event, time gran and non-exp keys', function() {
      var counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'year',
        expireKeys: false
      });

      return counter.incr('bar').then(function(results) {
        expect(utils.parseIntArray(results)).to.deep.equal([1, 1]);
      });
    });

  });

  describe('incrby', function() {
    it('should call redis without a tran when keys do not exp', function(done) {
      var multiSpy = sandbox.spy(metrics.client, 'multi');
      var incrSpy = sandbox.spy(metrics.client, 'incrby');

      var counter = new TimestampedCounter(
        metrics,
        'foo',
        { expireKeys: false }
      );
      counter.incrby(2).then(function() {
        sinon.assert.calledOnce(incrSpy);
        sinon.assert.notCalled(multiSpy);
        done();
      })
      .catch(done);
    });

    it('should call redis without a trans when counters exp', function(done) {
      var multiSpy = sandbox.spy(metrics.client, 'multi');
      var evalSpy = sandbox.spy(metrics.client, 'eval');

      var counter = new TimestampedCounter(metrics, 'foo', {
        expireKeys: true,
        expiration: {
          total: 10
        }
      });
      counter.incrby(2).then(function() {
        sinon.assert.calledOnce(evalSpy);
        sinon.assert.notCalled(multiSpy);
        done();
      })
      .catch(done);
    });

    it('should call redis when a time granularity is chosen', function(done) {
      var multiSpy = sandbox.spy(metrics.client, 'multi');

      var counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'year'
      });

      counter.incrby(3).then(function() {
        sinon.assert.calledOnce(multiSpy);
        done();
      })
      .catch(done);
    });

    it('should call the callback on success', function(done) {
      var counter = new TimestampedCounter(metrics, 'foo');
      counter.incrby(4, function(err, result) {
        expect(err).to.equal(null);
        expect(result).to.equal(4);
        done();
      });
    });

    it('should resolve a promise on success', function(done) {
      var counter = new TimestampedCounter(metrics, 'foo');
      counter.incrby(5).then(function(result) {
        expect(result).to.equal(5);
        done();
      })
      .catch(done);
    });

    it('should call the callback on error from incrby', function(done) {
      sandbox.stub(metrics.client, 'incrby')
        .yields(new Error('oh no'), null);

      var counter = new TimestampedCounter(
        metrics,
        'foo',
        { expireKeys: false }
      );
      counter.incrby(6, function(err, result) {
        expect(err).to.not.equal(null);
        expect(result).to.equal(null);
        done();
      });
    });

    it('should call the callback on error from eval', function(done) {
      sandbox.stub(metrics.client, 'eval')
        .yields(new Error('oh no'), null);

      var counter = new TimestampedCounter(metrics, 'foo', {
        expireKeys: true,
        expiration: {
          total: 10
        }
      });
      counter.incrby(6, function(err, result) {
        expect(err).to.not.equal(null);
        expect(result).to.equal(null);
        done();
      });
    });

    it('should reject the promise on redis error from incrby', function(done) {
      sandbox.stub(metrics.client, 'incrby')
        .yields(new Error('oh no'), null);

      var counter = new TimestampedCounter(
        metrics,
        'foo',
        { expireKeys: false }
      );
      counter.incrby(7).then(function() {
        done(new Error('Should not be here'));
      })
      .catch(function(err) {
        expect(err).to.not.equal(null);
        done();
      });
    });

    it('should reject the promise on redis error with eval', function(done) {
      sandbox.stub(metrics.client, 'eval')
        .yields(new Error('oh no'), null);

      var counter = new TimestampedCounter(metrics, 'foo', {
        expireKeys: true,
        expiration: {
          total: 10
        }
      });
      counter.incrby(7).then(function() {
        done(new Error('Should not be here'));
      })
      .catch(function(err) {
        expect(err).to.not.equal(null);
        done();
      });
    });

    it('should return a list of results from the operation', function(done) {
      var counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'year'
      });

      counter.incrby(8).then(function(results) {
        expect(results).to.be.instanceof(Array);
        expect(results).to.deep.equal([8, 8]);
        done();
      })
      .catch(done);
    });
  });

  describe('incrby with event object', function() {

    it('should work with an event object', function(done) {
      var counter = new TimestampedCounter(metrics, 'foo');

      counter.incrby(9, 'bar').then(function(result) {
        expect(parseInt(result)).to.equal(9);
        done();
      })
      .catch(done);
    });

    it('should work with an event object and time granularity', function(done) {
      var counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'year'
      });

      counter.incrby(10, 'bar').then(function(results) {
        expect(utils.parseIntArray(results)).to.deep.equal([10, 10]);
        done();
      })
      .catch(done);
    });

  });

  describe('count', function() {
    it('should work with callback', function(done) {
      var mock = sandbox.mock(metrics.client)
        .expects('get')
        .once()
        .yields(null, '10');

      var counter = new TimestampedCounter(metrics, 'foo');
      counter.count(function(err, result) {
        mock.verify();
        expect(result).to.equal(10);
        done(err);
      });
    });

    it('should work with time granularity and callback', function(done) {
      var mock = sandbox.mock(metrics.client)
        .expects('get')
        .once()
        .yields(null, '10');

      var counter = new TimestampedCounter(metrics, 'foo');
      counter.count('none', function(err, result) {
        mock.verify();
        expect(result).to.equal(10);
        done(err);
      });
    });

    it('should work with time gran, event object and callback', function(done) {
      var mock = sandbox.mock(metrics.client)
        .expects('zscore')
        .once()
        .yields(null, '10');

      var counter = new TimestampedCounter(metrics, 'foo');
      counter.count('none', 'bar', function(err, result) {
        mock.verify();
        expect(result).to.equal(10);
        done(err);
      });
    });

    it('should return a single res when no arg are given', function(done) {
      var counter = new TimestampedCounter(metrics, 'foo');
      counter.incr()
        .then(function() {
          return counter.incr();
        })
        .then(function() {
          return counter.incr();
        })
        .then(function() {
          return counter.count();
        })
        .then(function(result) {
          // Counter has been incremented 3 times.
          expect(result).to.equal(3);
          done();
        })
        .catch(done);
    });

    it('should return a single result for an event object', function(done) {
      var counter = new TimestampedCounter(metrics, 'foo');
      counter.incr('bar')
        .then(function() {
          return counter.incr('bar');
        })
        .then(function() {
          return counter.incr('bar');
        })
        .then(function() {
          return counter.count('total', 'bar');
        })
        .then(function(result) {
          // Counter has been incremented 3 times.
          expect(result).to.equal(3);
          done();
        })
        .catch(done);
    });

    it('should return 0 when the key does not exist (cb)', function(done) {
      var mock = sandbox.mock(metrics.client)
        .expects('get')
        .once()
        .yields(null, null);

      var counter = new TimestampedCounter(metrics, 'foo');
      counter.count(function(err, result) {
        mock.verify();
        expect(result).to.equal(0);
        done(err);
      });
    });

    it('should return 0 when the key does not exist (promise)', function(done) {
      var mock = sandbox.mock(metrics.client)
        .expects('get')
        .once()
        .yields(null, null);

      var counter = new TimestampedCounter(metrics, 'foo');
      counter.count()
        .then(function(result) {
          mock.verify();
          expect(result).to.equal(0);
          done();
        })
        .catch(done);
    });

    it('should return a count for a specific time granularity', function(done) {
      var counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'year'
      });

      // Increment in 2014 and 2015
      // Total should be 2 but year should be 1.

      var clock = sandbox.useFakeTimers(new Date('2014-02-01').getTime());
      counter.incr()
        .then(function() {
          clock.tick(1000 * 60 * 60 * 24 * 365);
          return counter.incr();
        })
        .then(function() {
          return counter.count('none'); // same as counter.count();
        })
        .then(function(result) {
          expect(result).to.equal(2);
          return counter.count('year');
        })
        .then(function(result) {
          expect(result).to.equal(1);
          done();
        })
        .catch(done);
    });

    it('should return a count for a specific time gran and event', function() {
      var counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'year'
      });

      // Increment in 2014 and 2015
      // Total should be 2 but year should be 1.

      var clock = sandbox.useFakeTimers(new Date('2014-02-01').getTime());
      return counter.incr('bar')
        .then(function() {
          clock.tick(1000 * 60 * 60 * 24 * 365);
          return counter.incr('bar');
        })
        .then(function() {
          return counter.count('none', 'bar');
        })
        .then(function(result) {
          expect(result).to.equal(2);
          return counter.count('year', 'bar');
        })
        .then(function(result) {
          expect(result).to.equal(1);
        });
    });
  });

  describe('countRange', function() {
    it('should return a range of counts', function(done) {
      var counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'year'
      });

      // Increment 2014 once and 2015 twice

      var start = moment.utc({ year: 2014 });
      var end = moment.utc({ year: 2015 });
      var expected = {};
      expected[start.format()] = 1;
      expected[end.format()] = 2;

      var clock = sandbox.useFakeTimers(new Date('2014-02-01').getTime());
      counter.incr()
        .then(function() {
          clock.tick(1000 * 60 * 60 * 24 * 365);
          return counter.incr();
        })
        .then(function() {
          return counter.incr();
        })
        .then(function() {
          return counter.countRange('year', start, end);
        })
        .then(function(result) {
          // Check promise
          expect(result).to.deep.equal(expected);

          // Check callback
          counter.countRange('year', start, end, function(err, res) {
            expect(res).to.deep.equal(expected);
            done();
          });
        })
        .catch(done);
    });

    it('should return a range of counts at the second level', function(done) {
      var counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'second'
      });

      // Increment 2014 once and 2015 twice

      var start = moment.utc({ year: 2015, second: 0 });
      var end = moment.utc({ year: 2015, second: 1 });
      var expected = {};
      expected[start.format()] = 1;
      expected[end.format()] = 2;

      var clock = sandbox.useFakeTimers(new Date('2015-01-01').getTime());
      counter.incr()
        .then(function() {
          clock.tick(1000);
          return counter.incr();
        })
        .then(function() {
          return counter.incr();
        })
        .then(function() {
          return counter.countRange('second', start, end);
        })
        .then(function(result) {
          // Check promise
          expect(result).to.deep.equal(expected);

          // Check callback
          counter.countRange('second', start, end, function(err, res) {
            expect(res).to.deep.equal(expected);
            done();
          });
        })
        .catch(done);
    });

    it('should return a range of count for an event object', function(done) {
      var counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'year'
      });

      // Increment 2014 once and 2015 twice

      var start = moment.utc({ year: 2014 });
      var end = moment.utc({ year: 2015 });
      var expected = {};
      expected[start.format()] = 1;
      expected[end.format()] = 2;

      var clock = sandbox.useFakeTimers(new Date('2014-02-01').getTime());
      counter.incr('bar')
        .then(function() {
          clock.tick(1000 * 60 * 60 * 24 * 365);
          return counter.incr('bar');
        })
        .then(function() {
          return counter.incr('bar');
        })
        .then(function() {
          return counter.countRange('year', start, end, 'bar');
        })
        .then(function(result) {
          // Check promise
          expect(result).to.deep.equal(expected);

          // Check callback
          counter.countRange('year', start, end, 'bar', function(err, res) {
            expect(res).to.deep.equal(expected);
            done();
          });
        })
        .catch(done);
    });

    it('should use current date if no end date is provided', function(done) {
      var counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'year'
      });

      // Increment 2014 once and 2015 twice

      var start = moment.utc({ year: 2014 });
      var expected = {};
      expected[start.format()] = 1;
      expected[moment.utc(start).add(1, 'years').format()] = 2;

      var clock = sandbox.useFakeTimers(new Date('2014-02-01').getTime());
      counter.incr()
        .then(function() {
          clock.tick(1000 * 60 * 60 * 24 * 365);
          return counter.incr();
        })
        .then(function() {
          return counter.incr();
        })
        .then(function() {
          return counter.countRange('year', start);
        })
        .then(function(result) {
          // Check promise
          expect(result).to.deep.equal(expected);

          // Check callback
          counter.countRange('year', start, function(err, res) {
            expect(res).to.deep.equal(expected);
            done();
          });
        })
        .catch(done);
    });

    it('should return 0 for counters where no data registed', function(done) {
      var counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'year'
      });

      // Increment 2014 once.

      var start = moment.utc({ year: 2014 });
      var end = moment.utc({ year: 2015 });
      var expected = {};
      expected[start.format()] = 1;
      expected[end.format()] = 0;

      sandbox.useFakeTimers(new Date('2014-02-01').getTime());
      counter.incr()
        .then(function() {
          return counter.countRange('year', start, end);
        })
        .then(function(result) {
          // Check promise
          expect(result).to.deep.equal(expected);

          // Check callback
          counter.countRange('year', start, end, function(err, res) {
            expect(res).to.deep.equal(expected);
            done();
          });
        })
        .catch(done);
    });

    it('should return 0 for counters where no data is reg', function(done) {
      var counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'year'
      });

      // Increment 2014 once.

      var start = moment.utc({ year: 2014 });
      var end = moment.utc({ year: 2015 });
      var expected = {};
      expected[start.format()] = 1;
      expected[end.format()] = 0;

      sandbox.useFakeTimers(new Date('2014-02-01').getTime());
      counter.incr('bar')
        .then(function() {
          return counter.countRange('year', start, end, 'bar');
        })
        .then(function(result) {
          // Check promise
          expect(result).to.deep.equal(expected);

          // Check callback
          counter.countRange('year', start, end, 'bar', function(err, res) {
            expect(res).to.deep.equal(expected);
            done();
          });
        })
        .catch(done);
    });

    it('should accept strings for date range', function(done) {
      var counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'year'
      });

      // Increment 2014 once and 2015 twice

      var start = moment.utc({ year: 2014 });
      var end = moment.utc({ year: 2015 });
      var expected = {};
      expected[start.format()] = 1;
      expected[end.format()] = 2;

      var startStr = '2014-01-01T00:00:00Z';
      var endStr = '2015-01-01T00:00:00Z';

      var clock = sandbox.useFakeTimers(new Date('2014-02-01').getTime());
      counter.incr()
        .then(function() {
          clock.tick(1000 * 60 * 60 * 24 * 365);
          return counter.incr();
        })
        .then(function() {
          return counter.incr();
        })
        .then(function() {
          return counter.countRange('year', startStr, endStr);
        })
        .then(function(result) {
          // Check promise
          expect(result).to.deep.equal(expected);

          // Check callback
          counter.countRange('year', startStr, endStr, function(err, res) {
            expect(res).to.deep.equal(expected);
            done();
          });
        })
        .catch(done);
    });

    it('should accept numbers for date range', function(done) {
      var counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'year'
      });

      // Increment 2014 once and 2015 twice

      var start = moment.utc({ year: 2014 });
      var end = moment.utc({ year: 2015 });
      var expected = {};
      expected[start.format()] = 1;
      expected[end.format()] = 2;

      var startNum = start.valueOf();
      var endNum = end.valueOf();

      var clock = sandbox.useFakeTimers(new Date('2014-02-01').getTime());
      counter.incr()
        .then(function() {
          clock.tick(1000 * 60 * 60 * 24 * 365);
          return counter.incr();
        })
        .then(function() {
          return counter.incr();
        })
        .then(function() {
          return counter.countRange('year', startNum, endNum);
        })
        .then(function(result) {
          // Check promise
          expect(result).to.deep.equal(expected);

          // Check callback
          counter.countRange('year', startNum, endNum, function(err, res) {
            expect(res).to.deep.equal(expected);
            done();
          });
        })
        .catch(done);
    });

    it('should return a single num if total gran is selected', function(done) {
      var counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'year'
      });

      // Increment 2014 once and 2015 twice => total is 3

      var start = moment.utc({ year: 2014 });
      var end = moment.utc({ year: 2015 });

      var clock = sandbox.useFakeTimers(new Date('2014-02-01').getTime());
      counter.incr()
        .then(function() {
          clock.tick(1000 * 60 * 60 * 24 * 365);
          return counter.incr();
        })
        .then(function() {
          return counter.incr();
        })
        .then(function() {
          return counter.countRange('total', start, end);
        })
        .then(function(result) {
          // Check promise
          expect(result).to.equal(3);

          // Check callback
          counter.countRange('total', start, end, function(err, res) {
            expect(res).to.equal(3);
            done();
          });
        })
        .catch(done);
    });

    it('should throw an exc if countRange is used on a counter', function() {
      var counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'total'
      });

      var throwClosure = function() {
        counter.countRange('total', '2015', '2016');
      };
      expect(throwClosure).to.throw(Error);
    });
  });

  describe('incr with expiration', function() {
    it('should set a ttl for a key', function(done) {
      var counter = new TimestampedCounter(metrics, 'foo', {
        expireKeys: true,
        expiration: {
          total: 100
        }
      });

      counter.incr().then(function() {
        var key = counter.getKeys()[0];
        metrics.client.ttl(key, function(err, ttl) {
          expect(err).to.equal(null);
          expect(ttl).to.be.above(0);
          expect(ttl).to.be.most(100);
          done();
        });
      })
      .catch(done);
    });

    it('should set a ttl for a key with event objects', function(done) {
      var counter = new TimestampedCounter(metrics, 'foo', {
        expireKeys: true,
        expiration: {
          total: 100
        }
      });

      counter.incr('bar').then(function() {
        var key = counter.getKeys()[0] + ':z';
        metrics.client.ttl(key, function(err, ttl) {
          expect(err).to.equal(null);
          expect(ttl).to.be.above(0);
          expect(ttl).to.be.most(100);
          done();
        });
      })
      .catch(done);
    });

    it('should not renew the ttl on the second call', function(done) {
      var counter = new TimestampedCounter(metrics, 'foo', {
        expireKeys: true,
        expiration: {
          total: 100
        }
      });

      counter.incr().then(function() {
        var key = counter.getKeys()[0];
        metrics.client.ttl(key, function(err, ttl) {
          setTimeout(function() {
            counter.incr().then(function() {
              metrics.client.ttl(key, function(err2, ttl2) {
                // Expect that ttl has decreased.
                console.log(ttl, ttl2);
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

    it('should not renew the ttl on the second call', function(done) {
      var counter = new TimestampedCounter(metrics, 'foo', {
        expireKeys: true,
        expiration: {
          total: 100
        }
      });

      counter.incr('bar').then(function() {
        var key = counter.getKeys()[0] + ':z';
        metrics.client.ttl(key, function(err, ttl) {
          setTimeout(function() {
            counter.incr('bar').then(function() {
              metrics.client.ttl(key, function(err2, ttl2) {
                // Expect that ttl has decreased.
                console.log(ttl, ttl2);
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

  describe('incrby with expiration', function() {
    it('should set a ttl for a key', function(done) {
      var counter = new TimestampedCounter(metrics, 'foo', {
        expireKeys: true,
        expiration: {
          total: 60 // Gone in 60 seconds :-)
        }
      });

      counter.incrby(10).then(function() {
        var key = counter.getKeys()[0];
        metrics.client.ttl(key, function(err, ttl) {
          expect(err).to.equal(null);
          expect(ttl).to.be.above(0);
          expect(ttl).to.be.most(60);
          done();
        });
      })
      .catch(done);
    });

    it('should set a ttl for a key with event objects', function(done) {
      var counter = new TimestampedCounter(metrics, 'foo', {
        expireKeys: true,
        expiration: {
          total: 100
        }
      });

      counter.incrby(10, 'bar').then(function() {
        var key = counter.getKeys()[0] + ':z';
        metrics.client.ttl(key, function(err, ttl) {
          expect(err).to.equal(null);
          expect(ttl).to.be.above(0);
          expect(ttl).to.be.most(100);
          done();
        });
      })
      .catch(done);
    });
  });

  describe('top', function() {
    it('should work with callback', function(done) {
      var mock = sandbox.mock(metrics.client)
        .expects('zrevrange')
        .once()
        .withArgs('c:foo:z', 0, -1, 'WITHSCORES')
        .yields(null, ['foo', '39', 'bar', '13']);

      var counter = new TimestampedCounter(metrics, 'foo');
      counter.top('foo', function(err, results) {
        mock.verify();
        expect(results).to.have.length(2);
        expect(results[0]).to.have.property('foo');
        expect(results[0].foo).to.equal(39);
        done(err);
      });
    });

    it('should work with promises', function(done) {
      var mock = sandbox.mock(metrics.client)
        .expects('zrevrange')
        .once()
        .yields(null, ['foo', '39', 'bar', '13']);

      var counter = new TimestampedCounter(metrics, 'foo');
      counter.top('foo')
        .then(function(results) {
          mock.verify();
          expect(results).to.have.length(2);
          expect(results[0]).to.have.property('foo');
          expect(results[0].foo).to.equal(39);
          done();
        })
        .catch(done);
    });

    it('should accept a startingAt argument', function(done) {
      var mock = sandbox.mock(metrics.client)
        .expects('zrevrange')
        .once()
        .withArgs('c:foo:z', 10, -1, 'WITHSCORES')
        .yields(null, ['foo', '39', 'bar', '13']);

      var counter = new TimestampedCounter(metrics, 'foo');
      counter.top('foo', 'desc', 10, function(err) {
        mock.verify();
        done(err);
      });
    });

    it('should accept a startingAt and a limit argument', function(done) {
      var mock = sandbox.mock(metrics.client)
        .expects('zrevrange')
        .once()
        .withArgs('c:foo:z', 10, 15, 'WITHSCORES')
        .yields(null, ['foo', '39', 'bar', '13']);

      var counter = new TimestampedCounter(metrics, 'foo');
      counter.top('foo', 'desc', 10, 15, function(err) {
        mock.verify();
        done(err);
      });
    });

    it('should accept a direction argument with asc value', function(done) {
      var mock = sandbox.mock(metrics.client)
        .expects('zrange')
        .once()
        .withArgs('c:foo:z', 0, -1, 'WITHSCORES')
        .yields(null, ['foo', '39', 'bar', '13']);

      var counter = new TimestampedCounter(metrics, 'foo');
      counter.top('foo', 'asc', function(err) {
        mock.verify();
        done(err);
      });
    });

    it('should throw an exception if the direction argument is not correct',
      function(done) {
        var counter = new TimestampedCounter(metrics, 'foo');
        try {
          counter.top('foo', 'dummy');
        } catch (e) {
          return done();
        }

        throw new Error('This should never be called.');
      });
  });
});
