'use strict';

var chai = require('chai'),
    expect = chai.expect,
    assert = chai.assert,
    sinon = require('sinon'),
    redis = require('redis'),
    moment = require('moment'),
    RedisMetrics = require('../lib/metrics'),
    TimestampedCounter = require('../lib/counter'),
    utils = require('../lib/utils');

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
      expect(counter.key).to.equal('c:foo');
      expect(counter.metrics).to.equal(metrics);
      expect(counter.options.timeGranularity).to.equal(0);
    });

    it('should reset an incorrect time granularity to "none"', function() {
      var counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: -10
      });
      expect(counter.options.timeGranularity).to.equal(0);

      var counter = new TimestampedCounter(metrics, 'foo', {
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

  describe('incr', function() {
    it('should call redis incrby without a transaction when no time granularity is chosen', function(done) {
      var multiSpy = sandbox.spy(metrics.client, 'multi');
      var incrSpy = sandbox.spy(metrics.client, 'incrby');

      var counter = new TimestampedCounter(metrics, 'foo');
      counter.incr().then(function() {
        sinon.assert.calledOnce(incrSpy);
        sinon.assert.notCalled(multiSpy);
        done();
      })
      .catch(done);
    });

    it('should call redis with a transaction when a time granularity is chosen', function(done) {
      var multiSpy = sandbox.spy(metrics.client, 'multi');

      var counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'year'
      });

      counter.incr().then(function() {
        sinon.assert.calledOnce(multiSpy);
        done();
      })
      .catch(done);
    });

    it('should call the callback on success', function(done) {
      var counter = new TimestampedCounter(metrics, 'foo');
      counter.incr(function(err, result) {
        expect(err).to.be.null;
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
      var mock = sandbox.stub(metrics.client, 'incrby')
        .yields(new Error('oh no'), null);

      var counter = new TimestampedCounter(metrics, 'foo');
      counter.incr(function(err, result) {
        expect(err).to.not.be.null;
        expect(result).to.be.null;
        done();
      });
    });

    it('should reject the promise on error', function(done) {
      var mock = sandbox.stub(metrics.client, 'incrby')
        .yields(new Error('oh no'), null);

      var counter = new TimestampedCounter(metrics, 'foo');
      counter.incr().then(function() {
        done(new Error('Should not be here'));
      })
      .catch(function(err) {
        expect(err).to.not.be.null;
        done();
      });
    });

    it('should return a list of results from the operation', function(done) {
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

    it('should work with an event object', function(done) {
      var counter = new TimestampedCounter(metrics, 'foo');

      counter.incr('bar').then(function(result) {
        expect(parseInt(result)).to.equal(1);
        done();
      })
      .catch(done);
    });

    it('should work with an event object and time granularity', function(done) {
      var counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'year'
      });

      counter.incr('bar').then(function(results) {
        expect(utils.parseIntArray(results)).to.deep.equal([1, 1]);
        done();
      })
      .catch(done);
    });

  });

  describe('incrby', function() {
    it('should call redis incrby without a transaction when no time granularity is chosen', function(done) {
      var multiSpy = sandbox.spy(metrics.client, 'multi');
      var incrSpy = sandbox.spy(metrics.client, 'incrby');

      var counter = new TimestampedCounter(metrics, 'foo');
      counter.incrby(2).then(function() {
        sinon.assert.calledOnce(incrSpy);
        sinon.assert.notCalled(multiSpy);
        done();
      })
      .catch(done);
    });

    it('should call redis with a transaction when a time granularity is chosen', function(done) {
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
        expect(err).to.be.null;
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

    it('should call the callback on error', function(done) {
      var mock = sandbox.stub(metrics.client, 'incrby')
        .yields(new Error('oh no'), null);

      var counter = new TimestampedCounter(metrics, 'foo');
      counter.incrby(6, function(err, result) {
        expect(err).to.not.be.null;
        expect(result).to.be.null;
        done();
      });
    });

    it('should reject the promise on error', function(done) {
      var mock = sandbox.stub(metrics.client, 'incrby')
        .yields(new Error('oh no'), null);

      var counter = new TimestampedCounter(metrics, 'foo');
      counter.incrby(7).then(function() {
        done(new Error('Should not be here'));
      })
      .catch(function(err) {
        expect(err).to.not.be.null;
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

    it('should work with time granularity, event object and callback', function(done) {
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

    it('should return a single result when no arguments are given', function(done) {
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

    it('should return 0 when the key does not exist (callback)', function(done) {
      var mock = sandbox.mock(metrics.client)
        .expects('get')
        .once()
        .yields(null, null);

      var counter = new TimestampedCounter(metrics, 'foo');
      counter.count(function(err, result) {
        mock.verify();
        expect(result).to.equal(0);
        done(err);
      })
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
          clock.tick(1000*60*60*24*365);
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

    it('should return a count for a specific time granularity and event object', function(done) {
      var counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'year'
      });

      // Increment in 2014 and 2015
      // Total should be 2 but year should be 1.

      var clock = sandbox.useFakeTimers(new Date('2014-02-01').getTime());
      counter.incr('bar')
        .then(function() {
          clock.tick(1000*60*60*24*365);
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
          done();
        })
        .catch(done);
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
          clock.tick(1000*60*60*24*365);
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
          clock.tick(1000*60*60*24*365);
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

    it('should use current timestamp if no end date is provided', function(done) {
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
          clock.tick(1000*60*60*24*365);
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

    it('should return 0 for counters where no data is registed', function(done) {
      var counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'year'
      });

      // Increment 2014 once.

      var start = moment.utc({ year: 2014 });
      var end = moment.utc({ year: 2015 });
      var expected = {};
      expected[start.format()] = 1;
      expected[end.format()] = 0;

      var clock = sandbox.useFakeTimers(new Date('2014-02-01').getTime());
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

    it('should return 0 for counters where no data is registed for the event object', function(done) {
      var counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'year'
      });

      // Increment 2014 once.

      var start = moment.utc({ year: 2014 });
      var end = moment.utc({ year: 2015 });
      var expected = {};
      expected[start.format()] = 1;
      expected[end.format()] = 0;

      var clock = sandbox.useFakeTimers(new Date('2014-02-01').getTime());
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
          clock.tick(1000*60*60*24*365);
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
          clock.tick(1000*60*60*24*365);
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
  });
});
