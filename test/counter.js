'use strict';

var chai = require('chai'),
    expect = chai.expect,
    assert = chai.assert,
    sinon = require('sinon'),
    redis = require('redis'),
    RedisMetrics = require('../lib/metrics'),
    TimestampedCounter = require('../lib/counter');

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

  describe('Constructor', function() {

    it('should set sane defaults', function() {
      var counter = new TimestampedCounter(metrics, 'foo');
      expect(counter.key).to.equal('foo');
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
      expect(keys).to.include('foo');
    });

    it('should return keys based on the granularity level', function() {
      sandbox.useFakeTimers(Date.parse('2015-01-02T03:04:05Z'));

      var counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'none'
      });
      var keys = counter.getKeys();
      expect(keys).to.be.instanceof(Array);
      expect(keys).to.have.length(1);
      expect(keys).to.include('foo');

      counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'year'
      });
      keys = counter.getKeys();
      expect(keys).to.be.instanceof(Array);
      expect(keys).to.have.length(2);
      expect(keys).to.include('foo');
      expect(keys).to.include('foo:2015');

      counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'month'
      });
      keys = counter.getKeys();
      expect(keys).to.be.instanceof(Array);
      expect(keys).to.have.length(3);
      expect(keys).to.include('foo');
      expect(keys).to.include('foo:2015');
      expect(keys).to.include('foo:201501');

      counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'day'
      });
      keys = counter.getKeys();
      expect(keys).to.be.instanceof(Array);
      expect(keys).to.have.length(4);
      expect(keys).to.include('foo');
      expect(keys).to.include('foo:2015');
      expect(keys).to.include('foo:201501');
      expect(keys).to.include('foo:20150102');

      counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'hour'
      });
      keys = counter.getKeys();
      expect(keys).to.be.instanceof(Array);
      expect(keys).to.have.length(5);
      expect(keys).to.include('foo');
      expect(keys).to.include('foo:2015');
      expect(keys).to.include('foo:201501');
      expect(keys).to.include('foo:2015010203');

      counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'minute'
      });
      keys = counter.getKeys();
      expect(keys).to.be.instanceof(Array);
      expect(keys).to.have.length(6);
      expect(keys).to.include('foo');
      expect(keys).to.include('foo:2015');
      expect(keys).to.include('foo:201501');
      expect(keys).to.include('foo:2015010203');
      expect(keys).to.include('foo:201501020304');

      counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'second'
      });
      keys = counter.getKeys();
      expect(keys).to.be.instanceof(Array);
      expect(keys).to.have.length(7);
      expect(keys).to.include('foo');
      expect(keys).to.include('foo:2015');
      expect(keys).to.include('foo:201501');
      expect(keys).to.include('foo:2015010203');
      expect(keys).to.include('foo:201501020304');
      expect(keys).to.include('foo:20150102030405');
    });
  });

  describe('incr', function() {
    it('should call redis incr without a transaction when no time granularity is chosen', function(done) {
      var multiSpy = sandbox.spy(metrics.client, 'multi');
      var incrSpy = sandbox.spy(metrics.client, 'incr');

      var counter = new TimestampedCounter(metrics, 'foo');
      counter.incr().then(function() {
        sinon.assert.calledOnce(incrSpy);
        sinon.assert.notCalled(multiSpy);
        done();
      })
      .catch(done);
    });

    it('should call redis incr with a transaction when a time granularity is chosen', function(done) {
      var multi
      var multiSpy = sandbox.spy(metrics.client, 'multi');

      var counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 1
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
      var mock = sandbox.stub(metrics.client, 'incr')
        .yields(new Error('oh no'), null);

      var counter = new TimestampedCounter(metrics, 'foo');
      counter.incr(function(err, result) {
        expect(err).to.not.be.null;
        expect(result).to.be.null;
        done();
      });
    });

    it('should reject the promise on error', function(done) {
      var mock = sandbox.stub(metrics.client, 'incr')
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
        timeGranularity: 1
      });

      counter.incr().then(function(results) {
        expect(results).to.be.instanceof(Array);
        expect(results).to.deep.equal([1, 1]);
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
  });

  describe('countRange', function() {
    it('should return a range of counts for a specific time granularity', function(done) {
      var counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'year'
      });

      // Increment in 2014 and 2015
      // Year granularity should return two values for a start and end date
      // that use 2014-2015

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
          return counter.countRange('year', { year: 2014 }, { year: 2015 });
        })
        .then(function(result) {
          expect(result).to.deep.equal([1, 2]);
          done();
        })
        .catch(done);
    });

    it('should use current timestamp if no end date is provided', function(done) {
      var counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'year'
      });

      // Increment in 2014 and 2015
      // Year granularity should return two values for a start and end date
      // that use 2014-2015

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
          return counter.countRange('year', { year: 2014 });
        })
        .then(function(result) {
          expect(result).to.deep.equal([1, 2]);
          done();
        })
        .catch(done);
    });

    it('should return 0 for counters where no data is registed', function(done) {
      var counter = new TimestampedCounter(metrics, 'foo', {
        timeGranularity: 'year'
      });

      // Increment in 2014
      // Year granularity should return two values for a start and end date
      // that are in this year.

      var clock = sandbox.useFakeTimers(new Date('2014-02-01').getTime());
      counter.incr()
        .then(function() {
          return counter.countRange('year', { year: 2014 }, { year: 2015 });
        })
        .then(function(result) {
          expect(result).to.deep.equal([1, 0]);
          done();
        })
        .catch(done);
    });
  });
});
