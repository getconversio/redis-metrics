'use strict';

/**
 * Useful for lua scripts for Redis
 */

module.exports = {
  incrbyExpire: "local v = redis.call('INCRBY', KEYS[1], ARGV[1]) if tostring(v) == tostring(ARGV[1]) then redis.call('EXPIRE', KEYS[1], ARGV[2]) end return v",
  zincrbyExpire: "local v = redis.call('ZINCRBY', KEYS[1], ARGV[1], ARGV[2]) if tostring(v) == tostring(ARGV[1]) then redis.call('EXPIRE', KEYS[1], ARGV[3]) end return v"
};
