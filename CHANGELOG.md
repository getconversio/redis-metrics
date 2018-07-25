# Changelog

## v2.0.0

### Breaking Changes

* Removed support for Node 4
* Removed callback interface
* Changed counter method interfaces from optional arguments to an options argument, where applicable:
  * `incr` receives an option object with the `eventObj` as a key.
  * `incrby` same as `incr` on the last argument.
  * `count` receives all previous arguments as a single option object.
  * `countRange` still gets `timeGranularity` as a first argument, but range dates are passed on a single second argument, and the `eventObj` is passed within an optional third argument object.
  * `top` receives all previous arguments as a single option object.
  * `topRange` same as `countRange` for range dates, and all arguments go onto the last optional argument.
  * `trimEvents` same as `count`.
  * `zero` same as `incr`.

### Other Changes

* Changed linting tools and configs
