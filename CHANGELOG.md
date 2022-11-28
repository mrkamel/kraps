# CHANGELOG

## v0.7.0

* Added a `jobs` option to the actions to limit the concurrency
  when e.g. accessing external data stores and to avoid overloading
  them
* Added a queue using redis for the jobs to avoid starving workers
* Removed `distributed_job` dependency

## v0.6.0

* Added `map_partitions`
* Added `combine`
* Added `dump` and `load`

## v0.5.0

* Added a `before` option to specify a callable to run before
  a step to e.g. populate caches upfront, etc.

## v0.4.0

* Pre-reduce in a map step when the subsequent step is a
  reduce step

## v0.3.0

* Changed partitioners to receive the number of partitions
  as second parameter

## v0.2.0

* Updated map-reduce-ruby to allow concurrent uploads
