# Kraps

**Easily process big data in ruby**

Kraps allows to process and perform calculations on very large datasets in
parallel using a map/reduce framework and runs on a background job framework
you already have. You just need some space on your filesystem, S3 as a storage
layer with temporary lifecycle policy enabled, the already mentioned background
job framework (like sidekiq, shoryuken, etc) and redis to keep track of the
progress. Most things you most likely already have in place anyways.

## Installation

Install the gem and add to the application's Gemfile by executing:

    $ bundle add kraps

If bundler is not being used to manage dependencies, install the gem by executing:

    $ gem install kraps

## Usage

The first thing you need to do is to tell Kraps about your desired
configuration in an initializer for example:

```ruby
Kraps.configure(
  driver: Kraps::Drivers::S3Driver.new(s3_client: Aws::S3::Client.new("..."), bucket: "some-bucket", prefix: "temp/kraps/"),
  redis: Redis.new,
  namespace: "my-application", # An optional namespace to be used for redis keys, default: nil
  job_ttl: 24.hours, # Job information in redis will automatically be removed after this amount of time, default: 24 hours
  show_progress: true # Whether or not to show the progress in the terminal when executing jobs, default: true
  enqueuer: ->(worker, json) { worker.perform_async(json) } # Allows to customize the enqueueing of worker jobs
)
```

Afterwards, create a job class, which tells Kraps what your job should do.
Therefore, you create some class with a `call` method, and optionally some
arguments. Let's create a simple job, which reads search log files to analyze
how often search queries have been searched:

```ruby
class SearchLogCounter
  def call(start_date:, end_date:)
    job = Kraps::Job.new(worker: MyKrapsWorker)

    job = job.parallelize(partitions: 128) do |collector|
      (Date.parse(start_date)..Date.parse(end_date)).each do |date|
        collector.call(date.to_s)
      end
    end

    job = job.map do |date, _, collector|
      # fetch log file for the date from e.g. s3

      File.open(logfile).each_line do |line|
        data = JSON.parse(line)

        collector.call(data["q"], 1)
      end
    end

    job = job.reduce do |_, count1, count2|
      count1 + count2
    end

    job = job.each_partition do |partition, pairs|
      tempfile = Tempfile.new

      pairs.each do |q, count|
        tempfile.puts(JSON.generate(q: q, count: count))
      end

      # store tempfile on e.g. s3
    ensure
      tempfile.close(true)
    end

    job
  end
end
```

Please note that this represents a specification of your job. It should be as
free as possible from side effects, because your background jobs must also be
able to take this specification to be told what to do as Kraps will run the job
with maximum concurrency.

Next thing you need to do: create the background worker which runs arbitrary
Kraps job steps. Assuming you have sidekiq in place:

```ruby
class MyKrapsWorker
  include Sidekiq::Worker

  def perform(json)
    Kraps::Worker.new(json, memory_limit: 16.megabytes, chunk_limit: 64, concurrency: 8).call(retries: 3)
  end
end
```

The `json` argument is automatically enqueued by Kraps and contains everything
it needs to know about the job and step to execute. The `memory_limit` tells
Kraps how much memory it is allowed to allocate for temporary chunks. More
concretely, it tells Kraps how big the file size of a temporary chunk can grow
in memory up until Kraps must write it to disk. However, ruby of course
allocates much more memory for a chunk than the raw file size of the chunk. As
a rule of thumb, it allocates 10 times more memory. Still, choosing a value for
`memory_size` depends on the memory size of your container/server, how much
worker threads your background queue spawns and how much memory your workers
need besides of Kraps. Let's say your container/server has 2 gigabytes of
memory and your background framework spawns 5 threads. Theoretically, you might
be able to give 300-400 megabytes to Kraps then, but now divide this by 10 and
specify a `memory_limit` of around `30.megabytes`, better less. The
`memory_limit` affects how much chunks will be written to disk depending on the
data size you are processing and how big these chunks are. The smaller the
value, the more chunks and the more chunks, the more runs Kraps need to merge
the chunks. It can affect the performance The `chunk_limit` ensures that only
the specified amount of chunks are processed in a single run. A run basically
means: it takes up to `chunk_limit` chunks, reduces them and pushes the result
as a new chunk to the list of chunks to process. Thus, if your number of file
descriptors is unlimited, you want to set it to a higher number to avoid the
overhead of multiple runs. `concurrency` tells Kraps how much threads to use to
concurrently upload/download files from the storage layer. Finally, `retries`
specifies how often Kraps should retry the job step in case of errors. Kraps
will sleep for 5 seconds between those retries. Please note that it's not yet
possible to use the retry mechanism of your background job framework with
Kraps. Please note, however, that `parallelize` is not covered by `retries`
yet, as the block passed to `parallelize` is executed by the runner, not the
workers.


Now, executing your job is super easy:

```ruby
Kraps::Runner.new(SearchLogCounter).call(start_date: '2018-01-01', end_date: '2022-01-01')
```

This will execute all steps of your job, where the parts of a step are executed
in parallel, depending on the number of background job workers you have.

The runner by default also shows the progress of the execution:

```
SearchLogCounter: job 1/1, step 1/4, token 2407e38eb58233ae3cecaec86fa6a6ec, Time: 00:00:05, 356/356 (100%) => parallelize
SearchLogCounter: job 1/1, step 2/4, token 7f11a04c754389359f67c1e7627468c6, Time: 00:08:00, 128/128 (100%) => map
SearchLogCounter: job 1/1, step 3/4, token b602198bfeab20ff205a00af36e43402, Time: 00:03:00, 128/128 (100%) => reduce
SearchLogCounter: job 1/1, step 4/4, token d18acbb22bbd30faff7265c179d4ec5a, Time: 00:02:00, 128/128 (100%) => each_partition
```

How many "parts" a step has mostly boils down to the number of partitions you
specify in the job respectively steps. More concretely, As your data consists
of `(key, value)` pairs, the number of partitions specifies how your data gets
split. Kraps assigns every `key` to a partition, either using a custom
`partitioner` or the default built in hash partitioner. The hash partitioner
simply calculates a hash of your key modulo the number of partitions and the
resulting partition number is the partition where the respective key is
assigned to. A partitioner is a callable which gets the key and the number of
partitions as argument and returns a partition number. The built in hash
partitioner looks similar to this one:

```ruby
partitioner = proc { |key, num_partitions| Digest::SHA1.hexdigest(key.inspect)[0..4].to_i(16) % num_partitions }
```

Please note, it's important that the partitioner and the specified number of
partitions stays in sync. When you use a custom partitioner, please make sure
that the partitioner correctly returns a partition number in the range of
`0...num_partitions`.

## Datatypes

Be aware that Kraps converts everything you pass to it to JSON sooner or later,
i.e. symbols will be converted to strings, etc. Therefore, it is recommended to
only use the json compatible datatypes right from the start. However, the keys
that you pass to Kraps additionally must be properly sortable, such that it is
recommended to only use strings, numbers and arrays or a combination of those
for the keys. For more information, please check out
https://github.com/mrkamel/map-reduce-ruby/#limitations-for-keys

## Storage

Kraps stores temporary results of steps in a storage layer. Currently, only S3
is supported besides a in memory driver used for testing purposes. Please be
aware that Kraps does not clean up any files from the storage layer, as it
would be a safe thing to do in case of errors anyways. Instead, Kraps relies on
lifecycle features of modern object storage systems. Therefore, it is recommend
to e.g. configure a lifecycle policy to delete any files after e.g. 7 days
either for a whole bucket or for a certain prefix like e.g. `temp/` and tell
Kraps about the prefix to use (e.g. `temp/kraps/`).

```ruby
Kraps::Drivers::S3Driver.new(s3_client: Aws::S3::Client.new("..."), bucket: "some-bucket", prefix: "temp/kraps/"),
```

If you set up the lifecycle policy for the whole bucket instead and Kraps is
the only user of the bucket, then no prefix needs to be specified.

## API

Your jobs can use the following list of methods. Please note that you don't
always need to specify all the parameters listed here. Especially `partitions`,
`partitioner` and `worker` are used from the previous step unless changed in
the next one.

* `parallelize`: Used to seed the job with initial data

```ruby
job.parallelize(partitions: 128, partitioner: partitioner, worker: MyKrapsWorker) do |collector|
  ["item1", "item2", "item3"].each do |item|
    collector.call(item)
  end
end
```

The block must use the collector to feed Kraps with individual items. The
items are used as keys and the values are set to `nil`.

* `map`: Maps the key value pairs to other key value pairs

```ruby
job.map(partitions: 128, partitioner: partitioner, worker: MyKrapsWorker) do |key, value, collector|
  collector.call("changed #{key}", "changed #{value}")
end
```

The block gets each key-value pair passed and the `collector` block can be
called as often as neccessary. This is also the reason why `map` can not simply
return the new key-value pair, but the `collector` must be used instead.

* `map_partitions`: Maps the key value pairs to other key value pairs, but the
  block receives all data of each partition as an enumerable and sorted by key.
  Please be aware that you should not call `to_a` or similar on the enumerable.
  Prefer `map` over `map_partitions` when possible.

```ruby
job.map_partitions(partitions: 128, partitioner: partitioner, worker: MyKrapsWorker) do |pairs, collector|
  pairs.each do |key, value|
    collector.call("changed #{key}", "changed #{value}")
  end
end
```

* `reduce`: Reduces the values of pairs having the same key

```ruby
job.reduce(worker: MyKrapsWorker) do |key, value1, value2|
  value1 + value2
end
```

When the same key exists multiple times in the data, kraps feeds the values
into your reduce block and expects to get one value returned. This happens
until every key exists only once.

The `key` itself is also passed to the block for the case that you need to
customize the reduce calculation according to the value of the key. However,
most of the time, this is not neccessary and the key can simply be ignored.

* `combine`: Combines the results of 2 jobs by combining every key available
  in the current job result with the corresponding key from the passed job
  result. When the passed job result does not have the corresponding key,
  `nil` will be passed to the block. Keys which are only available in the
  passed job result are completely omitted.

```ruby
  job.combine(other_job, worker: MyKrapsWorker) do |key, value1, value2|
    (value1 || {}).merge(value2 || {})
  end
```

Please note that the keys, partitioners and the number of partitions must match
for the jobs to be combined. Further note that the results of `other_job` must
be reduced, meaning that every key must be unique. Finally, `other_job` must
not neccessarily be listed in the array of jobs returned by the `call` method,
since Kraps detects the dependency on its own.

* `repartition`: Used to change the partitioning

```ruby
job.repartition(partitions: 128, partitioner: partitioner, worker: MyKrapsWorker)
```

Repartitions all data into the specified number of partitions and using the
specified partitioner.

* `each_partition`: Passes the partition number and all data of each partition
  as an enumerable and sorted by key. Please be aware that you should not call
  `to_a` or similar on the enumerable.

```ruby
job.each_partition do |partition, pairs|
  pairs.each do |key, value|
    # ...
  end
end
```

Please note that every API method accepts a `before` callable:

```ruby
before_block = proc do
  # runs once before the map action in every worker, which can be useful to
  # e.g. populate caches etc.
end

job.map(before: before_block) do |key, value, collector|
  # ...
end
```

## More Complex Jobs

Please note that a job class can return multiple jobs and jobs can build up on
each other. Let's assume that we additionally want to calculate a total number
of searches made:

```ruby
class SearchLogCounter
  def call(start_date:, end_date:)
    count_job = Kraps::Job.new(worker: SomeBackgroundWorker)

    count_job = count_job.parallelize(partitions: 128) do |collector|
      (Date.parse(start_date)..Date.parse(end_date)).each do |date|
        collector.call(date.to_s)
      end
    end

    count_job = count_job.map do |date, _, collector|
      # ...

      collector.call(data["q"], 1)

      # ...
    end

    count_job = count_job.reduce do |_, count1, count2|
      count1 + count2
    end

    sum_job = count_job.map do |q, count, collector|
      collector.call('sum', count)
    end

    sum_job = sum_job.reduce do |_, count1, count2|
      count1 + count2
    end

    # ...

    [count_job, sum_job]
  end
end
```

When you execute the job, Kraps will execute the jobs one after another and as
the jobs build up on each other, Kraps will execute the steps shared by both
jobs only once.

## Dependencies

Kraps is built on top of
[map-reduce-ruby](https://github.com/mrkamel/map-reduce-ruby) for the
map/reduce framework,
[distributed_job](https://github.com/mrkamel/distributed_job)
to keep track of the job/step status,
[attachie](https://github.com/mrkamel/attachie) to interact with the storage
layer (s3),
[ruby-progressbar](https://github.com/jfelchner/ruby-progressbar) to
report the progress in the terminal.

It is highly recommended to check out `map-reduce-ruby` to dig into internals
and performance details.

## Contributing

Bug reports and pull requests are welcome on GitHub at
https://github.com/mrkamel/kraps. This project is intended to be a safe,
welcoming space for collaboration, and contributors are expected to adhere to
the [code of conduct](https://github.com/mrkamel/kraps/blob/main/CODE_OF_CONDUCT.md).

## License

The gem is available as open source under the terms of the
[MIT License](https://opensource.org/licenses/MIT).

## Code of Conduct

Everyone interacting in the Kraps project's codebases, issue trackers, chat
rooms and mailing lists is expected to follow the
[code of conduct](https://github.com/mrkamel/kraps/blob/main/CODE_OF_CONDUCT.md).
