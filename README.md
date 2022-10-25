# Kraps

**Easily process big data in ruby**

Kraps allows to process and perform calculations on extremely large datasets in
parallel using a map/reduce framework and runs on a background job framework
you already have. You just need some space on your filesystem, a storage layer
(usually s3) with temporary lifecycle policy enabled, the already mentioned
background job framework (like sidekiq, shoryuken, etc) and redis to keep track
of the progress.

## Installation

Install the gem and add to the application's Gemfile by executing:

    $ bundle add kraps

If bundler is not being used to manage dependencies, install the gem by executing:

    $ gem install kraps

## Usage

The first think you need to do, is to create a job class, which tells Kraps
what your job should do. Therefore, you create some class with a `call` method,
and optionally some arguments. Let's create a simple job, which reads search
log files to analyze how often search queries have been searched:

```ruby
class SearchLogCounter
  def call(start_date:, end_date:)
    job = Kraps::Job.new(worker: SomeBackgroundWorker)
    job = job.parallelize(partitions: 128) { Date.parse(start_date)..Date.parse(end_date) }

    job = job.map do |date, _, &collector|
      # fetch log file for the date from e.g. s3

      File.open(logfile).each_line do |line|
        data = JSON.parse(line)

        collector.call(data["q"], 1)
      end
    end

    job = job.reduce do |_, count1, count2|
      count1 + count2
    end

    job = job.each_partition do |partition|
      tempfile = Tempfile.new

      partition.each do |q, count|
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

Please note that this is represents a specification of your job. It should be
as free as possible from side effects, because your background jobs must also
be able to take this specification to be told what to do as Kraps will run the
job with maximum concurrency.

Next thing you need to do: create a background worker, which runs arbitrary
Kraps job steps. Let's assume you have sidekiq in place:

```ruby
class KrapsWorker
  include Sidekiq::Worker

  def perform(json)
    Kraps::Worker.new(json, memory_limit: 128.megabytes, chunk_limit: 64, concurrency: 8).call
  end
end
```

The `json` argument is automatically enqueued by Kraps and contains everything
Kraps needs to know about the job and step to execute. The `memory_limit` tells
Kraps how much memory it is allowed to allocate for temporary chunks, etc. This
value depends on the memory size of your container/server and how much worker
threads your background queue spawns.  Let's say your container/server has 2
gigabytes of memory and your background framework spawns 5 threads.
Theoretically, you might be able to give 300-400 megabytes to Kraps then. The
`chunk_limit` ensures that only the specified amount of chunks are processed in
a single run. A run basically means: it takes up to `chunk_limit` chunks,
reduces them and pushes the result as a new chunk to the list of chunks to
process. Thus, if your number of file descriptors is unlimited, you want to set
it to a higher number to avoid the overhead of multiple runs. Finally,
`concurrency` tells Kraps how much threads to use to concurrently
upload/download files from the storage layer.

Now, executing your job is super easy:

```ruby
Kraps::Runner.new(SearchLogCounter, start_date: '2018-01-01', end_date: '2022-01-01').call(retries: 3)
```

This will execute all steps of your job, where all parts of a step are executed
in parallel. How many "parts" a step has largely boils down to the number of
partitions you specify in the job respectively steps. More concretely, As your
data consists of `(key, value)` pairs, the number of partitions specify how
your data gets split. Kraps assigns every `key` to a partition, either using a
custom `partitioner` or the default build in hash partitioner. The hash
partitioner simply calculates a hash of your key modulo the number of
partitions and the resulting partition number is the partition where the
respective key is assigned to. Finally, `retries` specifies how often Kraps
should retry the job step in case of errors. Kraps will sleep for 5 seconds
between those retries. Please note that it's not yet possible to use the retry
mechanism of your background job framework.

## API

Your jobs can use the following methods:

* `parallelize(partitions:, partitioner:, worker:)`: Used to seed the job with initial data
* `map(partitions:, partitioner:, worker:)`: Maps the key value pairs to other key value pairs
* `reduce(worker:)`: Reduces the values of pairs having the same key
* `repartition(partitions:, partitioner:, worker:)`: Used to change the partitioning
* `each_partition(worker:)`: Iterates over all data of all partitions

## More Complex Jobs

Please note that a job class can return multiple jobs and jobs can build up on
each other. Let's assume that we additionally want to calculate a total number
of searches made:

```ruby
class SearchLogCounter
  def call(start_date:, end_date:)
    count_job = Kraps::Job.new(worker: SomeBackgroundWorker)
    count_job = count_job.parallelize(partitions: 128) { Date.parse(start_date)..Date.parse(end_date) }

    count_job = count_job.map do |date, _, &collector|
      # ...

      collector.call(data["q"], 1)

      # ...
    end

    count_job = count_job.reduce do |_, count1, count2|
      count1 + count2
    end

    sum_job = count_job.map do |q, count, &collector|
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
