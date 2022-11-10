module Kraps
  class JobResolver
    def call(jobs)
      resolve_dependencies(Array(jobs)).uniq
    end

    private

    def resolve_dependencies(jobs)
      jobs.map { |job| [resolve_dependencies(job.steps.map(&:dependency).compact), job] }.flatten
    end
  end
end
