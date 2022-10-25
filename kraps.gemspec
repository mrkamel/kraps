require_relative "lib/kraps/version"

Gem::Specification.new do |spec|
  spec.name = "kraps"
  spec.version = Kraps::VERSION
  spec.authors = ["Benjamin Vetter"]
  spec.email = ["vetter@flakks.com"]

  spec.summary = "Easily process big data in ruby"
  spec.description = "Kraps allows to process and perform calculations on extremely large datasets in parallel"
  spec.homepage = "https://github.com/mrkamel/kraps"
  spec.license = "MIT"
  spec.required_ruby_version = ">= 2.7.0"

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = "https://github.com/mrkamel/kraps"
  spec.metadata["changelog_uri"] = "https://github.com/mrkamel/kraps/blob/master/CHANGELOG.md"

  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  spec.files = Dir.chdir(File.expand_path(__dir__)) do
    `git ls-files -z`.split("\x0").reject do |f|
      (f == __FILE__) || f.match(%r{\A(?:(?:bin|test|spec|features)/|\.(?:git|travis|circleci)|appveyor)})
    end
  end
  spec.bindir = "exe"
  spec.executables = spec.files.grep(%r{\Aexe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_dependency "attachie"
  spec.add_dependency "distributed_job"
  spec.add_dependency "map-reduce-ruby", ">= 2.1.1"
  spec.add_dependency "redis"
  spec.add_dependency "ruby-progressbar"

  spec.add_development_dependency "bundler"
  spec.add_development_dependency "rspec"
  spec.add_development_dependency "rubocop"

  # For more information and examples about making a new gem, check out our
  # guide at: https://bundler.io/guides/creating_gem.html
end
