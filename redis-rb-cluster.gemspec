Gem::Specification.new do |s|
  s.name        = "rediscluster"
  s.platform    = Gem::Platform::RUBY
  s.version     = 0.1
  s.authors     = ["Antirez", "iandyh"]
  s.email       = []
  #s.homepage    = "https://github.com/"
  s.description = s.summary = %q{Redis Cluster client for Ruby}

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = ["."]
  #s.license = "BSD"
  s.add_runtime_dependency 'connection_pool'
end
