
require 'pathname'
require 'set'
require 'thread'
require 'thread_safe'
require 'concurrent'

require 'flash_find/version'

# == FlashFind
#
# High performance multi-threaded directory walker.
#
# `#push` directories to be walked, `#prune` entries you don't want to recurse
# into, and `#each` over the files and directories which match.
#
# Unlike `Find`, and `FastFind`, objects yielded are not `String` but `DirEntry`
# objects, which primarily delegate to the underlying `File::Stat` object.
#
# To get the filename, call `#path` or `#to_s`.  To check if there was an error
# lstating this entry, check `#error?`, and retrieve it with `#error`.
#
# ==== Example
#
#   FlashFind.push('/tmp').skip_errors # aka .prune(&:error?)
#            .prune {|entry| entry.directory? && entry.basename.to_s == '.git' }
#            .prune_directory('.git') # shorthand for above
#            .permit {|entry| entry.readable? }
#            .each { |entry| p entry }
#
# Each call to query method like `#prune` and `#push` returns a new object,
# wrapping the current one and inheriting its existing directories and filters.
#
# This lets you build custom trees of different finders with desired behaviour:
#
#    FailSafeFind = FlashFind.skip_errors
#    CodeSafeFind = FailSafeFind.prune_directory('.git', '.hg', '.svn')
#    FooFind = CodeSafeFind.push('src/foo')
#    CodeSafeFindToo = FooFind.parent

require 'logger'

module FlashFind
	Log = Logger.new STDERR
	# DirEntry delegates to `Pathname` and `File::Stat` to provide useful query and
	# path manipulation functionality bundled together.
	#
	#    FlashFind.push('bin').each do |entry|
	#      entry.path      # => 'bin/console'
	#      entry.file?     # => true
	#      entry.basename  # => 'console'
	#      entry.dirname   # => 'bin'
	#    end
	class DirEntry
		extend Forwardable
		def_delegators :@pathname, *Pathname.public_instance_methods(false)
		def_delegators :@stat, *File::Stat.public_instance_methods(false)

		attr_reader :path, :stat, :error, :dir
		alias_method :to_s, :path
		alias_method :name, :path
		alias_method :exception, :error

		def initialize(path, stat = nil, error = nil)
			@path = path
			@pathname = Pathname.new(path)
			@stat = @stat || File.lstat(path)
			@dir = Dir.new(path) if @stat && @stat.readable? && @stat.directory?
			@error = error
		rescue => e
			@error = e
		end

		def stat?
			!@stat.nil?
		end

		def error?
			!@error.nil?
		end
		alias_method :exception?, :error?

		def inspect
			"#<#{self.class.name} path=#{path.inspect} stat=#{stat.inspect} error=#{error.inspect}>"
		end
	end

	class Error < RuntimeError
		attr_reader :entry

		def initialize(entry)
			@entry = entry
		end

		def cause
			@entry.exception
		end
	end

	class << self
		def self.del(*meths)
			meths.each do |meth|
				define_method(meth) do |*args|
					Finder.new.send(meth, *args)
				end
			end
		end

		def self.del_block(*meths)
			meths.each do |meth|
				define_method(meth) do |*args, &block|
					Finder.new.send(meth, *args, &block)
				end
			end
		end

		del :push, :prune, :raise_errors, :skip_errors, :prune_directory
		del_block :prune, :permit
	end

	Job = Struct.new(:action, :runner, :args)

	MAX_THREADS    = 8
	ThreadPool     = Concurrent::CachedThreadPool.new(min_threads: 0, max_threads: MAX_THREADS)
	PoolQueue      = Queue.new
	SpunUp         = Concurrent::AtomicBoolean.new
	Active         = Concurrent::AtomicFixnum.new
	LastActive     = Concurrent::AtomicReference.new
	CooldownThread = Concurrent::AtomicReference.new
	RWLock = Concurrent::ReadWriteLock.new

	class DirectoryWalker
		def run
			while job = PoolQueue.deq
				if job.is_a? Concurrent::CountDownLatch
					job.count_down
					break
				end

				run_job(job)
			end
		end

		def run_job(job)
			begin
				case job.action
				when :stat then stat(job)
				when :dir then dir(job)
				else raise ArgumentError, "Unknown job type: #{job.inspect}"
				end
			rescue => e
				Log.error e
				job.runner.yield e
			end
		end

		private

		def enqueue(job)
			PoolQueue << job if job.runner.pending!
		end

		def stat(job)
			entries = job.args.map {|file| DirEntry.new(file) }.select do |entry|
				job.runner.filters.all? {|filter| filter.call(entry) }
			end

			entries.reject(&:error?).select(&:directory?).each {|dir| enqueue(Job.new(:dir, job.runner, dir)) }
			job.runner.yield entries
		end

		def dir(job)
			path = job.args
			if path.readable?
				entries = path.entries.drop(2) # Drop . and ..
				entries.each_slice([entries.size / 32, 8].max) do |ents|
					enqueue(Job.new(:stat, job.runner, ents.map {|ent| File.join(path, ent) }))
				end
			end
			job.runner.yield :dir
		end
	end
	PoolJob = DirectoryWalker.new

	# Core class
	class Finder
		include Enumerable

		attr_reader :filter, :dirs
		attr_reader :job_queue, :parent

		def initialize
			@concurrency = RUBY_ENGINE == 'ruby' ? 1 : 8
			@parent  = nil
			@dirs    = []
			@filter  = nil
			@lock    = Mutex.new
			@working = Concurrent::AtomicFixnum.new
		end

		# Return an array of starting point directories.
		def dirs
			@_dirs ||= if parent
				parent.dirs.to_a + @dirs
			else
				@dirs
			end.freeze
		end

		# Return an Array of filters
		def filters
			@filters ||= if parent
				parent.filters.dup << filter
			else
				[filter]
			end.compact.freeze
		end

		# Add a set of paths to the list to start the directory walk from.
		#
		# Dereferenced with `#to_path` if necessary.
		def push(*dirs)
			Push.new(self, dirs)
		end

		# Add a block that must return truthy to allow further processing.
		#
		# May run concurrently in background threads.
		def permit(&block)
			Permit.new(self, block)
		end

		# Add a block that must return falsey to allow further procesing.
		#
		# May run concurrently in background threads.
		def prune(&block)
			Prune.new(self, block)
		end
		# alias_method :prune, :reject

		# Begin iterating
		def each(&block)
			return enum_for(__method__) unless block_given?

			run {|runner| runner.each(&block) }
		end

		# Begin iterating using a pool of worker threads for `#each` as well.
		def concurrent_each(concurrency: Concurrent.processor_count, &block)
			return enum_for(__method__, concurrency: concurrency) unless block_given?

			run do |runner|
				pool = Concurrent::FixedThreadPool.new(concurrency)
				runner.concurrency = concurrency
				concurrency.times do
					pool.post do
						runner.each(&block)
					end
				end
				pool.shutdown
				pool.wait_for_termination
			end
		end

		# Silently skip over erroring entries.
		def skip_errors
			SkipError.new(prune(&:error?))
		end

		def skip_errors?
			@_skip_errors ||= if parent
				parent.skip_errors? || @skip_errors
			else
				@skip_errors
			end
		end

		# Raise an error rather than allow iteration to continue.
		def raise_errors
			prune { |entry| fail Error.new(entry) if entry.error? }
		end

		# Prune a directory with the given basename
		def prune_directory(basename)
			prune { |entry| entry.directory? && entry.basename.to_s == basename }
		end

		private

		def run
			working do
				yield Runner.new(self)
			end
		end

		# Only call with Active == 1
		def spinup_workers
			LastActive.set(time)
			RWLock.with_write_lock do
				return if SpunUp.true?

				MAX_THREADS.times do
					ThreadPool.post do
						DirectoryWalker.new.run
					end
				end

				SpunUp.make_true
			end
		end

		# Only call with Active = 0
		def spindown_workers
			RWLock.with_write_lock do
				thr = CooldownThread.get
				if thr
					return if thr.alive?

					thr.join
					CooldownThread.set(nil)
				end

				thr = Thread.new do
					loop do
						wakeup = time + POOL_SHUTDOWN_PERIOD + 1

						while (remaining = wakeup - time) > 0
							sleep remaining
						end

						RWLock.with_write_lock do
							if Active.value.zero? && time - LastActive.get >= POOL_SHUTDOWN_PERIOD
								SpunUp.make_false
								latch = Concurrent::CountDownLatch.new(MAX_THREADS)
								MAX_THREADS.times { PoolQueue.enq latch }
								latch.wait
								Thread.exit
							end
						end
					end
				end

				CooldownThread.set thr
			end
		end

		POOL_SHUTDOWN_PERIOD = 2
		def working
			if Active.increment == 1
				spinup_workers
			end
			@working.increment
			RWLock.with_read_lock { yield }
		ensure
			@working.decrement
			now = time
			LastActive.set(now)

			if Active.decrement.zero?
				spindown_workers
			end
		end

		def time
			Process.clock_gettime(Process::CLOCK_MONOTONIC)
		end

		def lock
			@lock.synchronize { yield }
		end

		class Push < Finder
			def initialize(parent, dirs)
				super()
				@parent = parent
				@dirs = dirs.flatten.map do |dir|
					dir = dir.to_path if dir.respond_to?(:to_path)
					dir.dup
				end

				dirs
			end
		end

		class Filter < Finder
			def initialize(parent, block)
				super()
				@parent = parent
				@filter = wrap(block)

				filters
			end
		end

		class SkipError < Finder
			def initialize(parent)
				@skip_errors = true
			end
		end

		class Permit < Filter
			private

			def wrap(entry)
				entry
			end
		end

		class Prune < Filter
			private

			def wrap(block)
				->(entry) { !block.call(entry) }
			end
		end

		class Runner
			attr_reader :finder, :workers, :results, :running
			attr_accessor :concurrency

			def initialize(finder)
				@concurrency = 8
				@finder    = finder
				@results   = Queue.new
				@iterators = Concurrent::AtomicFixnum.new
				@pending   = Concurrent::AtomicFixnum.new
				@running   = Concurrent::AtomicBoolean.new
			end

			def run
				return unless @running.make_true

				finder.dirs.each_slice(16) do |entries|
					pending!
					PoolQueue << Job.new(:stat, self, entries)
				end
			end

			def filters
				finder.filters
			end

			def pending!
				if @running.true?
					@pending.increment
					true
				end
			end

			def yield(result)
				if running.true?
					@results << result if result
					if @pending.decrement.zero?
						[1, @concurrency, @iterators.value].max.times { @results << nil }
					end
				end
			end

			def each
				return enum_for(__method__) unless block_given?

				@iterators.increment
				run
				nresults = 0
				while result = @results.deq
					case result
					when Array     then result.compact.each {|entry| yield(entry) }
					when DirEntry  then yield(entry)
					when Exception then raise result unless finder.skip_errors?
					when :dir then next
					else Log.warn "Unhandled result: #{result}"
					end
				end
			ensure
				if @iterators.decrement.zero?
					@pending.value = 0
					@results.clear
					@running.make_false
				end
			end
		end
	end
end
