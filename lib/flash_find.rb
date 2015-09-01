
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
# stating this entry, check `#error?`, and retrieve it with `#error`.
#
# ==== Example
#
#   FlashFind.push('/tmp').skip_errors # aka .prune(&:error?)
#            .prune {|entry| entry.directory? && entry.basename.to_s == '.git' }
#            .prune_directory('.git') # shorthand for above
#            .permit {|entry| entry.readable? }
#            .each { |entry| p entry }
#

require 'logger'

module FlashFind
	Log = Logger.new STDERR
	# DirEntry wraps File::Stat with additional information such as filename and
	# optional lstat error information.
	class DirEntry
		extend Forwardable
		def_delegators :@pathname, *Pathname.public_instance_methods(false)
		def_delegators :@stat, *File::Stat.public_instance_methods(false)

		attr_reader :path, :stat, :error
		alias_method :to_s, :path
		alias_method :name, :path
		alias_method :exception, :error

		def initialize(path, stat = nil, error = nil)
			@path = path
			@pathname = Pathname.new(path)
			@stat = @stat || File.lstat(path)
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
		def push(*dirs)
			Finder.new.push(*dirs)
		end
	end

	# Core class
	class Finder
		include Enumerable

		attr :filter, :dirs
		attr_reader :job_queue

		def initialize
			@dirs    = Set.new
			@filter  = []
			@lock    = Mutex.new
			@working = Concurrent::AtomicFixnum.new
		end

		# Add a set of paths to the list to start the directory walk from.
		#
		# Dereferenced with `#to_path` if necessary.
		def push(*dirs)
			dirs.flatten.each do |dir|
				dir = dir.to_path if dir.respond_to?(:to_path)
				@dirs << dir.dup
			end
			self
		end

		# Silently skip over erroring entries.
		def skip_errors
			prune(&:error?)
		end

		# Raise an error rather than allow iteration to continue.
		def raise_errors
			prune { |entry| fail Error.new(entry) if entry.error? }
		end

		def prune_directory(basename)
			prune { |entry| entry.directory? && entry.basename.to_s == basename }
		end

		# Add a block that must return truthy to allow further processing.
		#
		# May run concurrently in background threads.
		def permit(&block)
			@filter << Permit.new(block)
			self
		end

		# Add a block that must return falsey to allow further procesing.
		#
		# May run concurrently in background threads.
		def prune(&block)
			@filter << Prune.new(block)
			self
		end
		# alias_method :prune, :reject

		# Begin iterating
		def each(&block)
			return enum_for(__method__) unless block_given?

			run {|runner| runner.each(&block) }
		end

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

		private

		def run
			work_pool do |workers|
				yield Runner.new(self, workers)
			end
		end

		def work_pool
			@working.increment
			lock do
				@job_queue ||= Queue.new
				@concurrency = 8 # [Concurrent.processor_count, 32].max

				if !@workers
					@workers ||= Concurrent::FixedThreadPool.new(@concurrency)
					@concurrency.times do
						@workers.post do
							DirectoryWalker.new(@job_queue).run
						end
					end
				end
			end

			yield(@job_queue)
		ensure
			@working.decrement
		end

		def lock
			@lock.synchronize { yield }
		end

		class Filter # :nodoc:
			def initialize(block)
				@block = block
			end
		end

		class Permit < Filter # :nodoc:
			def apply?(entry)
				@block.call(entry)
			end
		end

		class Prune < Filter # :nodoc:
			def apply?(entry)
				!@block.call(entry)
			end
		end

		class Runner
			attr_reader :finder, :workers, :results, :running
			attr_accessor :concurrency

			def initialize(finder, workers)
				@concurrency = 1
				@finder = finder
				@workers = workers
				@results = Queue.new
				@iterators = Concurrent::AtomicFixnum.new
				@pending = Concurrent::AtomicFixnum.new
				@running = Concurrent::AtomicBoolean.new
			end

			def run
				return unless @running.make_true

				finder.dirs.each_slice(256) do |entries|
					pending!
					workers << Job.new(:stat, self, entries)
				end
			end

			def filter
				finder.filter
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
						@results << nil
						[@concurrency, @iterators.value].max.times { @results << nil }
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
					when Array     then result.each {|entry| yield(entry) }
					when DirEntry  then yield(entry)
					when Exception then raise result
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

		Job = Struct.new(:action, :runner, :args)

		class DirectoryWalker
			attr_reader :queue
			attr_reader :thread

			def initialize(queue)
				@queue = queue
			end

			def run
				while job = queue.deq
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
			end

			def enqueue(job)
				@queue << job if job.runner.pending!
			end

			def stat(job)
				entries = job.args.map {|file| DirEntry.new(file) }.select do |entry|
					job.runner.filter.all? {|filter| filter.apply?(entry) }
				end

				entries.select(&:directory?).each {|dir| enqueue(Job.new(:dir, job.runner, dir)) }
				job.runner.yield entries
			end

			def dir(job)
				path = job.args
				entries(path).each_slice(16) do |ents|
					enqueue(Job.new(:stat, job.runner, ents.map {|ent| File.join(path, ent) }))
				end
				job.runner.yield nil
			end

			private

			IgnoreDirs = ['.'.freeze, '..'.freeze].freeze
			def entries(path)
				Dir.entries(path) - IgnoreDirs
			end
		end
	end
end
