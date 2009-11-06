require 'thread'
require 'socket'

module Coxswain
  def Coxswain.pool(*args, &block)
    Pool.new(*args, &block)
  end

  class Pool < ::Array
    attr_accessor 'block'
    attr_accessor 'queue'
    attr_accessor 'threads'

    def initialize(*n, &block)
      @block = block
      @queue = Queue.new
      @threads = []
      @workers = []
      spawn(n.first) unless n.empty?
    end

    def pool
      self
    end

    def spawn(n)
      Integer(n).times do
        worker = Worker.new(pool)
        @workers.push(worker)

        q = Queue.new

        thread =
          Thread.new(worker) do |worker|
            q.push(:running)
            loop do
              job, callback = @queue.pop
              result = worker.run(job)
              callback.call(result) if callback
            end
          end

        running = q.pop

        @threads.push(thread)
      end
    end

    def run(job, &callback)
      @queue.push([job, callback])
    end

    def shutdown!
      @workers.each do |worker|
        Process.kill('TERM', worker.pid) rescue nil
        Process.waitpid(worker.pid, Process::WNOHANG|Process::WUNTRACED)
      end
    end

    class Worker
      attr_accessor 'block'
      attr_accessor 'parent'
      attr_accessor 'child'
      attr_accessor 'socket'
      attr_accessor 'pid'

      def initialize(pool)
        @block = pool.block
        fork!
      end

      def fork!
        pair = Socket.pair(Socket::AF_UNIX, Socket::SOCK_STREAM, 0)

        if((child = fork))
          @parent = Process.pid
          @socket = pair[0]
          pair[1].close
          pair[1] = nil
          @socket.sync = true
          @child = Integer(@socket.gets)
          @pid = @child
        else
          @parent = Process.ppid
          @child = Process.pid
          @socket = pair[1]
          pair[0].close
          pair[0] = nil
          @socket.sync = true
          @socket.puts(@child)
          @pid = @child
          process!
          exit!
        end
      end

      def process!
        loop do
          job = NetString.read(@socket)
          result = @block.call(job)
          NetString.write(result, @socket)
        end
      end

      def run(job)
        NetString.write(job, @socket)
        result = NetString.read(@socket)
      end
    end
  end

  module NetString
    SIZEOF_INT = [42].pack('i').size

    def read(io)
      buf = io.read(SIZEOF_INT)
      len = buf.unpack('i').first
      buf = io.read(len)
      Marshal.load(buf)
    end

    def write(obj, io)
      buf = Marshal.dump(obj)
      len = buf.size
      io.write([len].pack('i'))
      io.write(buf)
    end

    extend self
  end
end



if $0 == __FILE__
  require 'coxswain'

  pool =
    Coxswain.pool do |job|
      "#{ job } ran at #{ Time.now.to_f } in #{ Process.pid }..."
    end

  pool.spawn(5)

  q = Queue.new

  10.times{|i| pool.run('job %d' % i){|result| q.push(result)}}
  10.times{ p q.pop }

  pool.shutdown!
end

__END__

"job 0 ran at 1257517331.80438 in 60310..."
"job 1 ran at 1257517331.80474 in 60311..."
"job 2 ran at 1257517331.80499 in 60312..."
"job 4 ran at 1257517331.80536 in 60314..."
"job 3 ran at 1257517331.80559 in 60313..."
"job 6 ran at 1257517331.80602 in 60312..."
"job 5 ran at 1257517331.80592 in 60310..."
"job 8 ran at 1257517331.80624 in 60314..."
"job 7 ran at 1257517331.80618 in 60311..."
"job 9 ran at 1257517331.80645 in 60313..."
