require 'thread'
require 'socket'

module Coxswain
  def Coxswain.pool(&block)
    Pool.new(&block)
  end

  class Pool < ::Array
    attr_accessor 'block'
    attr_accessor 'queue'
    attr_accessor 'threads'

    def initialize(&block)
      @block = block
      @queue = Queue.new
      @threads = []
    end

    def pool
      self
    end

    def spawn(n)
      Integer(n).times do
        worker = Worker.new(pool)

        q = Queue.new

        thread =
          Thread.new(worker) do |worker|
            q.push(:running)
            loop do
              job, q = @queue.pop
              result = worker.run(job)
              q.push(result)
            end
          end

        running = q.pop

        @threads.push(thread)
      end
    end

    def run(job)
      @queue.push([job, q=Queue.new])
      q.pop
    end

    class Worker
      attr_accessor 'block'
      attr_accessor 'parent'
      attr_accessor 'child'
      attr_accessor 'socket'

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
        else
          @parent = Process.ppid
          @child = Process.pid
          @socket = pair[1]
          pair[0].close
          pair[0] = nil
          @socket.sync = true
          @socket.puts(@child)
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
  STDOUT.sync = true

  pool =
    Coxswain.pool do |job|
      "#{ job } ran at #{ Time.now.to_f } in #{ Process.pid }..."
    end

  pool.spawn(10)

  10.times do |i|
    puts pool.run('job %d' % i)
  end

  sleep
end


__END__

job 0 ran at 1257481668.77393 in 57260...
job 1 ran at 1257481668.77452 in 57261...
job 2 ran at 1257481668.775 in 57262...
job 3 ran at 1257481668.77549 in 57263...
job 4 ran at 1257481668.77594 in 57264...
job 5 ran at 1257481668.77643 in 57265...
job 6 ran at 1257481668.77692 in 57266...
job 7 ran at 1257481668.77739 in 57267...
job 8 ran at 1257481668.77782 in 57268...
job 9 ran at 1257481668.77833 in 57269...
