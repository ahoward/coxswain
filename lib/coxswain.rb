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
  STDOUT.sync = true

  pool =
    Coxswain.pool do |job|
      "#{ job } ran at #{ Time.now.to_f } in #{ Process.pid }..."
    end

  pool.spawn(10)

  q = Queue.new

  10.times do |i|
    pool.run('job %d' % i){|result| q.push(result)}
  end

  10.times do
    p q.pop
  end

  pool.shutdown!
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
