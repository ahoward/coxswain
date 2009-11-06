require 'coxswain'
require 'open-uri'

# setup the process pool
#
  pool =
    Coxswain.pool(10) do |url|
      open(url){|socket| socket.read}
    end

  at_exit{ pool.shutdown! }


# we'll just curl this to emulate 'work'
#
  url = 'http://codeforpeople.com'
  n = 20

# time running via the pool
#
  a = Time.now.to_f
  q = Queue.new

  n.times do |i|
    pool.run(url){|result| q.push(result) }
  end

  n.times{ q.pop } # wait for all jobs

  b = Time.now.to_f

  puts "pool: #{ b - a }"

# time running serial
#
  a = Time.now.to_f

  n.times do |i|
    open(url){|socket| socket.read}
  end

  b = Time.now.to_f

  puts "serial: #{ b - a }"


__END__
pool: 0.345747947692871
serial: 1.50806093215942
