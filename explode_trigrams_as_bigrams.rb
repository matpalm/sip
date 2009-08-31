#!/usr/bin/env ruby
# 7hmvg10 0 5 10	1
# to
#0 5.1f   7hmvg10 0 5 10
#5 10.1f  7hmvg10 0 5 10
STDIN.each do |record|
	record =~ /(.*) (.*) (.*) (.*)\t/ # ignore last, freq
	doc_id,t1,t2,t3 = $1,$2,$3,$4
	puts "#{t1} #{t2}.1f\t#{doc_id} #{t1} #{t2} #{t3}"
	puts "#{t2} #{t3}.1f\t#{doc_id} #{t1} #{t2} #{t3}"
end
