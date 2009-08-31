#!/usr/bin/env ruby
STDIN.each do |record|
	record =~ /(.*) (.*)\t(.*)/
	from,to,count = $1,$2,$3
	puts "#{from}.1f\t#{from} #{to} #{count}" 
end
