#!/usr/bin/env ruby
STDIN.each do |record|
	key,value = record.split "\t"
	puts "DoubleValueSum:#{key}\t#{value}"
end
