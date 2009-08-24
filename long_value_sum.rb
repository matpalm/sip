#!/usr/bin/env ruby
STDIN.each do |record|
	key,value = record.split "\t"
	puts "LongValueSum:#{key}\t#{value}"
end
