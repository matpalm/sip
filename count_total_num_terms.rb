#!/usr/bin/env ruby
STDIN.each do |line|	
	line =~ /(.*)\t(.*)/	
	freq = $2
	puts "LongValueSum:T\t#{freq}"
end
