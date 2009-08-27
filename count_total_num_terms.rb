#!/usr/bin/env ruby
STDERR.puts ENV['QWE']
STDIN.each do |line|
	line =~ /(.*)\t(.*)/	
	freq = $2
	puts "LongValueSum:T\t#{freq}"
end
