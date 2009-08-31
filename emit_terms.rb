#!/usr/bin/env ruby
STDIN.each do |line|
	line_without_filename = line.chomp.sub /(.*?) /,''
	terms = line_without_filename.split
	terms.each { |term| puts "LongValueSum:#{term}.0p\t1" }
end
