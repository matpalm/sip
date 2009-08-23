#!/usr/bin/env ruby
STDIN.each do |line|
	line_with_filename = line.chomp.downcase.sub /(.*?) /,''
	terms = line_with_filename.gsub(/\'/,'').gsub(/[^a-z0-9]/,' ').strip.split
	terms.each { |term| puts "LongValueSum:#{term}\t1" }
end
