#!/usr/bin/env ruby
STDIN.each do |line|
	line_without_filename = line.chomp.downcase.sub /(.*?) /,''
	terms = line_without_filename.gsub(/\'/,'').gsub(/[^a-z0-9]/,' ').strip.split
	terms.each { |term| puts "LongValueSum:#{term} 0p\t1" }
end
