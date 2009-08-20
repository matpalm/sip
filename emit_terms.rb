#!/usr/bin/env ruby
STDIN.each do |line|
	terms = line.downcase.gsub(/\'/,'').gsub(/[^a-z0-9]/,' ').chomp.strip.split
	terms.each { |term| puts "LongValueSum:#{term}\t1" }
end
