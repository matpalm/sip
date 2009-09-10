#!/usr/bin/env ruby
# diy combiner by emiting only when there are a 1000 unique terms encountered

class Hash
	def inc key
		self[key] ||= 0
		self[key] += 1
		dump if keys.size > 1000
	end
	def dump
		each { |k,v| puts "LongValueSum:#{k}.0p\t#{v}" }
		clear
	end
end

freq = {}
STDIN.each do |line|
	line_without_filename = line.chomp.sub /(.*?) /,''
	tokens = line_without_filename.split
	tokens.each { |token| freq.inc token }
end
freq.dump
