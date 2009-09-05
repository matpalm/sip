#!/usr/bin/env ruby
STDIN.each do |line|
		line =~ /(.*) .*\t(.*)/	
		first_comp, freq = $1, $2
		puts "LongValueSum:#{first_comp}.0p\t#{freq}"
end
