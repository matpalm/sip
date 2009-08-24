#!/usr/bin/env ruby
frequency = nil
STDIN.each do |record|
	record =~ /(.*?)\t(.*)/
	key, value = $1, $2
	if key =~ /f$/
		# frequency
		frequency = value.to_i
	else
		# exploded trigram
		puts "#{value}\t#{frequency}"
	end
end
