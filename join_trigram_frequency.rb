#!/usr/bin/env ruby
log_frequency = nil
STDIN.each do |record|
	record =~ /(.*?)\t(.*)/
	key, value = $1, $2
	if key =~ /f$/
		# log_frequency
		log_frequency = Math.log(value.to_i)
	else
		# exploded trigram
		puts "#{value}\t#{log_frequency}"
	end
end
