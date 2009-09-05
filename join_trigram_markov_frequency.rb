#!/usr/bin/env ruby
# 0 5.0p   0.333333333333333
# 0 5.1f   7hmvg10 0 5 10
# 0 pa.0p  0.333333333333333
# 0 pa.1f  7hmvg10 0 pa rhubarb

log_frequency = nil
STDIN.each do |record|
	record =~ /(.*)\t(.*)/
	key, value = $1, $2
	if key =~ /0p$/
		# log_frequency
		log_frequency = ( Math.log value.to_f ) / 2 # /2 since going for mean
	else
		puts "#{value}\t#{log_frequency}"
	end
end
