#!/usr/bin/env ruby
log_frequency = nil
TOTAL_NUM_TERMS = ENV['TOTAL_NUM_TERMS'].to_f or raise "need ENV['TOTAL_NUM_TERMS'] set"
STDIN.each do |record|
	record =~ /(.*?)\t(.*)/
	key, value = $1, $2
	if key =~ /f$/
		# log_frequency
		log_frequency = Math.log(value.to_f / TOTAL_NUM_TERMS)
	else
		# exploded trigram
		puts "#{value}\t#{log_frequency}"
	end
end
