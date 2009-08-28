#!/usr/bin/env ruby
raise "need ENV['TOTAL_NUM_TERMS'] set" unless ENV['TOTAL_NUM_TERMS']
log_frequency = nil
TOTAL_NUM_TERMS = ENV['TOTAL_NUM_TERMS'].to_f
STDIN.each do |record|
	record =~ /(.*?)\t(.*)/
	key, value = $1, $2
	if key =~ /0p$/
		# log_frequency
		log_frequency = Math.log(value.to_f / TOTAL_NUM_TERMS)
	else
		# exploded trigram, need to remove frequency
		value.sub!(/ \d+$/,'')
		puts "#{value}\t#{log_frequency}"
	end
end
