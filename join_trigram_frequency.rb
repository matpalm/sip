#!/usr/bin/env ruby
raise "need ENV['TOTAL_NUM_TERMS'] set" unless ENV['TOTAL_NUM_TERMS']
log_frequency = nil
TOTAL_NUM_TERMS = ENV['TOTAL_NUM_TERMS'].to_f
#debug = File.new("/tmp/debug.#{$$}.out","w")
STDIN.each do |record|
	record = record.chomp.strip # avoid \t put on end by, i think, partitioner (?)
#	debug.puts record
	record =~ /(.*)\t(.*)/
	key, value = $1, $2
	if key =~ /0p$/
		# log_frequency
		log_frequency = (Math.log(value.to_f / TOTAL_NUM_TERMS) )/3  # /3 since going for mean
	else
		# exploded trigram, need to remove frequency
		value.sub!(/ \d+$/,'')
		puts "#{value}\t#{log_frequency}"
	end
end
#debug.close
