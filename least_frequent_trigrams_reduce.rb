#!/usr/bin/env ruby

NUM_TO_KEEP = 10

processing_doc = nil
min_freq = max_freq = 0
min_trigrams = [] # [[-4,'abc'],[-3,'wer']]

STDIN.each do |record|
	record =~ /(.*)\t(.*?) (.*)/
	doc,freq,trigram = $1, $2.to_f, $3
	if doc != processing_doc
		if processing_doc != nil
			puts "#{processing_doc}\t#{min_trigrams.inspect}"
		end
		processing_doc = doc
		min_freq = max_freq = freq
		min_trigrams = [[freq,trigram]]
	else
		if min_trigrams.size < NUM_TO_KEEP
			min_trigrams << [freq,trigram]
			min_trigrams = min_trigrams.sort{|a,b| a[0]<=>b[0]}
			max_freq = min_trigrams.last[0]
		elsif freq < max_freq
			min_trigrams.last[0] = freq
			min_trigrams.last[1] = trigram
			min_trigrams = min_trigrams.sort{|a,b| a[0]<=>b[0]}
			max_freq = min_trigrams.last[0]
			min_freq = min_trigrams.first[0]
		end
	end
end			

puts "#{processing_doc}\t#{min_trigrams.inspect}"

