#!/usr/bin/env ruby

NUM_TO_KEEP = 10

processing_doc = nil
min_freq = max_freq = 0
min_trigrams = [] # [[4,'abc'],[3,'wer']]

STDIN.each do |record|
	record =~ /(.*)\t(.*?) (.*)/
	doc,freq,trigram = $1, $2.to_f, $3

	if doc != processing_doc
		if processing_doc != nil
			puts "#{processing_doc}\t#{min_freq} #{min_trigrams.inspect}"
		end
		processing_doc = doc
		min_freq = max_freq = freq
		min_trigrams = [[freq,trigram]]

	elsif freq < max_freq
		min_trigrams.push [freq,trigram]
		min_trigrams.shift if min_trigrams.size > NUM_TO_KEEP
		min_trigrams = min_trigrams.sort_by{|a,b| b[0]<=>a[0]}
		max_freq = min_trigrams.first[0]
		min_freq = min_trigrams.last[0]
	end

end			

puts "#{processing_doc}\t#{min_freq} #{min_trigrams.inspect}"

