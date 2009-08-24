#!/usr/bin/env ruby

processing_doc = nil
min_freq = nil
min_trigram = nil

STDIN.each do |record|
	record =~ /(.*)\t(.*?) (.*)/
	doc,freq,trigram = $1, $2.to_i, $3

	if doc != processing_doc
		if processing_doc != nil
			puts "#{processing_doc}\t#{min_freq} #{min_trigram}"
		end
		processing_doc = doc
		min_freq = freq
		min_trigram = trigram
	else
		if freq < min_freq
			min_freq = freq
			min_trigram = trigram
		end
	end

end			

puts "#{processing_doc}\t#{min_freq} #{min_trigram}"

