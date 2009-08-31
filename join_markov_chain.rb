#!/usr/bin/env ruby
frequency = nil
STDIN.each do |record|
	record =~ /(.*?)\.(.*)\t(.*)/
	key_term, key_type, value = $1, $2, $3
	if key_type =~ /0p$/
		# frequency
		frequency = value.to_f
	else
		# exploded bigram 
    # d1 e f 1
		value =~ /(.*) (\d+)$/
		doc_and_bigram, count = $1, $2 
		puts "#{doc_and_bigram}.0p\t#{count.to_f / frequency}"
	end
end
