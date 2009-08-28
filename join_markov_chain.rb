#!/usr/bin/env ruby
frequency = nil
STDIN.each do |record|
	record =~ /(.*?)\t(.*)/
	key, value = $1, $2
	if key =~ /0p$/
		# frequency
		frequency = value.to_f
	else
		# exploded bigram 
    # d1 e f 1
		value =~ /(.*) (\d+)$/
		doc_and_bigram, count = $1, $2 
		puts "#{doc_and_bigram}\t#{count.to_f / frequency}"
	end
end
