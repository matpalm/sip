#!/usr/bin/env ruby
def emit tuple
		puts "LongValueSum:#{tuple.join(' ')}\t1"	
end

NGRAM_SIZE = 3

STDIN.each do |line|
	terms = line.downcase.gsub(/\'/,'').gsub(/[^a-z0-9]/,' ').chomp.strip.split
	next if terms.size < NGRAM_SIZE
	
	tuple = []
	NGRAM_SIZE.times { tuple << terms.shift }
	emit tuple

	while not terms.empty?
		tuple.shift
		tuple << terms.shift
		emit tuple
	end

end
