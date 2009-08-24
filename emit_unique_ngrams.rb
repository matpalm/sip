#!/usr/bin/env ruby
def emit docid, tuple
		puts "UniqValueCount:#{docid} #{tuple.join(' ')}\t1"	
end

NGRAM_SIZE = 3

STDIN.each do |line|
	line =~ /(.*?) (.*)/	
	file, data = $1, $2

	terms = data.downcase.gsub(/\'/,'').gsub(/[^a-z0-9]/,' ').strip.split
	next if terms.size < NGRAM_SIZE
	
	tuple = []
	NGRAM_SIZE.times { tuple << terms.shift }
	emit file, tuple

	while not terms.empty?
		tuple.shift
		tuple << terms.shift
		emit file, tuple
	end

end
