#!/usr/bin/env ruby
def emit docid, tuple
		puts "UniqValueCount:#{docid} #{tuple.join(' ')}\t1"	
end

NGRAM_SIZE = 3

STDIN.each do |line|
	begin
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
	rescue
		#STDERR.puts "problem with line [#{line.chomp}] ??"
		raise "problem with line [#{line.chomp}] ??"
	end
end
