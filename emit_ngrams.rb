#!/usr/bin/env ruby

['AGGREGATE_TYPE', 'NGRAM_SIZE'].each do |p|
	raise "#{p} env var not set" unless ENV[p]
end

AGGREGATE_TYPE = ENV['AGGREGATE_TYPE']
NGRAM_SIZE = ENV['NGRAM_SIZE'].to_i

def emit docid, tuple
		puts "#{AGGREGATE_TYPE}:#{docid} #{tuple.join(' ')}\t1"	
end

STDIN.each do |line|
#	begin
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
#	rescue
		#STDERR.puts "problem with line [#{line.chomp}] ??"
#		raise "problem with line [#{line.chomp}] ??"
#	end
end
