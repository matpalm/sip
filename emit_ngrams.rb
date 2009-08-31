#!/usr/bin/env ruby

['AGGREGATE_TYPE', 'NGRAM_SIZE', 'INCLUDE_DOC_ID'].each do |p|
	raise "#{p} env var not set" unless ENV[p]
end

AGGREGATE_TYPE = ENV['AGGREGATE_TYPE']
NGRAM_SIZE = ENV['NGRAM_SIZE'].to_i
INCLUDE_DOC_ID = ENV['INCLUDE_DOC_ID']=='true'

def emit docid, tuple
		printf "#{AGGREGATE_TYPE}:"
		printf "#{docid} " if INCLUDE_DOC_ID
		printf "#{tuple.join(' ')}\t1\n"	
end

STDIN.each do |line|
#	begin
		line =~ /(.*?) (.*)/	
		file, data = $1, $2

		terms = data.split
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
