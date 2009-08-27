#!/usr/bin/env ruby
STDIN.each do |record|
	record =~ /(.*?) (.*)\t(.*)/
	doc,trigram,freq = $1,$2,$3
	puts "#{doc}\t#{freq} #{trigram}"
end
