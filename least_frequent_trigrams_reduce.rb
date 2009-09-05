#!/usr/bin/env ruby
require "#{File.dirname(__FILE__)}/top_n.rb"

processing_doc = nil
top_n = nil
STDIN.each do |record|
	record =~ /(.*)\t(.*?) (.*)/
	doc,freq,trigram = $1, $2.to_f, $3
	if doc != processing_doc		
		puts "#{processing_doc}\t#{top_n.top.inspect}" if processing_doc != nil
		processing_doc = doc
		top_n = TopN.new 10
	else
		top_n.add trigram, freq
	end
end			

puts "#{processing_doc}\t#{top_n.top.inspect}"

