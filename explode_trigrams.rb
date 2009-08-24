#!/usr/bin/env ruby
STDIN.each do |record|
	record =~ /(.*?)\t(.*)/
	data = $1
	components = data.split
	components.shift # doc id
	components.each { |c| puts "#{c} t\t#{data}" }
end
