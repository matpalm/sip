#!/usr/bin/env ruby
STDIN.each do |record|
	record =~ /(.*?)\t(.*)/
	data,count = $1,$2
	components = data.split
	components.shift # doc id
	puts "#{components.first} 1f\t#{data} #{count}" 
end
