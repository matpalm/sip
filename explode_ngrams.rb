#!/usr/bin/env ruby
STDIN.each do |record|
	record =~ /(.*?)\t(.*)/
	data,count = $1,$2
	components = data.split
	components.shift # doc id
	components.each { |c| puts "#{c} 1f\t#{data} #{count}" }
end
