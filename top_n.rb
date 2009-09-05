#!/usr/bin/env ruby

class TopN
	attr_reader :top

	def initialize num_to_keep
		@top = []
		@num_to_keep = num_to_keep	
	end

	def would_add? value
		@top.size < @num_to_keep || value < @max_value
	end

	def add key, value
		if @top.size < @num_to_keep
			@top << [key,value]
			resort
		elsif value < @max_value
			@top.last[0] = key
			@top.last[1] = value
			resort
		end
	end

	def resort
		@top = @top.sort{|a,b| a[1] <=> b[1]}
		@max_value = @top.last[1]
	end
end

=begin
t = TopN.new 5
id =0
[3,5,2,1,4,1,5,9,2,6].each do |e|
	t.add "e#{id}", e
	id += 1
end
=end


