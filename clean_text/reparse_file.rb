#!/usr/bin/env ruby
# take preamble and post amble off prj gut files
# do this by... 
# look for the biggest gap for lines with 'project gutenburg'
# tighten lower bound by scanning forward to the first non empty line after the bound
# tighten upper bound by scanning backwards to to the first non empty line before the bound
# do this same bounds checking for 'etext'
# choose the lower bound as the max of the 'prjgut' and 'etext' bounds
# choose the upper bound as the min of the 'prjgut' and 'etext' bounds
# output lines between bounds

class Array
	def upper 
		self[0]
	end
	def lower
		self[1]
	end
end

class BoundsFinder

	def initialize file_name, stream
		@file = file_name
		@lines = stream.readlines
		@lower_bound = @upper_bound = 0
	end

	def tighten_bound bound, delta
		while @lines[bound].chomp.strip != ''
			bound += delta
		end
		while @lines[bound].chomp.strip == ''
			bound += delta
		end
		bound
	end

	def bounds_matching regex

		lines_matching_regex = [1]
		@lines.each_with_index do |line, index|
				lines_matching_regex << index if line =~ regex
		end
		lines_matching_regex << @lines.size-1
		#puts lines_matching_regex.inspect
		return if lines_matching_regex.empty?

		lower_bound = upper_bound = biggest_gap = last_line_no = 0
		lines_matching_regex.each do |line_no|
			gap = line_no - last_line_no
			if gap > biggest_gap
				biggest_gap = gap
				lower_bound = last_line_no
				upper_bound = line_no
			end
			last_line_no = line_no
		end

		@lower_bound = tighten_bound lower_bound, +1		
		@upper_bound = tighten_bound upper_bound, -1		

	end

	def write_to file_name
		out = File.open(file_name,'w')		
		#puts "writing #{@lower_bound} -> #{@upper_bound}" 
		(@lower_bound..@upper_bound).each do |line_no|
			out.puts @lines[line_no]
		end
		out.close
	end

end


raise "reparse in_file, out_file" unless ARGV.length==2
in_file, out_file = ARGV

bc = BoundsFinder.new in_file, File.open(in_file,'r')
bc.bounds_matching /project gutenberg/i
bc.write_to out_file
	
