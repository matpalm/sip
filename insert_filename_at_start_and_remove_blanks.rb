#!/usr/bin/env ruby
raise "usage: insert_and_remove.rb input_dir output_dir" unless ARGV.length==2
input,output = ARGV.collect {|d| d.strip.chomp}
`mkdir #{output}`
`ls #{input}`.each do |filename|
	filename.chomp!
	prefix = filename.sub '.txt.gz', ''
	cmd = "zcat #{input}/#{filename} | perl -ne'next if /^\\s+$/;s/^/#{prefix} /;print $_' | gzip - > #{output}/#{filename}"
	puts cmd
	puts `#{cmd}`
end

