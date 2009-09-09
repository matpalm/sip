#!/usr/bin/env ruby
def reparse path
	dir,file = path.split '/'
	cmd = "./reparse_file.rb #{dir}/#{file} etexts_unzipped_reparsed/#{file}"
	puts cmd
	fork do 
		out =  `#{cmd}` 
		puts out unless out.empty?
	end
end

CORES = 4
files = `ls etexts_unzipped_txts/*txt`.split "\n"
CORES.times { reparse files.shift }
while not files.empty?
	Process.wait
	reparse files.shift
end
Process.waitall
