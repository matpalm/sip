
def run cmd
	puts Time.now
	puts cmd
	puts `#{cmd}`
end

def hadoop input, output, mapper, reducer
	run "hadoop fs -rmr #{output}"
	run "rm -r #{output}"
	cmd = [ "$HADOOP_HOME/bin/hadoop",
			"jar $HADOOP_HOME/contrib/streaming/hadoop-0.20.0-streaming.jar",
			"-D mapred.output.compress=true",
			"-D mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec",
			"-output \"#{output}\" ",
			"-mapper #{mapper} ",
			"-reducer #{reducer}"
		]
	input.split.each { |i| cmd << "-input \"#{i}\" " }
	cmd.join(' ')	
end

desc "insert_filename_at_start_and_remove_blanks input=input. outputs to hadoop_input"
task :insert_filename_at_start_and_remove_blanks do
	raise "usage: insert_and_remove input=input_dir" unless ENV['input']
	input,output = ENV['input'], 'hadoop_input'
	run "rm -rf #{output}"
	run "mkdir #{output}"
	`ls #{input}`.each do |filename|
		filename.chomp!
		prefix = filename.sub '.txt.gz', ''
		run "zcat #{input}/#{filename} | perl -ne'next if /^\\s+$/;s/^/#{prefix} /;print $_' | gzip - > #{output}/#{filename}"
	end
end

desc "cleanup and upload to hdfs a new input dir"
task :upload_input do
	run "hadoop fs -rmr input"
	run "hadoop fs -put hadoop_input input"
	run "hadoop fs -ls"
end

desc "run everything sans upload_input"
task :calc_least_frequent_trigram => 
	[	:term_frequencies, 
		:trigrams, :exploded_trigrams, 
		:trigram_frequency, :trigram_frequency_sum, :least_frequent_trigram ]

task :term_frequencies do
	run hadoop "input", "term_frequencies", 
		"/home/mat/dev/sip/emit_terms.rb",
		"aggregate"
end

task :trigrams do
	run hadoop "input", "trigrams", 
		"/home/mat/dev/sip/emit_unique_ngrams.rb",
		"aggregate"
end

task :exploded_trigrams do
	run hadoop "trigrams", "exploded_trigrams",
		"/home/mat/dev/sip/explode_trigrams.rb",
		"/bin/cat"
end

task :trigram_frequency do
	run hadoop "term_frequencies exploded_trigrams", "trigram_frequency",
		"/bin/cat",
		"/home/mat/dev/sip/join_trigram_frequency.rb"
end

task :trigram_frequency_sum do
	run hadoop "trigram_frequency", "trigram_frequency_sum",
		"/home/mat/dev/sip/double_value_sum.rb",
		"aggregate"
end

task :least_frequent_trigram do
	run hadoop "trigram_frequency_sum", "least_frequent_trigram",
		"/home/mat/dev/sip/least_frequent_trigram_map.rb",
		"/home/mat/dev/sip/least_frequent_trigram_reduce.rb"
end

desc "cat DIR/*gz from hdfs"
task :cat do
	raise "DIR=?" unless ENV['DIR']
	run "hadoop fs -cat #{ENV['DIR']}/part* | gunzip "
end

