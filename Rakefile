
def run cmd
	puts Time.now
	puts cmd
	puts `#{cmd}`
end

def hadoop input, output, mapper, reducer
	run "hadoop fs -rmr #{output}"
	[ "$HADOOP_HOME/bin/hadoop",
			"jar $HADOOP_HOME/contrib/streaming/hadoop-0.20.0-streaming.jar",
			"-D mapred.output.compress=true",
			"-D mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec",
			"-input \"#{input}\" ",
			"-output \"#{output}\" ",
			"-mapper #{mapper} ",
			"-reducer #{reducer}"
		].join(' ')	
end

desc "insert_filename_at_start_and_remove_blanks input=input. outputs to hadoop_input"
task :insert_filename_at_start_and_remove_blanks do
	raise "usage: insert_and_remove input=input_dir" unless ENV['input']
	input,output = ENV['input'], 'hadoop_input'
	`mkdir #{output}`
	`ls #{input}`.each do |filename|
		filename.chomp!
		prefix = filename.sub '.txt.gz', ''
		cmd = "zcat #{input}/#{filename} | perl -ne'next if /^\\s+$/;s/^/#{prefix} /;print $_' | gzip - > #{output}/#{filename}"
		puts cmd
		puts `#{cmd}`
	end
end

desc "cleanup and upload to hdfs a new input dir"
task :upload_input do
	run "hadoop fs -rmr hadoop_input"
	run "hadoop fs -put hadoop_input input"
	run "hadoop fs -ls"
end

desc "term frequencies"
task :term_frequencies do
	run hadoop "input", "term_frequencies", 
		"/home/mat/dev/sip/emit_terms.rb",
		"aggregate"
	run "hadoop fs -ls"
end



=begin

run hadoop INPUT,
	"term_freq", 
	"/home/mat/dev/sip/emit_terms.rb",
	"aggregate"

run hadoop INPUT,
	"bigrams", 
	"/home/mat/dev/sip/emit_ngrams.rb",
	"aggregate"

hadoop "bigrams",
	"term_exits",
	"/home/mat/dev/hadoop_test/emit_first_term_as_key.rb",
	"/home/mat/dev/hadoop_test/collect_if_key_same.rb"
hadoop INPUT,
	"start_stop_states", 
	"/home/mat/dev/hadoop_test/emit_start_stop_states.rb",
	"/bin/cat"
=end

