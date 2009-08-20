#!/usr/bin/env ruby

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

INPUT = "input"

run hadoop INPUT,
	"term_freq", 
	"/home/mat/dev/sip/emit_terms.rb",
	"aggregate"

=begin
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

