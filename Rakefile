require 'zlib'

def run cmd
	puts Time.now
	puts cmd
	puts `#{cmd}`
end

=begin
getting weird error when using more than one reduce task..
HADOOP-6130
http://issues.apache.org/jira/browse/MAPREDUCE-735

when
-D mapred.reduce.tasks=4 -D mapred.map.tasks=4

in :trigram_frequency have to use 
-D stream.num.map.output.key.fields=2 -D mapred.text.key.partitioner.options=-k1,2

then get
2009-08-30 21:59:54,396 WARN org.apache.hadoop.streaming.PipeMapRed: java.lang.ArrayIndexOutOfBoundsException: 4
	at org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner.hashCode(KeyFieldBasedPartitioner.java:95)
	at org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner.getPartition(KeyFieldBasedPartitioner.java:87)
	at org.apache.hadoop.mapred.MapTask$MapOutputBuffer.collect(MapTask.java:801)
	at org.apache.hadoop.streaming.PipeMapRed$MROutputThread.run(PipeMapRed.java:378)

=end

def hadoop args
	input, output = [:input,:output].collect { |a| raise "no #{o} set" unless args[a]; args[a]}
	mapper, reducer = [:mapper,:reducer].collect { |a| args[a] || '/bin/cat' }
	run "hadoop fs -rmr #{output}" # when running against cluster
	run "rm -r #{output}"          # when running as single node
	cmd = [ "$HADOOP_HOME/bin/hadoop",
			"jar $HADOOP_HOME/contrib/streaming/hadoop-0.20.0-streaming.jar",
			"-D mapred.output.compress=true",
			"-D mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec"
#			"-D mapred.job.name=blah"
#			"-D mapred.reduce.tasks=4 ",
#			"-D mapred.map.tasks=4 "
			]
	cmd << args[:extra_D_flags] if args[:extra_D_flags]
	cmd += [
			"-output \"#{output}\" ",
			"-mapper #{mapper} ",
			"-reducer #{reducer}",
		]
	cmd << "-partitioner #{args[:partitioner]}" if args[:partitioner]
	input.split.each { |i| cmd << "-input \"#{i}\" " }
	cmd << args[:env_vars] if args[:env_vars]
	cmd.join(' ')
end

def spawn_prepare input, filename, output
	return unless filename
	fork {
		infile = Zlib::GzipReader.open("#{input}/#{filename}")
		outfile = Zlib::GzipWriter.new(File.new("#{output}/#{filename}",'w'))
		prefix = filename.sub '.txt.gz', ''
		outfile.printf "%s ", prefix
		infile.each do |line|
			outfile.printf "%s ", line.downcase.gsub(/\'/,'').gsub(/[^a-z0-9]/,' ').gsub(/\s+/,' ').strip
		end
		outfile.printf "\n"
		outfile.close
		infile.close
	}
end

desc "prepare files for upload from dir=input. outputs to hadoop_input"
task :prepare_files do
	raise "usage: insert_and_remove input=input_dir" unless ENV['input']
	input, output = ENV['input'], 'hadoop_input'
	run "rm -rf #{output} 2>/dev/null"
	run "mkdir #{output}"
	CORES = 4
	files = `ls #{input}`.split "\n"
	CORES.times { spawn_prepare input, files.shift, output }
	while not files.empty?
		Process.wait
		spawn_prepare input, files.shift, output 
	end
	Process.waitall
end

desc "cleanup and upload to hdfs a new input dir"
task :upload_input do
	run "hadoop fs -rmr input"
	run "hadoop fs -put hadoop_input input"
	run "hadoop fs -ls"
end

desc "cat dir/*gz from hdfs"
task :cat do
	raise "dir=?" unless ENV['dir']
	run "hadoop fs -cat #{ENV['dir']}/part* | gunzip "
end

def total_num_terms
	run "hadoop fs -get total_num_terms total_num_terms" # clumsy hack to ensure copy is always local
	cmd = "zcat total_num_terms/part* | perl -plne's/.*\t//'"
	`#{cmd}`.to_i
end

desc "calculate sips from term frequency"
task :term_frequency_calculate_sips => 
	[	:term_frequencies, :total_num_terms,
		:trigrams, :exploded_trigrams, 
		:trigram_frequency, :trigram_frequency_sum, :least_frequent_trigrams ]

task :term_frequencies do
	run hadoop(
		:input => "input", 
		:output => "term_frequencies", 
		:mapper => "/home/mat/dev/sip/emit_terms.rb",
		:reducer => "aggregate"
#		:extra_D_flags => '-D stream.num.map.output.key.fields=2'
		)
end

task :total_num_terms do
	run hadoop( 
		:input => "term_frequencies", 
		:output => "total_num_terms", 
		:mapper => "/home/mat/dev/sip/count_total_num_terms.rb",
		:reducer => "aggregate"
		)
end

task :trigrams do
	run hadoop( 
		:input => "input",
		:output => "trigrams", 
		:mapper => "/home/mat/dev/sip/emit_ngrams.rb",
		:reducer => "aggregate",
		:env_vars => "-cmdenv AGGREGATE_TYPE=UniqValueCount -cmdenv NGRAM_SIZE=3 -cmdenv INCLUDE_DOC_ID=true"
		)
end

task :exploded_trigrams do
	run hadoop(
		:input => "trigrams", 
		:output => "exploded_trigrams",
		:mapper => "/home/mat/dev/sip/explode_ngrams.rb"
		)
end

task :trigram_lme_frequency do
	run hadoop(
		:input => "term_frequencies exploded_trigrams", 
		:output => "trigram_lme_frequency",
		:reducer => "/home/mat/dev/sip/join_trigram_frequency.rb",
#		:partitioner => "org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner",
#		:extra_D_flags => '-Dmap.output.key.field.separator=. -D stream.num.map.output.key.fields=2 -D mapred.text.key.partitioner.options=-k1 ',
		:env_vars => "-cmdenv TOTAL_NUM_TERMS=#{total_num_terms}"
		)
end

desc "calculate sips from markov chain"
task :markov_chain_calculate_sips => 
	[ :bigrams, :markov_chain_start_edges, :markov_chain
	]

task :bigrams do
	run hadoop(
		:input => "input",
		:output => "bigrams", 
		:mapper => "/home/mat/dev/sip/emit_ngrams.rb",
		:reducer => "aggregate",
		:env_vars => "-cmdenv AGGREGATE_TYPE=LongValueSum -cmdenv NGRAM_SIZE=2 -cmdenv INCLUDE_DOC_ID=false"
		)
end

task :markov_chain_start_edges do
	run hadoop(
		:input => "bigrams",
		:output =>"markov_chain_start_edges",
		:mapper =>"/home/mat/dev/sip/emit_first_component_as_key.rb"
		)
end

task :markov_chain do
	run hadoop(
		:input => "term_frequencies markov_chain_start_edges",
		:output => "markov_chain",
		:reducer => "/home/mat/dev/sip/join_markov_chain.rb"
		)
end

task :trigrams_exploded_as_bigrams do
	run hadoop(
		:input => "trigrams",
		:output => "trigrams_exploded_as_bigrams", 
		:mapper => "/home/mat/dev/sip/explode_trigrams_as_bigrams.rb"
		)
end

task :trigram_markov_frequency do
	run hadoop(
		:input => "markov_chain trigrams_exploded_as_bigrams", 
		:output => "trigram_markov_frequency",
		:reducer => "/home/mat/dev/sip/join_trigram_markov_frequency.rb"
		)
end

task :trigram_frequency_sum do
	run hadoop(
		:input => "trigram_lme_frequency trigram_markov_frequency", 
		:output => "trigram_frequency_sum",
		:mapper => "/home/mat/dev/sip/double_value_sum.rb",
		:reducer => "aggregate"
		)
end

task :least_frequent_trigrams do
	run hadoop( 
		:input => "trigram_frequency_sum",
		:output => "least_frequent_trigrams",
		:mapper => "/home/mat/dev/sip/least_frequent_trigrams_map.rb",
		:reducer => "/home/mat/dev/sip/least_frequent_trigrams_reduce.rb"
		)
end


