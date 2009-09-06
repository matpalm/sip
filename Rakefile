require 'zlib'

def run cmd
	puts Time.now
	puts cmd
	puts `#{cmd}`
	raise "error running last command!!!" if ! cmd =~ /^-rmr/ and $?!=0
end

B = File.dirname(__FILE__)

def hadoop args
	input, output = [:input,:output].collect { |a| raise "no #{o} set" unless args[a]; args[a]}
	mapper, reducer = [:mapper,:reducer].collect { |a| args[a] || '/bin/cat' }
	run "hadoop fs -rmr \"sip/#{output}\" 2>/dev/null" # when running against cluster
	run "rm -r \"sip/#{output}\" 2>/dev/null"          # when running as single node
	cmd = [ "$HADOOP_HOME/bin/hadoop",
			"jar $HADOOP_HOME/contrib/streaming/hadoop-0.20.0-streaming.jar",
			"-D mapred.output.compress=true",
			"-D mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec"
			]

	cmd << args[:extra_D_flags] if args[:extra_D_flags]
	cmd << "-D stream.num.map.output.key.fields=2 -D map.output.key.field.separator=. -D mapred.text.key.partitioner.options=-k1,1" if args[:join]

	input.split.each { |i| cmd << "-input \"sip/#{i}\" " }
	cmd += [
			"-output \"sip/#{output}\" ",
			"-mapper \"#{mapper}\" ",
			"-reducer \"#{reducer}\"",
		]

	cmd << "-partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner" if args[:join]

	cmd << "-file \"#{mapper}\"" if mapper =~ /rb$/
	cmd << "-file \"#{reducer}\"" if reducer =~ /rb$/
	
	args[:extra_files].each { |f| cmd << "-file \"#{B}/#{f}\"" }	

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
	run "hadoop fs -rmr sip"
	run "hadoop fs -mkdir sip"
	run "hadoop fs -put hadoop_input sip/input"
	run "hadoop fs -ls sip"
end

desc "cat dir/*gz from hdfs"
task :cat do
	raise "dir=?" unless ENV['dir']
	run "hadoop fs -cat #{ENV['dir']}/part* | gunzip "
end

def total_num_terms
	run "hadoop fs -get sip/total_num_terms total_num_terms" # clumsy hack to ensure copy is always local
	cmd = "zcat total_num_terms/part* | perl -plne's/.*\t//'"
	`#{cmd}`.to_i
end

desc "calculate sips"
task :calculate_sips => [	# arbitrary topological sorted order
		:term_frequencies, :trigrams, :bigrams, 
		:total_num_terms, :exploded_trigrams, :bigram_keyed_by_first_elem, :bigram_first_elem_frequency,
		:trigram_mle_frequency, :trigrams_exploded_as_bigrams, :markov_chain,
		:trigram_markov_frequency, :trigram_frequency_sum,
		:least_frequent_trigrams
	]

task :term_frequencies do
	run hadoop(
		:input => "input", 
		:output => "term_frequencies", 
		:mapper => "#{B}/emit_terms.rb",
		:reducer => "aggregate"
#		:extra_D_flags => '-D stream.num.map.output.key.fields=2'
		)
end

task :total_num_terms do
	run hadoop( 
		:input => "term_frequencies", 
		:output => "total_num_terms", 
		:mapper => "#{B}/count_total_num_terms.rb",
		:reducer => "aggregate"
		)
end

task :trigrams do
	run hadoop( 
		:input => "input",
		:output => "trigrams", 
		:mapper => "#{B}/emit_ngrams.rb",
		:reducer => "aggregate",
		:env_vars => "-cmdenv AGGREGATE_TYPE=UniqValueCount -cmdenv NGRAM_SIZE=3 -cmdenv INCLUDE_DOC_ID=true"
		)
end

task :exploded_trigrams do
	run hadoop(
		:input => "trigrams", 
		:output => "exploded_trigrams",
		:mapper => "#{B}/explode_ngrams.rb"
		)
end

task :trigram_mle_frequency do
	run hadoop(
		:input => "term_frequencies exploded_trigrams", 
		:output => "trigram_mle_frequency",
		:reducer => "#{B}/join_trigram_frequency.rb",
		:join => true,
		:env_vars => "-cmdenv TOTAL_NUM_TERMS=#{total_num_terms}" 
		)
end

task :bigrams do
	run hadoop(
		:input => "input",
		:output => "bigrams", 
		:mapper => "#{B}/emit_ngrams.rb",
		:reducer => "aggregate",
		:env_vars => "-cmdenv AGGREGATE_TYPE=LongValueSum -cmdenv NGRAM_SIZE=2 -cmdenv INCLUDE_DOC_ID=false"
		)
end

task :bigram_keyed_by_first_elem do
	run hadoop(
		:input => "bigrams",
		:output =>"bigram_keyed_by_first_elem",
		:mapper =>"#{B}/emit_first_component_as_key.rb"
		)
end

task :bigram_first_elem_frequency do
	run hadoop(
		:input => "bigrams",
		:output => "bigram_first_elem_frequency",
		:mapper => "#{B}/first_component_freq.rb",
		:reducer => "aggregate"
		)
end

task :markov_chain do
	run hadoop(
		:input => "bigram_first_elem_frequency bigram_keyed_by_first_elem",
		:output => "markov_chain",
		:reducer => "#{B}/join_markov_chain.rb",
		:join => true
		)
end

task :trigrams_exploded_as_bigrams do
	run hadoop(
		:input => "trigrams",
		:output => "trigrams_exploded_as_bigrams", 
		:mapper => "#{B}/explode_trigrams_as_bigrams.rb"
		)
end

task :trigram_markov_frequency do
	run hadoop(
		:input => "markov_chain trigrams_exploded_as_bigrams", 
		:output => "trigram_markov_frequency",
		:reducer => "#{B}/join_trigram_markov_frequency.rb",
		:join => true
		)
end

task :trigram_frequency_sum do
	run hadoop(
		:input => "trigram_mle_frequency trigram_markov_frequency", 
		:output => "trigram_frequency_sum",
		:mapper => "#{B}/double_value_sum.rb",
		:reducer => "aggregate"
		)
end

task :least_frequent_trigrams do
	run hadoop( 
		:input => "trigram_frequency_sum",
		:output => "least_frequent_trigrams",
		:mapper => "#{B}/least_frequent_trigrams_map.rb",
		:reducer => "#{B}/least_frequent_trigrams_reduce.rb",
		:extra_files => ["top_n.rb"]
		)
end


