require 'zlib'

def run cmd
	puts Time.now
	puts cmd
	puts `#{cmd}`
	raise "error running last command!!!" if ! cmd =~ /^-rmr/ and $?!=0
end

B = File.dirname(__FILE__)

def hadoop_version
	@@version ||= determine_hadoop_version
end
def determine_hadoop_version
	cmd = 'hadoop version | grep ^Hadoop | sed -es/Hadoop\ //'
	`#{cmd}`.chomp
end

def version_specific_property_seperator
	return hadoop_version=='0.18.3' ? '-jobconf' : '-D'
end

def hadoop args
	input, output = [:input,:output].collect { |a| raise "no #{o} set" unless args[a]; args[a]}
	mapper, reducer = [:mapper,:reducer].collect { |a| args[a] || '/bin/cat' }

	run "hadoop fs -rmr \"sip/#{output}\" 2>/dev/null" # when running against cluster
	run "rm -r \"sip/#{output}\" 2>/dev/null"          # when running as single node

	cmd = [ "hadoop","jar $HADOOP_HOME/contrib/streaming/hadoop-*-streaming.jar"]

	# props need to be here for at least v0.19
	# for v0.18 they need to be at end
	props = []
	if args[:join]
			props += ["stream.map.output.field.seperator=.", "stream.num.map.output.key.fields=2", "map.output.key.field.separator=."]
			props << "num.key.fields.for.partition=1" # hadoop 0.18.3
			props << "mapred.text.key.partitioner.options=-k1,1" # hadoop 0.19+
	end
	props += ["mapred.output.compress=true", "mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec"]
	props += ["mapred.reduce.tasks=10", "mapred.map.tasks=10"] # don't forget, ignore when running in local mode
	props.each { |prop| cmd << "#{version_specific_property_seperator} #{prop}" }

	input.split.each { |i| cmd << "-input \"sip/#{i}\" " }
	cmd += [
			"-output \"sip/#{output}\" ",
			"-mapper \"#{mapper}\" ",
			"-reducer \"#{reducer}\"",
		]

	cmd << "-partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner" if args[:join]

	cmd << "-file \"#{mapper}\"" if mapper =~ /rb$/
	cmd << "-file \"#{reducer}\"" if reducer =~ /rb$/
	
	args[:extra_files].each { |f| cmd << "-file \"#{B}/#{f}\"" }	if args[:extra_files]

#	cmd << "-verbose"

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
	run "hadoop fs -ls sip/input"
end

desc "cat dir/*gz from hdfs"
task :cat do
	raise "dir=?" unless ENV['dir']
	run "hadoop fs -cat #{ENV['dir']}/part* | gunzip "
end

def total_num_terms
	run "rm -rf total_num_terms "
	run "hadoop fs -get sip/total_num_terms total_num_terms" # clumsy hack to ensure copy is always local
	run "gunzip total_num_terms/*gz" # support either gz or not
	cmd = "cat total_num_terms/part* | perl -plne's/.*\t//'"
	result = `#{cmd}`
	raise "couldn't determine total_num_terms?" if result.empty?
	result.to_i
end

desc "calculate sips"
task :calculate_sips => [	# arbitrary topological sorted order
		:term_freq, :trigrams, :bigrams, 
		:total_num_terms, :exploded_trigrams, :bigram_keyed_by_first_elem, :bigram_first_elem_frequency,
		:trigram_mle_freq, :trigrams_exploded_as_bigrams, :markov_chain,
		:trigram_markov_frequency, :trigram_frequency_sum,
		:least_frequent_trigrams
	]

task :term_freq do
	run hadoop(
		:input => "input", 
		:output => "term_freq", 
		:mapper => "#{B}/emit_terms.rb",
		:reducer => "aggregate"
		)
end

task :total_num_terms do
	run hadoop( 
		:input => "term_freq", 
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

task :trigram_mle_freq do
	run hadoop(
		:input => "term_freq exploded_trigrams", 
		:output => "trigram_mle_freq",
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

task :bigram_first_elem_freq do
	run hadoop(
		:input => "bigrams",
		:output => "bigram_first_elem_freq",
		:mapper => "#{B}/first_component_freq.rb",
		:reducer => "aggregate"
		)
end

task :markov_chain do
	run hadoop(
		:input => "bigram_first_elem_freq bigram_keyed_by_first_elem",
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

task :trigram_markov_freq do
	run hadoop(
		:input => "markov_chain trigrams_exploded_as_bigrams", 
		:output => "trigram_markov_freq",
		:reducer => "#{B}/join_trigram_markov_frequency.rb",
		:join => true
		)
end

task :trigram_freq_sum do
	run hadoop(
		:input => "trigram_mle_freq trigram_markov_freq", 
		:output => "trigram_freq_sum",
		:mapper => "#{B}/double_value_sum.rb",
		:reducer => "aggregate"
		)
end

task :least_freq_trigrams do
	run hadoop( 
		:input => "trigram_freq_sum",
		:output => "least_freq_trigrams",
		:mapper => "#{B}/least_frequent_trigrams_map.rb",
		:reducer => "#{B}/least_frequent_trigrams_reduce.rb",
		:extra_files => ["top_n.rb"]
		)
end


