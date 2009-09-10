#!/usr/bin/env ruby
require "#{File.dirname(__FILE__)}/top_n.rb"
require 'set'

class Array

	def trigrams
		return unless length >= 3
		tuple = []
		idx = 0
		3.times { tuple << self[idx]; idx+=1 }
		seen = Set.new
		yield tuple
		seen << tuple.clone
		while idx < length
			tuple.shift
			tuple << self[idx]
			idx += 1			
			next if seen.include? tuple
			yield tuple
			seen << tuple.clone
		end
	end

	def sum
		inject{|a,v| a+v}
	end

	def mean
		sum.to_f / length
	end

end

class TermToIdx

	def initialize
		@term_to_idx = {}
		@idx_to_term = {}
		@seq = 0
	end

	def index_for term
		idx = @term_to_idx[term]
		return idx unless idx.nil?
		@seq += 1
		@term_to_idx[term] = @seq
		@idx_to_term[@seq] = term
		@seq		
	end

	def terms_to_ids terms
		terms.collect {|t| index_for t}
	end

	def ids_to_terms ids
		ids.collect {|id| @idx_to_term[id] } 
	end

end

class TermFreq

	attr_reader :term_freq

	def initialize
		@term_freq = {}
		@term_freq.default = 0
		@total_terms = 0
		@dirty = true
	end

	def add_term term
		raise "faildog! can add after call to mle" unless @dirty
		@term_freq[term] += 1
		@total_terms += 1
		#puts "added #{term} @total=#{@total_terms} @term_freq=#{@term_freq.inspect}"
	end

	def normalise
		@term_freq.keys.each do |term|
			@term_freq[term] = Math.log( @term_freq[term].to_f / @total_terms )
		end
		@dirty = false
	end

	def maximum_likelihood_estimate trigram
		normalise if @dirty
		trigram.collect {|t| @term_freq[t]}
	end

	def total_num_terms
		@total_terms	
	end

	def unique_terms
		@term_freq.keys.size
	end

end

class MarkovChain

	attr_reader :transistions

	def initialize
		@transistions = {}
		@dirty = true
	end	

	def add_edge from, to
		raise "failcat! can add after call to prob" unless @dirty
		@transistions[from] ||= {}
		@transistions[from][to] ||= 0
		@transistions[from][to] += 1
	end

	def normalise
		@transistions.keys.each do |from|
			edges = @transistions[from]
			total_outbound_for_node = edges.values.sum
			edges.keys.each do |to|
				edges[to] = Math.log( edges[to].to_f / total_outbound_for_node )
			end
		end
		@dirty = false
	end

	def prob trigram
		normalise if @dirty
		t1,t2,t3 = trigram
		[@transistions[t1][t2], @transistions[t2][t3]]
	end

end

term_to_idx = TermToIdx.new

# docid => list of term_ids; 
# { :d56prm => [1,3,2,4] }
documents = {}

# slurp in documents and convert to ids
STDIN.each do |record|
	terms = record.split
	doc_id = terms.shift
	documents[doc_id.to_sym] = term_to_idx.terms_to_ids terms
end

# build term frequency table and markov chain
term_freq = TermFreq.new
markov_chain = MarkovChain.new
documents.values.each do |terms|
	last_term = nil
	terms.each do |term|
		term_freq.add_term term
		markov_chain.add_edge last_term, term unless last_term.nil?
		last_term = term
	end
end

def prob_of sip_mle, sip_markov
	sip_mle.mean + sip_markov.mean  # mean of means (*2)
#	(sip_mle.sum + sip_markov.sum) / (sip_mle.length + sip_markov.length) # mean
#	sip_mle.sum + sip_markov.sum # sumtastic
end

#puts "total_num_terms #{term_freq.total_num_terms}"
#puts "unique_terms #{term_freq.unique_terms}"

# iterate over each document's trigrams remembering the least likely
documents.each do |doc_id, terms|	
	top_sips = TopN.new 10
	terms.trigrams do |trigram|
		sip_mle = term_freq.maximum_likelihood_estimate trigram
		sip_markov = markov_chain.prob trigram
		sip = prob_of sip_mle, sip_markov		
#		puts "analysing #{doc_id} #{trigram.inspect}/\"#{term_to_idx.ids_to_terms(trigram).join(' ')}\" sip_mle=#{sip_mle.inspect} sip_markov=#{sip_markov.inspect} sip=#{sip}"
		next unless top_sips.would_add? sip
		trigram_as_terms = term_to_idx.ids_to_terms(trigram).join(' ')
		top_sips.add trigram_as_terms, sip
	end
	puts "#{doc_id}\t#{top_sips.keys.inspect}"
end
