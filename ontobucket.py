#!/usr/bin/python

""" **************************************************************************
	python ontobucket.py [owl ontology file path or URL]
	Ontology fetch of category matching bucket definitions for given agency ids.
 
 	Author: Damion Dooley

	Ontology() class __main__() reads in given ontology file via path or 
	URL and imports all ontology class terms, including labels, 
	definitions, and boolean axioms.  Output is produced as json or tabular tsv.
	
	The focus is on elaborating boolean axioms into their parts.

	REQUIREMENTS
	This script requires python module RDFLib.

	EXAMPLES
	Retrieve local file root-ontology.owl and establish dictionary of rules 
	(each a class) to boolean matching expressions. Here LEXMAPR_0000001 is root
	term of a particular agency branch of buckets in the lexmapr agency ontology
	https://webprotege.stanford.edu/#projects/ff881d2b-1d6b-42ba-82f2-cc66130c9467/edit/Classes

		> python ontobucket.py root-ontology.owl -r http://genepio.org/ontology/LEXMAPR_0000001 
	
	**************************************************************************
""" 

import json
import sys
import os
import optparse
import datetime

#from ontohelper import OntoHelper as oh
import ontohelper as oh

import rdflib
from rdflib.plugins.sparql import prepareQuery

# Do this, otherwise a warning appears on stdout: No handlers could be 
#found for logger "rdflib.term"
import logging; logging.basicConfig(level=logging.ERROR) 

def stop_err(msg, exit_code = 1):
	sys.stderr.write("%s\n" % msg)
	sys.exit(exit_code)



class MyParser(optparse.OptionParser):
	"""
	Allows formatted help info.  From http://stackoverflow.com/questions/1857346/python-optparse-how-to-include-additional-info-in-usage-output.
	"""
	def format_epilog(self, formatter):
		return self.epilog


class OntologyBuckets(object):
	"""


	"""

	CODE_VERSION = '0.0.3'
	TEST = 0 # = 1 to test a hardcoded small subset of .owl ontology rules.
	
	def __init__(self):

		self.onto_helper = oh.OntoHelper()
		self.timestamp = datetime.datetime.now()
	 
		self.queries = {

			##################################################################
			# Membership Rules are boolean expressions or single entities linked
			# via 'has member' relation between a parent_id entity and children.
			# 
			# This script returns all triples that immediately compose the
			# owl.subject (aka owl.restriction). Below is simplest case
			#
			#   <owl:Restriction>
            #      <owl:onProperty rdf:resource="obo:RO_0002351"/>
            #      <owl:someValuesFrom rdf:resource="obo:FOODON_00002196"/>
            #   </owl:Restriction>			
			#	...

			'report_mapping': rdflib.plugins.sparql.prepareQuery("""

				SELECT DISTINCT ?label ?parent_id ?subject ?predicate ?object
				WHERE {
					BIND (OBO:RO_0002351 as ?has_member).  # MIGRATE TO RO:has member
					
					?parent_id rdfs:subClassOf* ?root.
					?parent_id owl:equivalentClass ?subject.
					?parent_id rdfs:label ?label.
					{	?subject owl:onProperty ?has_member.
						?subject (owl:someValuesFrom | owl:qualifiedCardinality | owl:minQualifiedCardinality | owl:maxQualifiedCardinality) ?object.
					}
					UNION
					{	?subject (owl:intersectionOf | owl:unionOf |  owl:complementOf) ?object.
					}
					?subject ?predicate ?object.

				 } ORDER BY ?parent_id

			""", initNs = self.onto_helper.namespace),

			# Anything is retrieved here, including annotations
			'triple_by_subject': rdflib.plugins.sparql.prepareQuery("""

				SELECT DISTINCT ?predicate ?object
				WHERE {?subject ?predicate ?object}
				ORDER BY ?subject

			""", initNs = self.onto_helper.namespace),

			# This query focuses on restriction parts and weeds out unneeded annotations.
			'triple_by_relation': rdflib.plugins.sparql.prepareQuery("""

				SELECT DISTINCT ?predicate ?object
				WHERE {
					?subject (owl:onClass | owl:intersectionOf | owl:unionOf | owl:complementOf) ?object.
					?subject ?predicate ?object.
				}
				ORDER BY ?subject

			""", initNs = self.onto_helper.namespace),

		}

		"""
		The "PREDICATE_SET" dictionary has for its keys the set of predicates
		we can expect in a bucket matching expression ('has member' axiom). 
		The dictionary contains datatypes as keys that object's data type 
		should match to, and for the match, a response specific to that 
		datatype. Recursion is carried out where the object is a BNode (blank
		or anonymous node) that must be investigated further.

		NOTE: If ontobucket.py prints out an error of form "unrecognized 
		predicate [BIG LONG PREFIX]" and you need that predicate, then 
		ensure its prefix is added to ontohelper.py self.struct['@context'].

		"""
		self.PREDICATE_SET = {
			'owl:someValuesFrom': {
				str: self.get_component_object_text, # A string value / term id
				rdflib.term.BNode: self.get_component_BNode, 
			},
			# These cases all require further function to fetch content
			'owl:qualifiedCardinality': 	{dict: self.get_component_cardinality},
			'owl:minQualifiedCardinality':	{dict: self.get_component_cardinality},
			'owl:maxQualifiedCardinality':	{dict: self.get_component_cardinality},

			 # used in conjunction with cardinality
			 # SHORTCUT, otherwise self.get_component_BNode 
			'owl:onClass': {rdflib.term.BNode: self.get_component_set},
			'owl:intersectionOf': {rdflib.term.BNode: self.get_component_BNode},
			'owl:unionOf': {rdflib.term.BNode: self.get_component_BNode},
			'owl:complementOf': {
				str: self.get_component_object_text, # A negated ontology id,
				rdflib.term.BNode: self.get_component_BNode, #self.get_component_set # SHORTCUT????
			},

			# self.get_component_object_text, # would return 'has member' or whichever relation was involved
			'owl:onProperty': {str: self.get_component_blank}, 

			# These two indicate a collection (list of expressions)
			'rdf:first': {
				rdflib.term.BNode: self.get_component_BNode,
				str: self.get_component_object_text # The ontology id
			},
			'rdf:rest': {
				rdflib.term.BNode: self.get_component_BNode,
				str: self.get_component_blank # signals end of script
			},
			# Instance or Class.  Not really informative unless some 
			# distinction is being made about them.
			'rdf:type': {str: self.get_component_blank} 
		}


	def log(self, *args):
		"""
			Show log messages and differential time between calls
		"""
		timestamp = datetime.datetime.now()
		print("time delta: ", str(timestamp - self.timestamp), "\n", str(args))
		self.timestamp = timestamp


	def __main__(self):
		"""
		LexMapr Agency Reporting Module:

		Objective: trigger/activate agency bucket in response to presentation of (a bucket of) a set of lexmapr ontology hits.  Matching is most effective if all lexmapr hits AND THEIR ontology ancestor term ids are presented in a single set.  Each rule needs to be applied to this set in turn.

		"""
		(options, args) = self.get_command_line()

		if options.code_version:
			print (self.CODE_VERSION)
			return self.CODE_VERSION

		if not len(args):
			stop_err('Please supply an OWL ontology file (in RDF/XML format)')

		(main_ontology_file, output_file_basename) = self.onto_helper.check_ont_file(args[0], options)

		# Load main ontology file into RDF graph
		print ("Fetching and parsing " + main_ontology_file + " ...")

		try:
			# ISSUE: ontology file taken in as ascii; rdflib doesn't accept
			# utf-8 characters so can experience conversion issues in string
			# conversion stuff like .replace() below
			self.onto_helper.graph.parse(main_ontology_file, format='xml')

		except Exception as e:
			#urllib2.URLError: <urlopen error [Errno 8] nodename nor servname provided, or not known>
			stop_err('WARNING:' + main_ontology_file + " could not be loaded!\n", e)

		# Add each ontology include file (must be in OWL RDF format)
		self.onto_helper.do_ontology_includes(main_ontology_file)

		for term_id in options.root_uri.split(','):

			# THE ONE CALL TO GET REPORT CATEGORY BOOLEAN EXPRESSIONS
			self.log('bucket rule compilation for', term_id)

			# If user has provided a set of comparison ids then actually 
			# execute bucket rules on them and return a boolean result for
			# each rule.
			bucket_rules = self.do_membership_rules(term_id)

		# If output folder specified then write out bucket rule file 
		if (options.output_folder):
			
			self.onto_helper.do_output_json(bucket_rules, output_file_basename)

		self.log('bucket reporting')
		# FUTURE: ALTERNATELY, INPUT TSV FILE OR JSON WITH records of hits
		if options.comparison_ids:
			comparison_set = set(options.comparison_ids.split(','))
			for rule in bucket_rules:
				do_bucket_rule(copy.deepcopy(rule), comparison_set)


	def do_bucket_rule(self, rule, comparison_set):
		"""
		The first parameter of a rule is one of the predicates. Remaining
		parameters are either cardinality restriction limits, or boolean
		set operators, or entity id references (strings).

		Picture the comparison_set as a class or instance having 'has member'
		relations to all its elements.  The rule expression is one or more
		tests of given elements against the comparison_set 'has member' items.
		"""
		rule_fn = rule.pop(0)

		# CARDINALITY SPECIFIES NUMBER OF ITEMS THAT CAN MATCH. USUALLY WITH 
		# CATEGORY MATCHING RULES one or more supporting (or negated) piece
		# of evidence is all we care about, but exact cardinality is also
		# enabled below.
		# Cardinality rules return boolean True / False, which means that 
		# parent term must work on boolean values.

		# 'member of' some ~= one or more 'member of ' relations to entities.
		if (rule_fn == 'owl:someValuesFrom'):
			return (len(do_collection(rule, comparison_set)) > 0)

		# Issue: check about recognition of combined min/max cardinality.
		if (rule_fn == 'owl:qualifiedCardinality'):
			limit = rule.pop(0)
			return (len(do_collection(rule, comparison_set)) == limit)

		if (rule_fn == 'owl:minQualifiedCardinality'):
			limit = rule.pop(0)
			return (len(do_collection(rule, comparison_set)) >= limit)

		if (rule_fn == 'owl:maxQualifiedCardinality'):
			limit = rule.pop(0)
			return (len(do_collection(rule, comparison_set)) <= limit)

		# Unused; 'owl:onProperty' not currently part of the rule syntax.
		#if (rule_fn == 'owl:onProperty'):
		#	return 

		# Matches to expressions like "a and b and c" but these would rarely
		# be entity references directly - if they were the constraint would be
		# that rule was indicating that member was simultaniously class a, b, c.
		# Instead, usually each entity would be an expression of predicate link 
		# 'has member' to some condition on presence or absense of member 
		# elements, i.e. more likely used in form of "(expression a) and 
		# (expression b)" where each expression is placing constraints on 
		# comparison_set elements.
		if (rule_fn == 'owl:intersectionOf'):
			bucket_set = set(do_collection(rule, comparison_set))
			return all( bucket_set.intersection(comparison_set) )

		# Matches to expressions like "a or b or c". Each can generate a True/
		# False hit on comparison_set.
		# JUST RETURN THE ITEMS THAT MATCHED.
		if (rule_fn == 'owl:unionOf'):
			bucket_set = set(do_collection(rule, comparison_set))
			return any( bucket_set.intersection(comparison_set) )

		# Matches to expressions like "not (a or b or c) ... " meaning
		# none of the target elements should be present in the comparison_set.
		if (rule_fn == 'owl:complementOf'):
			bucket_set = set(do_collection(rule, comparison_set))
			return not any(bucket_set)

		# Error condition, something should have matched above.
		return false 


	# A read-ahead would know which logical function was going to be applied
	# and could return on first instance of a true or false value.
	def do_collection(self, collection, comparison_set):
		for ptr in collection: # A list: 0, 1, 2... key
			item = collection[ptr]
			if type(item) == str:
				collection[ptr] = item in comparison_set
			else:
				collection[ptr] = do_bucket_rule(item, comparison_set)

		return collection


	def do_membership_rules(self, term_id):
		""" ####################################################################
			Membership Rules are boolean expressions or single entities linked
			via 'has member' relation between a parent_id entity and children.

			This script reports rule label and id (parent_id) and then sends
			triple containing "[member of] [cardinality]

			memberships_by_cardinality query returns just the cardinality part.
			From there we explore the rest of the guts.

			INPUTS
				?parent_id ?label ?subject ?predicate ?object

		"""

		specBinding = {'root': rdflib.URIRef(term_id)} 
		table = self.onto_helper.do_query_table(self.queries['report_mapping'], specBinding )

		print ("Buckets:", len(table))

		bucket_rules = {}

		# At this level each triple is a bucket-matching rule
		for triple in table: 
			# TEST EXAMPLES

			if self.TEST and self.TEST == 1 and not triple['parent_id'] in (['LEXMAPR:0000002', 'LEXMAPR:0000007', 'LEXMAPR:0000041']):
				continue
			
			bucket_rules[triple['parent_id']] = self.do_triple(triple)

			print (triple['label'], '('+triple['parent_id']+')', ":", self.do_triple(triple))

		return bucket_rules


	def do_triple(self, triple):
		"""
		Recursive processing of triples according to PREDICATE_SET rules.
		"""
		t_predicate = triple['predicate']

		if not t_predicate in self.PREDICATE_SET:
			print ("Unrecognized predicate", t_predicate, " in:", json.dumps(triple, sort_keys=False, indent=4, separators=(',', ': ')))
			return

		t_object = triple['object']
		t_object_type = type(t_object)
		object_type_set = self.PREDICATE_SET[t_predicate]

		if not t_object_type in object_type_set:
			print ("Unrecognized object for ", t_predicate,":",t_object_type, json.dumps(t_object, sort_keys=False, indent=4, separators=(',', ': ')))
			return

		t_triple_fn = object_type_set[t_object_type]
		return t_triple_fn(triple)


	def get_component_object_text(self, triple): 
		t_object = triple['object']
		if triple['predicate'] == 'rdf:first':
			return t_object

		return [triple['predicate'], t_object]


	def get_component_set(self, triple): 
		"""
		For the QualifiedCardinality nodes, rdflib adds a shortcut "expression"
		key-value dictionary which contains "datatype" [disjunction|conjunction]
		and a "data" key that is a list of ontology entity ids. This appears to
		be triggered by a rdf:parseType="Collection" attribute.

		HOWEVER, if Collection item is complex, does this shortcut still work?
		"""
		datatype = 'owl:unionOf' if triple['expression']['datatype'] else 'owl:intersectionOf'

		return [datatype] + triple['expression']['data']


	def get_component_cardinality(self, triple): 
		"""
		The cardinality cases all require 2nd query to fetch content
		"""
		result = [triple['predicate'], triple['object']['value']]

		triples = self.onto_helper.do_query_table(
			self.queries['triple_by_relation'], {'subject': triple['subject'] } # ON RESTRICTION
		)
		for bnode_triple in triples:
			bnode_result = self.do_triple(bnode_triple)
			result.append(bnode_result)

		return result # ISSUE: parts may be rdflib.term.BNode
		

	def get_component_BNode(self, triple):

		result = []

		# Find subordinate tripples that begin with triple's object.
		# Basically none of these are annotations
		triples = self.onto_helper.do_query_table(
			self.queries['triple_by_subject'], {'subject': triple['object'] }
		)
		for bnode_triple in triples:
			bnode_result = self.do_triple(bnode_triple) 
			if bnode_result:
				if type(bnode_result) == str:   # literal values
					result.append(bnode_result)
				elif bnode_result[0] in ['owl:intersectionOf','owl:unionOf','owl:complementOf']:
					result.append(bnode_result) # Append whole list [operator, value , value ...]
				else:
					result = result + bnode_result # merge arrays

 		# Skipping "rdf:first", "rdf:rest"
		if triple['predicate'] in ['owl:someValuesFrom','owl:intersectionOf','owl:unionOf','owl:complementOf']:
			return [triple['predicate']] + result

		return result


	def render_debug(self, triple):
		return ("DEBUG:", json.dumps(triple, sort_keys=False, indent=4, separators=(',', ': ')))


	def get_component_blank(self, triple):
		return None


	def get_command_line(self):
		"""
		*************************** Parse Command Line *****************************
		"""
		parser = MyParser(
			description = 'Ontology term fetch to tabular output.  See https://github.com/GenEpiO/genepio',
			usage = 'ontofetch.py [ontology file path or URL] [options]*',
			epilog="""  """)
		
		# first (unnamed) parameter is input file or URL
		# output to stdio unless -o provided in which case its to a file.

		# Standard code version identifier.
		parser.add_option('-v', '--version', dest='code_version', default=False, action='store_true', help='Return version of this code.')

		parser.add_option('-o', '--output', dest='output_folder', type='string', help='Path of output file to create')

		parser.add_option('-i', '--input', dest='comparison_ids', type='string', help='Comma separated list of term ids to match rules to.')

		parser.add_option('-r', '--root', dest='root_uri', type='string', help='Comma separated list of full URI root entity ids to fetch underlying terms from. Defaults to owl#Thing.', default='http://www.w3.org/2002/07/owl#Thing')

		return parser.parse_args()


if __name__ == '__main__':

	buckets = OntologyBuckets()
	buckets.__main__()  

