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

	if --cache used, then --output folder is required in order to read where
	cached [ontology].json file is.

	EXAMPLES
	Retrieve local file root-ontology.owl and establish dictionary of rules 
	(each a class) to boolean matching expressions. Here LEXMAPR_0000001 is root
	term of a particular agency branch of buckets in the lexmapr agency ontology
	https://webprotege.stanford.edu/#projects/ff881d2b-1d6b-42ba-82f2-cc66130c9467/edit/Classes

		> python ontobucket.py root-ontology.owl -r http://genepio.org/ontology/LEXMAPR_0000001 
	
	TEST CASES
		> python ontobucket.py ../lexmapr_ontology/lexmapr.owl -r http://genepio.org/ontology/LEXMAPR_0000001 -i FOODON:00001286
	I.e. "turkey meat food product" should lead to only:	
		LEXMAPR:0000073 True


	**************************************************************************
""" 

import json
import sys
import os
import optparse
import datetime
from copy import deepcopy

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
		should match to, and for the match, a function specific to that 
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
			'owl:onClass': {rdflib.term.BNode: self.get_component_set}, # always BNode
			'owl:intersectionOf': {
				str: self.get_component_object_text,
				rdflib.term.BNode: self.get_component_BNode
				},
			'owl:unionOf': {
				str: self.get_component_object_text,
				rdflib.term.BNode: self.get_component_BNode
				},
			'owl:complementOf': {
				str: self.get_component_object_text, # A negated ontology id, I.e. ensure comparison_set doesn't have this element or child.
				rdflib.term.BNode: self.get_component_BNode, #self.get_component_set # SHORTCUT????
			},

			# self.get_component_object_text, # would return 'has member' or whichever relation was involved
			'owl:onProperty': {str: self.get_component_blank}, 

			# These two indicate a collection (list of expressions)
			'rdf:first': {
				str: self.get_component_object_text, # The ontology id
				rdflib.term.BNode: self.get_component_BNode
			},
			'rdf:rest': {
				str: self.get_component_blank, # signals end of list
				rdflib.term.BNode: self.get_component_BNode

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

		cached_rules = False;

		if options.cache:
			# If there is a cached file to use, go for it, otherwise will have to generate it.
			if options.output_folder:
				# Output rule file takes on ontology name + .json
				json_file_path = output_file_basename + '.json'

				if os.path.isfile('./' + json_file_path):
					with (open(json_file_path)) as input_handle:
						self.log("Using cached file:", json_file_path)
						bucket_rules = json.load(input_handle);
						cached_rules = True;

			else:
				stop_err('If using the cache flag, you must specify an output folder to read .json file from (or regenerate it to)')

		if not cached_rules: 

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

		self.log('Bucket reporting')

		# FUTURE: ALTERNATELY, INPUT TSV FILE OR JSON WITH records of hits
		if options.comparison_ids:
			comparison_set = set(options.comparison_ids.split(','))
			for bucket_id, rule in bucket_rules.items():
				output = self.do_bucket_rule(rule, comparison_set)
				if output != {False}:
					print ("RULE:",bucket_id, output)

	owl_rules = {

		# CARDINALITY SPECIFIES NUMBER OF ITEMS THAT CAN MATCH. USUALLY WITH 
		# CATEGORY MATCHING RULES one or more supporting (or negated) piece
		# of evidence is all we care about, but exact cardinality is also
		# enabled below.
		# Cardinality rules return boolean True / False, which means that 
		# parent term must work on boolean values.

		# 'member of' some ~= one or more 'member of ' relations to entities.
		'owl:someValuesFrom': 
			lambda self, rule, comparison_set: 
				self.atLeastOne(rule[1], comparison_set),
 
		# Issue: check about recognition of combined min/max cardinality.
		'owl:qualifiedCardinality':
			lambda self, rule, comparison_set: 
				len(self.do_collection(rule[2], comparison_set)) == rule[1],

		'owl:minQualifiedCardinality': 
			lambda self, rule, comparison_set: 
				len(self.do_collection(rule[2], comparison_set)) >= rule[1],

		'owl:maxQualifiedCardinality': 
			lambda self, rule, comparison_set: 
				len(self.do_collection(rule[2], comparison_set)) <= rule[1],

		# Unused; 'owl:onProperty' not currently part of the rule syntax.
		#if (rule_fn == 'owl:onProperty'):
		#	return 

		# Matches to expressions like "(a and b and c)" but these would rarely
		# be entity references directly - if they were the constraint would be
		# that rule was indicating that member was simultaniously class a, b, c.
		# Instead, usually each entity would be an expression of predicate link 
		# 'has member' to some condition on presence or absense of member 
		# elements, i.e. more likely used in form of "(expression a) and 
		# (expression b)" where each expression is placing constraints on 
		# comparison_set elements.
		'owl:intersectionOf':
			lambda self, rule, comparison_set: 
				self.intersection(rule, comparison_set),

		# Matches to expressions like "(a or b or c)". Each can generate a True/
		# False hit on comparison_set.
		# RETURN JUST ITEMS THAT ARE COMMON TO BOTH SETS.
		'owl:unionOf':
			lambda self, rule, comparison_set: 
				self.do_bucket_rule(rule[1], comparison_set),

		# Matches to expressions like "not (a or b or c) ... " meaning
		# none of the target elements should be present in the comparison_set.
		'owl:complementOf':
			lambda self, rule, comparison_set: 
				set([True]) if not any(set(self.do_collection(rule[1], comparison_set)) ) else set([False])
				# Ideally just return the leaf element, not its ancestors.


	}

	def atLeastOne(self, rule, comparison_set):
		output_set = self.do_bucket_rule(rule, comparison_set)
		output_set.discard(False)
		#print("atLeastOne", output_set)
		if len(output_set) > 0:
			return output_set
		else:
			return set([False])

	def intersection(self, rule, comparison_set):

		intermediate = self.do_collection(rule[1:], comparison_set);
		intersect = intermediate.intersection(comparison_set);
		if len(intermediate) == len(intersect):
			return intersect
		else:
			return set([False])


	def do_bucket_rule(self, rule, comparison_set):
		"""
		The first parameter of a rule is one of the predicates. Remaining
		parameters are either cardinality restriction limits, or boolean
		set operators, or entity ids (strings).

		Picture the comparison_set as a class or instance having 'has member'
		relations to all its elements.  The rule expression is one or more
		tests of given elements against the comparison_set 'has member' items.

		OUTPUT: set() containing matching ids, or None elements.
		"""
		#print("Doing rule", rule)

		item = rule[0]

		if item in self.owl_rules:
			return self.owl_rules[item](self, rule, comparison_set)

		# Here we've hit expression that doesn't begin with a function
		# so it must simply return structure back to calling function.
		if type(item) == str:
			return self.do_collection(rule, comparison_set)

		# Error condition, something should have matched above.
		self.log("Error: unrecognized rule part", item)

		return False 


	# Limited implementation of lookahead function which will shortcut 
	# evaluation for some logical functions, e.g. first true of a
	# disjunction is returned.
	def do_collection(self, collection, comparison_set, lookahead = None):
		output = set()
		# self.log("doing collection:", collection)
		for item in collection: # A list: 0, 1, 2... key
			#print ("At", item)
			if type(item) == str:
				if (item in comparison_set):
					result = item;
					# if lookahead == 'owl:someValuesFrom':
					#	return set(item);
				else: result = False;
				output.add(result)
				
			else:
				# A list or some other data structure, with first term likely
				# some operator like unionOf
				result = self.do_bucket_rule(item, comparison_set)

				# ISSUE: SomeValuesFrom is inside an expression, usually UnionOf ...
				#if lookahead == 'owl:someValuesFrom':
				#	return result;
				output.union(result) 

			#print ('output', output)

		return output


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
	def do_membership_rules(self, term_id):

		specBinding = {'root': rdflib.URIRef(term_id)} 
		table = self.onto_helper.do_query_table(self.queries['report_mapping'], specBinding )

		print ("Buckets:", len(table))

		bucket_rules = {}

		# At this level each triple is a bucket-matching rule
		for triple in table: 
			# TEST EXAMPLES

			if self.TEST == 1 and not triple['parent_id'] in (['LEXMAPR:0000002', 'LEXMAPR:0000007', 'LEXMAPR:0000041']):
				continue
			
			bucket_rules[triple['parent_id']] = self.do_triple(triple)

			if self.TEST == 1:
				print (triple['label'], '('+triple['parent_id']+')', ":", self.do_triple(triple))

		return bucket_rules



	def do_triple(self, triple):
		"""
		Recursive processing of triples according to PREDICATE_SET rules.
		A given triple's predicate has a range of possible (python) data types
		for its object: str, rdflib.term.BNode etc. as given above.
		First get this set, then select the appropriate object function call 
		by its data type.
		Then call that function with given triple.

		E.g. if the triple['object'] IS A STRING, this calls the 'str' key's 
		function supplied in PREDICATE_SET.

		"""
		#predicate_match = self.PREDICATE_SET[ triple['predicate'] ]
		#object_call = predicate_match[ type(triple['object']) ]
		#return object_call(triple)
		return self.PREDICATE_SET[ triple['predicate'] ] [ type(triple['object']) ] (triple)


	def get_component_object_text(self, triple): 
		t_object = triple['object']
		if triple['predicate'] == 'rdf:first':
			return t_object

		return [triple['predicate']] + [[t_object]]


	def get_component_set(self, triple): 
		"""
		For the QualifiedCardinality nodes, rdflib adds a shortcut "expression"
		key-value dictionary which contains "datatype" [disjunction|conjunction]
		and a "data" key that is a list of ontology entity ids. This appears to
		be triggered by a rdf:parseType="Collection" attribute.

		HOWEVER, if Collection item is complex, does this shortcut still work?
		"""
		print ("DATATYPE", triple['expression']['datatype'])
 
		if triple['expression']['datatype']:
			datatype = 'owl:unionOf'
		else: 
			datatype = 'owl:intersectionOf'

		return [datatype] + triple['expression']['data']


	def get_component_cardinality(self, triple): 
		"""
		The cardinality cases all require 2nd query to fetch content
		"""
		result = triple['object']['value']

		triples = self.onto_helper.do_query_table(
			self.queries['triple_by_relation'], {'subject': triple['subject'] } # ON RESTRICTION
		)
		for bnode_triple in triples:
			bnode_result = self.do_triple(bnode_triple)
			result.append(bnode_result)

		print("NOT DOING CARDINALITY CURRENTLY")
		return triple['predicate'] + [result] # ISSUE: parts may be rdflib.term.BNode
		

	def get_component_BNode(self, triple):

		result = []

		# Find subordinate tripples that begin with triple's object.
		# Basically none of these are annotations
		triples = self.onto_helper.do_query_table(
			self.queries['triple_by_subject'], {'subject': triple['object'] }
		)

		for bnode_triple in triples:
			# Note: bnode_triple may be an iterable.

			bnode_result = self.do_triple(bnode_triple);

			if bnode_result:
				if type(bnode_result) == str:   # singletonliteral value
					result.append(bnode_result)
					#print("bnode whole append", bnode_result)
				else:
					for item in bnode_result:
						if type(item) == str:   # literal values
							result.append(item)
							#print("bnode item append", item)
						else:

							while isinstance(item, list) and len(item)==1 and isinstance(item[0], list):
								item = item[0]
							#print("bnode list append", item)
							result.append(item)

		# Top-level tripple call has to have [predicate]:[...] result 
		# encapsulated in array; lower level items dealt with in bnode_result
		if triple['predicate'] in ['owl:someValuesFrom','owl:intersectionOf','owl:unionOf','owl:complementOf']:

			return [triple['predicate']] + [result]

		# Ignoring "rdf:first", "rdf:rest", predicates (i.e. flattening list) but passing their contents on.

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

		parser.add_option('-c', '--cache', dest='cache', default=False, action="store_true", help='Allow use of cached json rule file?')

		parser.add_option('-o', '--output', dest='output_folder', type='string', help='Path of output file to create')

		parser.add_option('-i', '--input', dest='comparison_ids', type='string', help='Comma separated list of term ids to match rules to.')

		parser.add_option('-r', '--root', dest='root_uri', type='string', help='Comma separated list of full URI root entity ids to fetch underlying terms from. Defaults to owl#Thing.', default='http://www.w3.org/2002/07/owl#Thing')

		return parser.parse_args()


if __name__ == '__main__':

	buckets = OntologyBuckets()
	buckets.__main__()  

