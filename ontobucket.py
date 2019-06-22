#!/usr/bin/python

""" **************************************************************************
	python ontobucket.py [owl ontology file path or URL]
	Ontology fetch of category matching bucket definitions for given agency ids.
 
 	Author: Damion Dooley

	Ontology() class __main__() reads in given ontology file via path or 
	URL and imports all ontology class terms, including labels, 
	definitions, and boolean axioms.  Output is produced as json or tabular tsv.
	
	The focus is on elaborating boolean axioms into their parts.  Each part has a parent:
	
		negation: id of term or anon node 
		conjunction:
		disjunction:

	REQUIREMENTS
	This script requires python module RDFLib.

	EXAMPLES
	Retrieve local file root-ontology.owl and print boolean matching expressions.

		> python ontoaxiom.py root-ontology.owl -r http://genepio.org/ontology/lexmapr/AGENCY_0000001 
	
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


class Ontology(object):
	"""


	"""

	CODE_VERSION = '0.0.2'

	def __init__(self):

		self.onto_helper = oh.OntoHelper()
		self.timestamp = datetime.datetime.now()
	 
		self.queries = {

			##################################################################
			# Membership Rules are boolean expressions or single entities linked
			# via 'has member' relation between a parent_id entity and children.
			# 
			# This script returns all triples that immediately compose the
			# owl.restriction. Below is simplest case
			#
			#   <owl:Restriction>
            #      <owl:onProperty rdf:resource="obi:AGENCY_0000078"/>
            #      <owl:someValuesFrom rdf:resource="obi:FOODON_00002196"/>
            #   </owl:Restriction>			
			#	...

			'report_mapping': rdflib.plugins.sparql.prepareQuery("""

				SELECT DISTINCT ?label ?parent_id ?restriction ?predicate ?object
				WHERE {
					BIND (AGENCY:AGENCY_0000078 as ?has_member).  # MIGRATE TO RO:has member
					
					?parent_id rdfs:subClassOf* ?root.
					?parent_id owl:equivalentClass ?restriction.
					?parent_id rdfs:label ?label.
					?restriction owl:onProperty ?has_member.
					?restriction (owl:someValuesFrom | owl:qualifiedCardinality | owl:minQualifiedCardinality | owl:maxQualifiedCardinality) ?object.
					?restriction ?predicate ?object.

				 } ORDER BY ?parent_id

			""", initNs = self.onto_helper.namespace),

			'triple_by_subject': rdflib.plugins.sparql.prepareQuery("""

				SELECT DISTINCT ?predicate ?object
				WHERE {?subject ?predicate ?object}
				ORDER BY ?subject

			""", initNs = self.onto_helper.namespace),

			# This query weeds out unneeded annotations.
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
		"predicate_set" dictionary is the set of predicates we can expect
		in a bucket matching expression ('has member' axiom). The dictionary value 
		contains data types that object should match to, and if so, a 
		response specific to that triple.

		NOTE: If ontobucket.py prints out an error of form "unrecognized 
		predicate [BIG LONG PREFIX]" and you need that predicate, then 
		ensure its prefix is added to ontohelper.py self.struct['@context'].

		"""
		self.PREDICATE_SET = {
			'owl:someValuesFrom': {
				str: self.render_object_text, # A string value / term id
				rdflib.term.BNode: self.render_BNode, 
			},
			# The cardinality cases all require function to fetch content
			'owl:qualifiedCardinality': 	{dict: self.render_cardinality},
			'owl:minQualifiedCardinality':	{dict: self.render_cardinality},
			'owl:maxQualifiedCardinality':	{dict: self.render_cardinality},
			 # used in conjunction with cardinality
			 # SHORTCUT, otherwise self.render_BNode 
			'owl:onClass': {rdflib.term.BNode: self.render_set},
			'owl:intersectionOf': {rdflib.term.BNode: self.render_BNode},
			'owl:unionOf': {rdflib.term.BNode: self.render_BNode},
			'owl:complementOf': {
				str: self.render_object_text, # A negated ontology id,
				rdflib.term.BNode: self.render_BNode, #self.render_set # SHORTCUT????
			},
			'rdf:first': {
				rdflib.term.BNode: self.render_BNode,
				str: self.render_object_text # The ontology id
			},
			'rdf:rest': {
				rdflib.term.BNode: self.render_BNode,
				str: self.render_blank # signals end of script
			},
			'rdf:type': {str: self.render_blank} # Uninformative class
		}


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
			self.log('report_mapping')
			self.do_membership_rules(term_id)


	def log(self, *args):
		"""
			Show log messages and differential time between calls
		"""
		timestamp = datetime.datetime.now()
		print("time delta: ", str(timestamp - self.timestamp), "\n", str(args))
		self.timestamp = timestamp


	def render_object_text(self, triple): 
		t_object = triple['object']
		if triple['predicate'] == 'rdf:first':
			return t_object

		return [triple['predicate'], t_object]


	def render_set(self, triple): 
		"""
		For the QualifiedCardinality nodes, rdflib adds a shortcut "expression"
		key-value dictionary which contains "datatype" [disjunction|conjunction]
		and a "data" key that is a list of ontology entity ids. I think this is
		triggered by a rdf:parseType="Collection" attribute.
		"""
		datatype = 'owl:unionOf' if triple['expression']['datatype'] else 'owl:intersectionOf'

		return [datatype] + triple['expression']['data']


	def render_cardinality(self, triple): 
		"""
		The cardinality cases all require 2nd query to fetch content
		"""
		result = [triple['predicate'], triple['object']['value']]

		# Cardinality references only one triple, so triples[0].
		triples = self.onto_helper.do_query_table(
			self.queries['triple_by_relation'], {'subject': triple['restriction'] } # ON RESTRICTION
		)
		for bnode_triple in triples:
			bnode_result = self.do_triple(bnode_triple)
			result.append(bnode_result)

		return result # ISSUE: parts may be rdflib.term.BNode
		

	def render_BNode(self, triple):

		result = []

		# Find subordinate tripples that begin with triple's object.
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


	def render_dump(self, triple):
		return ("DEBUG:", json.dumps(triple, sort_keys=False, indent=4, separators=(',', ': ')))

	def render_blank(self, triple):
		return None

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

		for triple in table:
			# TEST EXAMPLES
			#if not triple['parent_id'] in (['AGENCY:0000002', 'AGENCY:0000007', 'AGENCY:0000041']):
			#	continue

			print (triple['label'], '('+triple['parent_id']+')', ":", self.do_triple(triple))

		return


	def do_triple(self, triple):
		"""
		Recursiver processing of triples according to PREDICATE_SET rules.
		"""
		t_predicate = triple['predicate']

		if not t_predicate in self.PREDICATE_SET:
			print ("unrecognized predicate", t_predicate, " in:", json.dumps(triple, sort_keys=False, indent=4, separators=(',', ': ')))
			return

		t_object = triple['object']
		t_object_type = type(t_object)
		object_type_set = self.PREDICATE_SET[t_predicate]
		if not t_object_type in object_type_set:
			print ("unrecognized object for ", t_predicate,":",t_object_type, json.dumps(t_object, sort_keys=False, indent=4, separators=(',', ': ')))
			return

		t_triple_fn = object_type_set[t_object_type]
		return t_triple_fn(triple)


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
		
		parser.add_option('-r', '--root', dest='root_uri', type='string', help='Comma separated list of full URI root entity ids to fetch underlying terms from. Defaults to owl#Thing.', default='http://www.w3.org/2002/07/owl#Thing')

		return parser.parse_args()


if __name__ == '__main__':

	genepio = Ontology()
	genepio.__main__()  

