# ontofetch.py Vocabulary Fetch

A command line fetch script (in https://github.com/GenEpiO/geem in scripts/ontofetch.py) will fetch and prepare the necessary field data from a given ontology URL or file. This is output in tabular and JSON if desired. Currently every term under “entity:Thing” in an OWL 2.0 ontology is fetched. Usage:

> ontofetch.py [file path or URL of ontology to fetch] -o [output file folder] -r [ontology root term URI]


# Design

The metadata component of a software system should take advantage of emerging standardized vocabularies - mostly provided by ontology communities working within the OWL 2.0 semantic web ontology technology.  Ontofetch has been designed to support vocabulary reuse in applications.

There are various requirements for such a vocabulary service:

A software installation needs to draw upon a simple term lookup table derived from online owl ontology terms and possibly custom terminology.  This lookup table needs to handle various functionality:

- Provide term Id, source ontology, label, definition, synonyms and term deprecation status and replacement term from online ontologies such as from OBOFoundry.org

- For a given term, if it plays a role as a measurable categorical variable, that its expected data type is provided, either categorical, string, or numeric (and possibly finer-grained, like decimal, integer, float etc.).
Receive refreshes from ontology source files on demand (via command line app initially), whether they are local or available at repositories like GitHub.
Question: do all software installs synchronize their vocabulary updates?
Question: if it is desired that a software install preserve particular term labels, how can we ensure this? A “lock” flag?

- Ensure existence of term identifiers through time.

- Track update history of a given term.

- Provide a diff (differential) report of terminology changes as updates occur.

- Provide a way of existing software installs to maintain display or output of term labels as desired by a given software installation.

- Provide alternative label, definition and user interface help.

- Lookup table does not provide further data specification details (such as required field); this is left for a separate specification component to define in the context of software template specifications.

- Accept terms minted by software itself (on a temporary basis) in case where reusable ontology terms can’t be found elsewhere.


The vocabulary lookup table would provide all the information necessary to render categorical selection lists for web form input in browsers. It could contain basic term hierarchies from very large ontologies. The lookup table could be packaged to be included directly for use in client (web browser) applications. 

It is a separate question of which ontology resources to trust and reuse.

It is a separate question about how to build a user interface solution whereby users can type (intellisense) a part of a term they are looking for, and have the interface return matching choices dynamically based on label or synonym - with possible server or ontology consultation for very large term lists like NCBITaxon.

To extend this system to be multilingual could involve a separate “multilingual” table that provided label, definition, ui_label, ui_definition and synonym content in a given language.

The following table details prototype vocabulary lookup table fields and functionality.

| field | type | Field label | Description |
| --- | --- | --- | --- |
| id | onto_id | Ontology identifier | Formatted as [ontology prefix]:[numeric id or other format identifier] .  ADD .X for updates?
| ontology | string | Ontology source | Prefix of ontology term came from. Some ontologies like GENEPIO have collections of terms from other ontologies. It helps to source a term from one and only one ontology, thus avoiding duplicates and the potential for variance that arises from that. However, duplicates are probably unavoidable, so determining which ontology takes priority, or how to merge, is a question.
| language | String (iso 2-letter country code) | Language | Language of text entries.  There will always be an english version. Text for other labels, definitions and synonyms can populate other records that have the same term id.
| parent_id | onto_id | Parent ontology identifier | Id of parent term with respect to ontology.
| other_parents | Comma-separated onto_id | Other parent ontology identifiers | A term can have more than one parent, which is provided as a list here. Above parent_id is the primary one for rendering display hierarchy if term returned in a search. How to choose primary parent?
| label | string | Label | English term label from original ontology
| definition | string | Definition | English definition as provided by original ontology
| ui_label | string | User interface label | English user interface label.  This is currently specific to the GenEpiO ontology which provides its own term overrides, to enable user friendly term labels to be superimposed on terms that are imported from 3rd party ontologies (and even for GenEpiO’s own terms).
| ui_definition | string | User interface definition | English User interface definition if different from original ontology definition. This could also be manually forced in this table? As well this can be a short form of a term’s longer definition, e.g. just the first 2 sentences?
| updated | datetime | Updated | Updated date
| synonym | string | Synonym | A divider-separated list of synonyms gathered from source ontology. These are important to search by when looking up a given term by “intellisense” typing.
| broad_synonym | string | Broad Synonym | A broad synonym references terms other than given term; equivalent to SKOS broader concept
| narrow_synonym | string | Narrow Synonym | A narrow synonym references terms that are more specific than given term. Equivalent to SKOS narrower concept.
| exact_synonym | string | Exact Synonym | An exact synonym for given term.
| ui_help | string | User interface help | User interface help info for this term?
| preferred | boolean | Preferred version | Selected when an software install prefers to force a particular term to use a given term record because install agency prefers that record until new content can be reviewed, or for all-time. Preferred record overrides all others and cannot be overwritten in an import. It is possible ui label and definition could be set independently of source ontology in such cases.
| deprecated | boolean | Deprecated | Flag (from owl:deprecated annotation) indicating whether term is no longer in use and should not be in user interfaces (from perspective of ontology curators).  Note, this only pertains to source ontology terms, not imports.
| replaced_by | onto_id | Replaced by | Ontology identifier pointing to another term (may need special provision to load given term into this table). Echoes “replaced_by”(IAO:0100001) annotation.
| version | uri | version | This is useful in the case where an ontology is pulling in terms from other ontologies, and needs version information for use in applications. Ideally all versions from a third party ontology are the same but parts of system may need to accomodate different components referencing different versions.

