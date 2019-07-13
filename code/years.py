from rdflib.graph import ConjunctiveGraph as Graph
from rdflib.store import Store
from rdflib.plugin import get as plugin
from rdflib.term import URIRef
from rdflib import OWL, RDFS, Literal, Namespace 
from rdflib.namespace import FOAF, RDF, XSD
import csv, os, time, shutil
from progress import Progress


def is_specified(val):
  unspecified_indicators = ['\\N']
  return val not in unspecified_indicators


identifier = 'yearsGraph'
uri_base = 'http://www.ourmoviedb.org/'
output = 'outs/' + identifier + '.ttl.n3'

# Create graph
g = Graph(identifier=identifier)
n = Namespace(uri_base)

years = range(1870, 2050)
progress = Progress(len(years))   
start = time.time()

for year in range(1870, 2050):
  year_node = n['Year/' + str(year)]
  class_node = n['Year']
  g.add((year_node, RDF.type, class_node)) 
  g.add((year_node, n.YearValue, Literal(year, datatype=XSD.gYear)))
  
  # Let's name the decades
  # I want to isolate the 
  if year < 2010 and year >= 1950:
    decade_prefix = str(year)[2]
    decade_name = decade_prefix + '0s'
    # For example Year/70s
    class_node = n['Year/' + decade_name]
    g.add((year_node, RDF.type, class_node)) 

  progress.count()
  if progress.finished():
    break
    
g.serialize(destination=output, format='turtle')
end = time.time()
print("Total Time: ", end - start)
g.close()