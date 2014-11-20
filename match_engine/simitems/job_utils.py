"""
utilities for processing text. 

Created on Jul 12, 2013
@author: zul110
"""

import os
from mrjob.job import MRJob

from whoosh.index import create_in
from whoosh.fields import *

schema = Schema(title=TEXT(stored=True), path=ID(stored=True), content=TEXT)
ix = create_in("data/index", schema)
writer = ix.writer()
writer.add_document(title = u"first documen", path=u"/a",
                     content=u"This is the first document we've added!")
writer.add_document(title=u"Second document", path=u"/b",
                    content=u"first!")
writer.commit()


from whoosh.qparser import QueryParser
from whoosh.query import *

tq1 = Term("content", u"first", 1.0)
tq2 = Term("title", u"second", 1.0)

tq3 = Or([tq1, tq2], 1.0)
print tq3.requires()
with ix.searcher() as searcher:
    results = searcher.search(tq3)
    for a in results:
        print a




