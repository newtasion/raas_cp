# -*-coding: utf-8 -*-

"""
 this class provide utilities to generate index files and conduct search.
 
Created on Jul 05, 2013
@author: zul110
"""
import os

from whoosh import index
from whoosh.query import Term, Or
from whoosh.analysis import StemmingAnalyzer
from whoosh.fields import ID, Schema, TEXT, KEYWORD, NUMERIC, BOOLEAN, DATETIME
from whoosh.filedb.filestore import FileStorage, RamStorage
from whoosh.writing import AsyncWriter

from jieba.analyse import ChineseAnalyzer

from match_engine.common.encoding_utils import force_text
from match_engine.datamodel.domain_descriptor import DomainDescriptor
from match_engine.datamodel.matches import Match, MatchList

LIMIT_RESULTS = 20

ANALYZER_MAP = {'en': StemmingAnalyzer, 'cn': ChineseAnalyzer}


class IndexProcessor():
    def __init__(self, domain_descriptor, path):
        self.use_file_storage = True
        self.path = path
        self.domain_descriptor = domain_descriptor
        self.indexFields = self.domain_descriptor.get_index_fields()
        self.searchmap = self.domain_descriptor.get_search_map()
        if self.domain_descriptor.get_language() in ANALYZER_MAP:
            self.analyzer = ANALYZER_MAP[self.domain_descriptor.get_language()]()
        else:
            # by default, we use chinese analyzer.
            self.analyzer = ChineseAnalyzer()

    def build_schema(self):
        """
        build the schema for the domain.
        """
        schema_fields = {}
        for fieldName in self.indexFields:
            fieldType = self.domain_descriptor.get_type(fieldName)
            if fieldType.upper() in [u'ID']:
                schema_fields[fieldName] = ID(stored=True, unique=True)
            elif fieldType.upper() in [u'SINGLE']:
                schema_fields[fieldName] = ID(stored=False)
            elif fieldType.upper() in [u'TEXT']:
                schema_fields[fieldName] = TEXT(stored=False, analyzer=self.analyzer, field_boost=1.0, sortable=True)
            elif fieldType.upper() in [u'INTEGER']:
                schema_fields[fieldName] = NUMERIC(stored=False, numtype=int, field_boost=1.0)
            elif fieldType.upper() in ['FLOAT', 'NUMERIC']:
                schema_fields[fieldName] = NUMERIC(stored=False, numtype=float, field_boost=1.0)
            elif fieldType.upper() in 'BOOLEAN':
                schema_fields[fieldName] = BOOLEAN(stored=False)
            elif fieldType.upper() in ['DATE', 'DATETIME']:
                schema_fields[fieldName] = DATETIME(stored=False, sortable=True)
            elif fieldType.upper() in ['TAGS', 'KEYWORDS']:
                schema_fields[fieldName] = KEYWORD(stored=False, lowercase=True, commas=True, scorable=True, field_boost=1.0)
        return Schema(**schema_fields)

    def setup(self, create=True):
        new_index = False
        self.setup_complete = False

        # Make sure the index is there.
        if self.use_file_storage and not os.path.exists(self.path):
            os.makedirs(self.path)
            create = True

        if self.use_file_storage and not os.access(self.path, os.W_OK):
            raise IOError("The path to your Whoosh index '%s' is not writable for the current user/group." % self.path)

        if self.use_file_storage:
            self.storage = FileStorage(self.path)
        else:
            global LOCALS

            if LOCALS.RAM_STORE is None:
                LOCALS.RAM_STORE = RamStorage()
            self.storage = LOCALS.RAM_STORE

        self.schema = self.build_schema()

        if create:
            self.index = self.storage.create_index(self.schema)
        else:
            try:
                self.index = self.storage.open_index(schema=self.schema)
            except index.EmptyIndexError:
                self.index = self.storage.create_index(self.schema)

    def start_update(self):
        self.index = self.index.refresh()
        self.writer = AsyncWriter(self.index)

    def go_update(self, doc):
        self.writer.update_document(**doc)

    def finish_update(self):
        self.writer.commit()

    def search_results(self, sourceId, query, res_limit=100,
                       narrow_queries=None):
        # A zero length query should return no results.
        if query is None:
            return {
                'results': [],
                'hits': 0,
            }
        """
        STEP 1: NARROW DOWN THE RESULTS.
        """
        narrowed_results = None
        self.index = self.index.refresh()

        if narrow_queries is None:
            narrow_queries = set()

        narrow_searcher = None
        if narrow_queries is not None:
            # Potentially expensive? I don't see another way to do it in Whoosh.
            narrow_searcher = self.index.searcher()

            for nq in narrow_queries:
                recent_narrowed_results = narrow_searcher.search(self.parser.parse(force_text(nq)))

                if len(recent_narrowed_results) <= 0:
                    return {
                        'results': [],
                        'hits': 0,
                    }
                if narrowed_results:
                    narrowed_results.filter(recent_narrowed_results)
                else:
                    narrowed_results = recent_narrowed_results
        self.index = self.index.refresh()

        """
        STEP 2: SUBMIT THE QUERY.
        """
        searcher = self.index.searcher()
        results = searcher.search(query, limit=res_limit)
        matchList = []
        dedupSet = set()

        for i, r in enumerate(results):
            tid = r["id"]
            if sourceId == tid:
                continue
            if tid in dedupSet:
                continue
            else:
                dedupSet.add(tid)
                tscore = results.score(i)
                match = Match(tid, tscore)
                matchList.append(match)

        matchList = MatchList(sourceId, matchList)
        searcher.close()
        if hasattr(narrow_searcher, 'close'):
            narrow_searcher.close()

        return matchList

    def build_query_from_tv_field(self, tv_field, target_field, boost_value):
        """
        build a boolean query for a tv_field obj.
        """
        # A zero length query should return no results.
        if target_field is None:
            return {
                'results': [],
                'hits': 0,
            }

        term_query_list = []
        for ele, tfidf in tv_field.get_term_vector().items():
            ele = force_text(ele)
            myboost = boost_value * tfidf  # for fixing whoosh bug.
            # myboost = boost_value
            term_query = Term(target_field, ele, myboost)
            term_query_list.append(term_query)
        if len(term_query_list) == 0:
            return None
        b_query = Or(term_query_list)
        return b_query

    def build_query_from_tv_rec(self, tv_rec):
        """
        build a boolean query for a tv_rec obj.
        """
        if tv_rec is None:
            return None
        b_queryList = []
        for tv_field in tv_rec.getFields():
            sourceFieldName = tv_field.get_name()
            if sourceFieldName not in self.searchmap:
                continue
            targets = self.searchmap[sourceFieldName]
            for targetName, targetBoost in targets.items():
                b_query = self.build_query_from_tv_field(tv_field, targetName, targetBoost)
                if b_query is None:
                    continue
                b_queryList.append(b_query)

        if len(b_queryList) == 0:
            return None

        tv_query = Or(b_queryList)
        return tv_query

    def build_doc_from_tv_rec(self, tv_rec):
        """
        build a document for a tv_rec obj.
        """
        if tv_rec is None:
            return None

        itemId = tv_rec.getId()
        doc = {'id': itemId}
        for tv_field in tv_rec.getFields():
            field = tv_field.get_name()
            if  field in self.indexFields:
                v = tv_field.get_terms_as_str()
                if v:
                    doc[field] = v
        return doc


def unitTest():
    from match_engine.datamodel.field_type import TextField, KeywordsField, IdField
    from match_engine.datamodel.tv_rec import TvRec
    cnAnalyzer = ChineseAnalyzer()
    textField = TextField(analyzer=cnAnalyzer)
    keywordField = KeywordsField()
    idField = IdField()

    tf11 = textField.to_tvrec_field('title', 'Harvard1 pig1 any world text any')
    tf12 = textField.to_tvrec_field('highlights', 'nothing happend')
    tf13 = keywordField.to_tvrec_field("genre", "match")
    tvRec1 = TvRec('45', [tf11, tf12, tf13])

    tf21 = textField.to_tvrec_field('title', 'Harvard2 world looked at setting up and installing hadoop')
    tf22 = textField.to_tvrec_field('highlights', 'this is for summer harvard')
    tf23 = keywordField.to_tvrec_field("genre", "math, math art")
    tvRec2 = TvRec('46', [tf21, tf22, tf23])

    tf31 = textField.to_tvrec_field('title', 'pig is a Harvard3 installing to start processing steps')
    tf32 = textField.to_tvrec_field('highlights', 'we also learn how to read and write using different data formats')
    tf33 = keywordField.to_tvrec_field('genre', 'art')
    tvRec3 = TvRec('47', [tf31, tf32, tf33])

    tf41 = textField.to_tvrec_field('title', 'The second one 你 中文测试中文 is even more interesting! 吃水果')
    tf42 = textField.to_tvrec_field('highlights', 'we also learn how to read and write using different data formats')
    tf43 = keywordField.to_tvrec_field('genre', 'art')
    tvRec4 = TvRec('48', [tf41, tf42, tf43])

    tf51 = textField.to_tvrec_field('title', '买水果然后来世博园中文 interesting White')
    tf52 = textField.to_tvrec_field('highlights', 'we also learn how to read and write using different data formats')
    tf53 = keywordField.to_tvrec_field('genre', 'art')
    tvRec5 = TvRec('49', [tf51, tf52, tf53])

    domain = '1'
    path = '/Users/znli/workraas/raas/data/unittest/jobflow'
    dd = DomainDescriptor.fromDbMockUp(domain, path)
    ib = IndexProcessor(dd, path + "/index")
    ib.setup()
    ib.start_update()

    print '>>>> writing index....'
    doc1 = ib.build_doc_from_tv_rec(tvRec1)
    print '>>>> doc1: ' + str(doc1)
    ib.go_update(doc1)
    doc2 = ib.build_doc_from_tv_rec(tvRec2)
    print '>>>> doc2: ' + str(doc2)
    ib.go_update(doc2)
    doc3 = ib.build_doc_from_tv_rec(tvRec3)
    print '>>>> doc3: ' + str(doc3)
    ib.go_update(doc3)
    doc4 = ib.build_doc_from_tv_rec(tvRec4)
    print '>>>> doc4: ' + str(doc4)
    ib.go_update(doc4)
    doc5 = ib.build_doc_from_tv_rec(tvRec5)
    print '>>>> doc5: ' + str(doc5)
    ib.go_update(doc5)

    ib.finish_update()
    print '>>>> finish indexing'

    print '>>>> searching, the query is : '
    query = ib.build_query_from_tv_rec(tvRec5)
    print query
    print 'the results are:'
    matches = ib.search_results(tvRec5.getId(), query, 20)
    print matches.toJson()
    # ib.build_query_from_field(tf, 'f3', 2.0)

if __name__ == "__main__":
    unitTest()


