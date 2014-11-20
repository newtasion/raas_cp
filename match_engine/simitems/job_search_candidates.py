# -*-coding: utf-8 -*-

"""
This job conducts search in Whoosh index.

Created on Sep 2013
@author: zul110
"""

from mrjob.job import MRJob
from mrjob.protocol import PickleProtocol, JSONProtocol

from match_engine.functions.index_processor import IndexProcessor
from match_engine.datamodel.domain_descriptor import DomainDescriptor
from match_engine.common.datafile_utils import getIndexFileFromDD


class JobSearchCandidates(MRJob):
    INPUT_PROTOCOL = PickleProtocol
    INTERNAL_PROTOCOL = PickleProtocol
    OUTPUT_PROTOCOL = PickleProtocol
    #OUTPUT_PROTOCOL = JSONProtocol

    """
    add the configuration options.
    """
    def configure_options(self):
        super(JobSearchCandidates, self).configure_options()
        self.add_file_option('--whooshindex')
        self.add_passthrough_option(
            '--domain', dest='domain', help='domain name')
        self.add_passthrough_option(
            '--data-root', dest='dataroot', help='data root')
        self.add_passthrough_option(
            '--domain-desc', dest='domain_desc', help='domain descriptor')

    def steps(self):
        return [
            self.mr(
                    mapper_init=self.search_mapper_configure,
                    mapper=self.search_mapper)]

    """
    convert tv_recs to queries.
    search against index.
    """
    def search_mapper_configure(self):
        """If domain descriptor is specified on the command line, take it.
        otherwise, fall back to the mockup"""
        if self.options.domain_desc:
            self.domainDescriptor = DomainDescriptor.from_json(
                                        self.options.domain_desc)
        else:
            self.domainDescriptor = DomainDescriptor.fromDbMockUp(
                            self.options.domain, self.options.dataroot)
        indexPath = getIndexFileFromDD(self.domainDescriptor)
        self.ib = IndexProcessor(self.domainDescriptor, indexPath)
        self.ib.setup(create=False)

        self.num_candidates = self.domainDescriptor.get_num_candidates()

    def search_mapper(self, key, line):
        tvRec = line
        self.increment_counter('mapper', 'tvrecs_input', 1)

        if tvRec is not None:
            self.increment_counter('mapper', 'tvrecs_valid', 1)
            query = self.ib.build_query_from_tv_rec(tvRec)
            if not query:
                self.increment_counter('mapper', 'query_invalid', 1)
                return

            matchList = self.ib.search_results(tvRec.getId(), query,
                                                res_limit=self.num_candidates)
            yield matchList.get_sourceid(), matchList


if __name__ == '__main__':
    JobSearchCandidates.run()
