#-*-coding: utf-8 -*-

"""
This job conducts search in Whoosh index. 

Created on Sep 2013
@author: zul110
"""

from mrjob.job import MRJob
from mrjob.protocol import PickleProtocol

from match_engine.functions.index_processor import IndexProcessor
from match_engine.datamodel.tv_rec import TvRec
from match_engine.datamodel.domain_descriptor import DomainDescriptor
from match_engine.common.datafile_utils import getIndexFileFromDD


class JobGenerateIndex(MRJob):
    INTERNAL_PROTOCOL = PickleProtocol
    INPUT_PROTOCOL = PickleProtocol
    """
    add the configuration options.
    """
    def configure_options(self):
        super(JobGenerateIndex, self).configure_options()
        self.add_file_option('--whooshindex')
        self.add_passthrough_option(
            '--domain', dest='domain', help='domain name')
        self.add_passthrough_option(
            '--data-root', dest='dataroot', help='data root')
        self.add_passthrough_option(
            '--domain-desc', dest='domain_desc', help='domain descriptor')

    def steps(self):
#        mr_job = MRJob(args=['-r', options.engine, '--output', options.data_root])
#        runner = mr_job.make_runner()
#        for i in  runner.fs.ls('.'):
        return [
            self.mr(
                    mapper_init=self.index_mapper_configure,
                    mapper=self.index_mapper,
                    reducer_init=self.index_reducer_configure,
                    reducer=self.index_reducer,
                    reducer_final=self.index_reducer_final)]

    def index_mapper_configure(self):
        if self.options.domain_desc:
            self.domainDescriptor = DomainDescriptor.from_json(
                                        self.options.domain_desc)
        else:
            self.domainDescriptor = DomainDescriptor.fromDbMockUp(
                            self.options.domain, self.options.dataroot)

    def index_mapper(self, key, line):
        self.increment_counter('mapper', 'input_lines', 1)
        yield 1, line

    def index_reducer_configure(self):
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
        self.ib.setup()
        self.ib.start_update()

    def index_reducer(self, user_id, values):
        for tvRec in values:
            doc = self.ib.build_doc_from_tv_rec(tvRec)
            if doc != None:
                self.increment_counter('reducer', 'added_docs', 1)
                self.ib.go_update(doc)

    def index_reducer_final(self):
        self.ib.finish_update()


if __name__ == '__main__':
    JobGenerateIndex.run()
