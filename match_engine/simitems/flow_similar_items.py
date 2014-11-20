# -*-coding: utf-8 -*-

"""
HOW TO RUN THIS JOB:
    python flow_similar_items -d amazon
    
Created on Jul 12, 2013
@author: zul110

"""

import argparse
import os

from django.conf import settings

from match_engine.common.datafile_utils import getCandidateFileFromDD
from match_engine.common.datafile_utils import getIndexFileFromDD
from match_engine.common.datafile_utils import getMatchesFileFromDD
from match_engine.common.datafile_utils import getProfileFileFromDD
from match_engine.common.datafile_utils import getTvrecFileFromDD
from match_engine.simitems.job_generate_index import JobGenerateIndex
from match_engine.simitems.job_tvrecfy import JobTvRecfy
from match_engine.simitems.job_rerank_candidates import JobRerankCandidates
from match_engine.simitems.job_search_candidates import JobSearchCandidates
from raas.helpers import DomainDescriptorFactory

from raas.logic.controller import fetch_domain_meta
from raas.models import DomainConfig

LOG_NOTE = "***********"


class FlowSimilarItems():
    def __init__(self, domain_descriptor, runtype, engine, runlist):
        if runlist:
            self._runlist = runlist.split(',')
        else:
            self._runlist = ['r', 'i', 's', 'f']
        self._runtype = runtype
        self._data_root = domain_descriptor.get_data_path()
        self._engine = engine
        self._domainDescriptor = domain_descriptor
        # self._domainDescriptor = DomainDescriptor.fromDbMockUp(domain, dataroot)
        self._profile_file = getProfileFileFromDD(self._domainDescriptor)
        if not os.path.exists(self._profile_file):
            raise Exception("File {0} does not exist".format(self._profile_file))
        self._tvrec_file = getTvrecFileFromDD(self._domainDescriptor)
        self._matches_file = getMatchesFileFromDD(self._domainDescriptor)
        self._index_file = getIndexFileFromDD(self._domainDescriptor)
        self._candidates_file = getCandidateFileFromDD(self._domainDescriptor)

    def configure_options(self):
        super(JobGenerateIndex, self).configure_options()

    def print_counters_output(self, runner, needOutput=False):
        self.print_log("START PRINTING COUNTERS")
        counters = runner.counters()
        for counter in counters:
            print counter
            print '\n'
        if needOutput:
            self.print_log("START PRINTING OUTPUTS")
            for line in runner.stream_output():
                print line

    def print_log(self, note):
        print LOG_NOTE + " " + note + " " + LOG_NOTE

    def run(self):
        ddstring = self._domainDescriptor.to_json_string()
        if 'r' in self._runlist:
            mr_job_tvrecfy = JobTvRecfy(
                args=['-r', self._engine,
                      '--data-root', self._data_root,
                      '--domain-desc', ddstring,
                      '--output', self._tvrec_file, self._profile_file])
            with mr_job_tvrecfy.make_runner() as runner:
                runner.run()
                self.print_log('FINISH THE TVRECYFY JOB')
                self.print_counters_output(runner)

        """
        Now we only use one reducer for generating index. We can enhance it in the future.
        """
        if 'i' in self._runlist:
            mr_job_index = JobGenerateIndex(
                args=['-r', self._engine,
                      '--data-root', self._data_root,
                      '--domain-desc', ddstring,
                      '--jobconf', 'mapred.reduce.tasks=1', self._tvrec_file])
            with mr_job_index.make_runner() as runner:
                runner.run()
                self.print_log('FINISH THE GENERATEINDEX JOB')
                self.print_counters_output(runner)

        if self._runtype != 'full':
            next_output = self._matches_file
        else:
            next_output = self._candidates_file

        if 's' in self._runlist:
            mr_job_search = JobSearchCandidates(
                args=['-r', self._engine,
                      '--data-root', self._data_root,
                      '--domain-desc', ddstring,
                      '--output', next_output, self._tvrec_file])
            with mr_job_search.make_runner() as runner:
                runner.run()
                self.print_log('FINISH THE SEARCHCANDIDATES JOB')
                self.print_counters_output(runner)

            if self._runtype != 'full':
                return

        if 'f' in self._runlist:
            mr_job_rerank = JobRerankCandidates(
                args=['-r', self._engine,
                      '--data-root', self._data_root,
                      '--domain-desc', ddstring,
                      '--output', self._matches_file, self._tvrec_file, self._candidates_file])
            with mr_job_rerank.make_runner() as runner:
                runner.run()
                self.print_log('FINISH THE RERANK JOB')
                self.print_counters_output(runner)


def main():
    parser = argparse.ArgumentParser(description="run recommendation flow")

    parser.add_argument("-d", "--domain",
                        default=None,
                        help="domain to compute matches")
    parser.add_argument("-r", "--run",
                        dest="engine",
                        default='local',
                        help="where to run the flow")
    parser.add_argument("-t", "--type",
                        default="full",
                        help="defines how to run the flow")
    parser.add_argument("-l", "--runlist",
                        default="r,i,s,f",
                        help="the jobs enabled.")
    parser.add_argument("-p", "--data-root",
                        default=os.path.join(settings.PROJECT_ROOT, "../data/unittest/jobflow"),
                        help="absolute path of data")
    parser.add_argument("-u", "--user",
                        type=int,
                        default=1,
                        help="userid of the domain owner")

    options = parser.parse_args()

    if not options.domain:
        parser.error("please provide a domain.")

    domain_config = DomainConfig.objects.get(user_id=options.user, domain=options.domain)
    domain_config._data_path = options.data_root
    flow_similar_items = FlowSimilarItems(
        domain_config,
        options.type,
        options.engine,
        options.runlist
    )
    flow_similar_items.run()

if __name__ == '__main__':
    main()
