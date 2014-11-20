#-*-coding: utf-8 -*-

"""
This job converts profiles to tvrecs.

To set the number of maps/reduces to run,
you can use --jobconf to access the appropriate hadoop options. For example:
mr_your_job.py --jobconf mapred.map.tasks=23 --jobconf mapred.reduce.tasks=42

Created on Sep 2013
@author: zul110

"""

from mrjob.job import MRJob
from mrjob.protocol import PickleProtocol
from match_engine.datamodel.profile import Profile
from match_engine.datamodel.domain_descriptor import DomainDescriptor
from match_engine.functions.tvrec_processor import TvRecProcessor


class JobTvRecfy(MRJob):
    INTERNAL_PROTOCOL = PickleProtocol
    OUTPUT_PROTOCOL = PickleProtocol

    def configure_options(self):
        super(JobTvRecfy, self).configure_options()
        self.add_passthrough_option(
            '--inputformat', dest='inputformat', default='json',
            help='the format of the input')
        self.add_passthrough_option(
            '--domain', dest='domain', help='domain name')
        self.add_passthrough_option(
            '--data-root', dest='dataroot', help='data root')
        self.add_passthrough_option(
            '--domain-desc', dest='domain_desc', help='domain descriptor')

    def steps(self):
        return [
            self.mr(
                    mapper_init=self.tvrecfy_mapper_configure,
                    mapper=self.tvrecfy_mapper,
                    reducer=self.tvrecfy_reducer,
                    reducer_final=self.tvrecfy_reducer_final)]

    def tvrecfy_mapper_configure(self):
        """If domain descriptor is specified on the command line, take it.
        otherwise, fall back to the mockup"""
        if self.options.domain_desc:
            self.domainDescriptor = DomainDescriptor.from_json(
                                        self.options.domain_desc)
        else:
            self.domainDescriptor = DomainDescriptor.fromDbMockUp(
                            self.options.domain, self.options.dataroot)

        self.ana_dict = TvRecProcessor.tvrec_analyzer_init()

    def tvrecfy_mapper(self, key, line):
        self.increment_counter('mapper', 'input_lines', 1)
        profileObj = Profile.fromLine(line, self.options.inputformat)
        if (profileObj == None):
            raise Exception("empty profile")
            return
        self.increment_counter('mapper', 'profiles', 1)

        tv_rec = profileObj.toTvRec(self.domainDescriptor)
        if tv_rec != None:
            self.increment_counter('mapper', 'output_tvrecs', 1)
            TvRecProcessor.tvrec_analyzer(tv_rec, self.ana_dict, self.domainDescriptor)
            yield tv_rec.getId(), tv_rec

    def tvrecfy_mapper_final(self):
        yield TvRecProcessor.ana_dict_key(), self.ana_dict


    def tvrecfy_reducer(self, key, values):
        """
        dedup.
        """
        vKeep = None
        for value in values:
            vKeep = value
            break

        if vKeep != None:
            self.increment_counter('reducer', 'output_tvrecs', 1)
            yield key, vKeep

    def tvrecfy_reducer_final(self):
        pass

if __name__ == '__main__':
    JobTvRecfy.run()
