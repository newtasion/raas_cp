#-*-coding: utf-8 -*-

"""
This job load raw training data (now only support tsv),
then convert it to pickle files.

Created on Sep 2013
@author: zul110
"""

from mrjob.job import MRJob
from mrjob.protocol import PickleProtocol, RawValueProtocol

from match_engine.datamodel.domain_descriptor import DomainDescriptor
from match_engine.datamodel.training_point import TrainingPoint


class JobLoadRawTrainings(MRJob):
    INTERNAL_PROTOCOL = RawValueProtocol
    OUTPUT_PROTOCOL = PickleProtocol

    def configure_options(self):
        super(JobLoadRawTrainings, self).configure_options()
        self.add_passthrough_option(
            '--delimiter', dest='delimiter',  \
            help='the delimter for the raw training data')

    def steps(self):
        return [
            self.mr(
                    mapper_init=self.load_mapper_configure,
                    mapper=self.load_mapper)]

    def load_mapper_configure(self):
        self.delimiter = None
        if self.options.delimiter:
            self.delimiter = self.options.delimiter

    def load_mapper(self, key, line):
        self.increment_counter('mapper', 'input_lines', 1)
        tp = TrainingPoint.fromTsv(line, self.delimiter)
        if (tp == None):
            return
        self.increment_counter('mapper', 'trainingPoints', 1)
        yield tp._label, tp


if __name__ == '__main__':
    JobLoadRawTrainings.run()
