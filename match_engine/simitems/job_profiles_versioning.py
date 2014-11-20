#-*-coding: utf-8 -*-

"""
version the output data.

Created on Sep 2013
@author: zul110

"""
import argparse
import os

from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol
from match_engine.datamodel.profile import Profile
from match_engine.common.date_utils import get_latest_versioning_timestamp
from match_engine.common.datafile_utils import get_base_dir

PRIOR_COUNT = 10
PRIOR_CORRELATION = 0
LIMIT = 10
APPENDED = "-1"


class jobProfilesVersioning(MRJob):
    OUTPUT_PROTOCOL = RawValueProtocol

    def configure_options(self):
        super(jobProfilesVersioning, self).configure_options()
        self.add_passthrough_option(
            '--inputformat', dest='inputformat', default='json',
            help='the format of the input')
        self.add_passthrough_option(
            '--domain', dest='domain', help='domain name')
        self.add_passthrough_option(
            '--data-root', dest='dataroot', help='data root')
        self.add_passthrough_option(
            '--new-data', dest='newdata', help='new data')

    def steps(self):
        return [
            self.mr(
                    mapper_init=self.versioning_mapper_configure,
                    mapper=self.versioning_mapper,
                    reducer=self.versioning_reducer)]

    def versioning_mapper_configure(self):
        inputfile = str(os.environ['map_input_file'])
        newdata_dir = self.options.newdata

        if newdata_dir is not None and inputfile.find(newdata_dir) >= 0:
            self.filetimestamp = APPENDED
        else:
            self.filetimestamp = get_base_dir(inputfile)

    def versioning_mapper(self, key, line):
        self.increment_counter('mapper', 'input_lines', 1)
        profileObj = Profile.fromLine(line, self.options.inputformat)
        if (profileObj == None):
            raise Exception("empty profile")
            return

        tid = profileObj.getIid()
        yield tid, (self.filetimestamp, line)

    def versioning_reducer(self, key, values):
        tdict = {}
        beusedline = None
        for value in values:
            filetimestamp, line = value
            if filetimestamp == APPENDED:
                beusedline = line
                break
            else:
                tdict[filetimestamp] = line

        if beusedline is not None:
            yield None, beusedline

        if len(tdict) <= 0:
            return
        beused = get_latest_versioning_timestamp(tdict.keys())
        yield None, tdict[beused]


def main():
    parser = argparse.ArgumentParser(description="run recommendation flow")

    parser.add_argument("-r", "--run",
                      action="store",
                      dest="engine",
                      default='local',
                      help="where to run the flow")
    parser.add_argument("-p", "--data-root",
                      dest="data_root",
                      default=None,
                      help="absolute path of data")
    parser.add_argument("-dt", "--data-type",
                      dest="data_type",
                      default="append",
                      help="data type")

    options = parser.parse_args()
    datatype = options.data_type
    dataroot = options.data_root


    if not dataroot:
        parser.error("please provide the data root.")

    if datatype == 'append':
        """
        TODO: find the latest version under data_root.
        Now we are using all files under data_root.
        """
#        mr_job = MRJob(args=['-r', options.engine, '--output', options.data_root])
#        runner = mr_job.make_runner()
#        for i in  runner.fs.ls('.'):
#            print i
        mr_job_versioning = jobProfilesVersioning(
                    args=['-r', options.engine,
                          '--data-root', options.data_root,
                          '--new-data', options.data_root,
                          '--output', options.data_root])
        with mr_job_versioning.make_runner() as runner:
            for i in  runner.fs.ls(dataroot):
                print i
                print dir(runner.fs.ls)
                runner.run()


if __name__ == '__main__':
    jobProfilesVersioning.run()
