# -*- coding: utf-8 -*-
"""
This job flow prepare dataset for model training.

Created on Sep 2013
@author: zul110
"""
import os

from mrjob.job import MRJob
from mrjob.protocol import PickleProtocol, RawValueProtocol

from match_engine.datamodel.domain_descriptor import DomainDescriptor
from match_engine.common.datafile_utils import isTvrecFile
from match_engine.functions.item_similarity import ItemSimilarityFunc
from match_engine.datamodel.model import Model
from match_engine.common.datafile_utils import getTrainningDirFromDD


class JobGetTrainingData(MRJob):
    INPUT_PROTOCOL = PickleProtocol
    INTERNAL_PROTOCOL = PickleProtocol
    OUTPUT_PROTOCOL = RawValueProtocol

    def configure_options(self):
        super(JobGetTrainingData, self).configure_options()
        self.add_passthrough_option(
            '--delimiter', dest='delimiter', default='\t',
            help='the delimiter for the tsv file.')
        self.add_passthrough_option(
            '--domain', dest='domain', help='domain name')
        self.add_passthrough_option(
            '--data-root', dest='dataroot', help='data root')
        self.add_passthrough_option(
            '--domain-desc', dest='domain_desc', help='domain descriptor')

    def steps(self):
        join_id_tvrec = self.mr( \
                        mapper_init=self.join_id_tvrec_mapper_configure,
                        mapper=self.join_id_tvrec_mapper,
                        reducer=self.join_id_tvrec_reducer
                        )
        join_pair_tvrec = \
            self.mr(reducer_init=self.join_pair_tvrec_reducer_configure,
                                  reducer=self.join_pair_tvrec_reducer)

        output_training = \
            self.mr(reducer_init=self.output_training_reducer_configure,
                                  reducer=self.output_training_tvrec_reducer,
                                  reducer_final=self.joutput_training_tvrec_reducer_final)

        return [join_id_tvrec, join_pair_tvrec, output_training]

    def join_id_tvrec_mapper_configure(self):
        # break matchList to pairs
        self.myfileName = str(os.environ['map_input_file'])
        print 'input file' + self.myfileName
        self.recType = None
        if isTvrecFile(self.myfileName) == True:
            self.recType = 'TVREC'
        else:
            self.recType = 'TRAIN'
        self.delimiter = self.options.delimiter

    def join_id_tvrec_mapper(self, key, line):
        if self.recType == 'TRAIN':
            self.increment_counter('join_id_tvrec_mapper', 'input_trains', 1)
            tp = line
            sourceId = tp._source
            targetId = tp._target
            label = tp._label
            yield unicode(sourceId), (1, (sourceId, targetId, label))
            yield unicode(targetId), (1, (sourceId, targetId, label))

        elif self.recType == 'TVREC':
            tvrec = line
            self.increment_counter('join_id_tvrec_mapper', 'input_tvrecs', 1)
            yield unicode(tvrec.getId()), (2, tvrec)

    def join_id_tvrec_reducer(self, key, values):
        pairSrcTgtList = []
        tvrec = None
        for value in values:
            t, obj = value
            if t == 1:
                pairSrcTgtList.append(obj)
            elif t == 2:
                tvrec = obj

        if len(pairSrcTgtList) == 0:
            self.increment_counter('join_id_tvrec_reducer', \
                                   'pairSrcTgtList_empty', 1)
            return

        if tvrec == None:
            self.increment_counter('join_id_tvrec_reducer', 'tvrecs_none', 1)
            return

        for pairSrcTgt in pairSrcTgtList:
            self.increment_counter('join_id_tvrec_reducer', 'outputs', 1)
            yield pairSrcTgt, tvrec

    def join_pair_tvrec_reducer_configure(self):
        if self.options.domain_desc:
            self.domainDescriptor = DomainDescriptor.from_json(
                                        self.options.domain_desc)
        else:
            self.domainDescriptor = DomainDescriptor.fromDbMockUp(
                            self.options.domain, self.options.dataroot)
        self._model = Model.from_dict(
                            self.domainDescriptor.get_rank_model_dict())
        self._itemSimilarityFunc = ItemSimilarityFunc(self._model)

    def join_pair_tvrec_reducer(self, key, values):
        sourceId, targetId, label = key
        sourceTvrec = None
        targetTvrec = None

        for value in values:
            myId = value.getId()
            if sourceId == myId:
                sourceTvrec = value
            elif targetId == myId:
                targetTvrec = value

        if sourceTvrec == None and targetTvrec == None:
            self.increment_counter('join_pair_tvrec_reducer',
                                   'src_tgt_2_none', 1)
            return

        if sourceTvrec == None or targetTvrec == None:
            self.increment_counter('join_pair_tvrec_reducer',
                                   'src_tgt_1_none', 1)
            return

        features = self._itemSimilarityFunc.get_features(sourceTvrec,
                                                        targetTvrec)

        self.increment_counter('join_pair_tvrec_reducer', 'outputs', 1)
        yield 1, (label, features)

    def output_training_reducer_configure(self):
        if self.options.domain_desc:
            self.domainDescriptor = DomainDescriptor.from_json(
                                        self.options.domain_desc)
        else:
            self.domainDescriptor = DomainDescriptor.fromDbMockUp(
                            self.options.domain, self.options.dataroot)

        training_dir = getTrainningDirFromDD(self.domainDescriptor)
        if not os.path.exists(training_dir):
            os.makedirs(training_dir)
        if not os.access(training_dir, os.W_OK):
            raise IOError("The path to your training dir '%s' is not writable for the current user/group." % training_dir)

        self.meta_dict = {}
        self.meta_dict['domain'] = self.options.domain
        self.meta_dict['format'] = 'json'
        self.meta_dict['count'] = 0
        self.count = 0

        self._model = Model.from_dict(
                            self.domainDescriptor.get_rank_model_dict())
        self._itemSimilarityFunc = ItemSimilarityFunc(self._model)
        self._meta_file = 

    def output_training_tvrec_reducer(self, key, values):
        for value in values:
            self.count += 1
            yield None, value

    def joutput_training_tvrec_reducer_final(self):
        pass


if __name__ == '__main__':
    JobGetTrainingData.run()
