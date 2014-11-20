#-*-coding: utf-8 -*-

"""
This job rerank the candidates we have. 

HOW TO RUN THIS JOB:
    python match_engine/simitems/job_search_candidates.py --output tmp/matches tmp/tvrecs

Created on Sep 2013
@author: zul110

"""
import os

from mrjob.job import MRJob
from mrjob.protocol import PickleProtocol, JSONProtocol

from match_engine.datamodel.domain_descriptor import DomainDescriptor
from match_engine.datamodel.matches import Match, MatchList
from match_engine.common.datafile_utils import isTvrecFile

from match_engine.functions.item_similarity import ItemSimilarityFunc
from match_engine.datamodel.model import Model

DEFAULT_LIMIT = 10


class JobRerankCandidates(MRJob):
    INPUT_PROTOCOL = PickleProtocol
    INTERNAL_PROTOCOL = PickleProtocol
    #OUTPUT_PROTOCOL = PickleProtocol
    OUTPUT_PROTOCOL = JSONProtocol

    def configure_options(self):
        super(JobRerankCandidates, self).configure_options()
        self.add_passthrough_option(
            '--domain', dest='domain', help='domain name')
        self.add_passthrough_option(
            '--data-root', dest='dataroot', help='data root')
        self.add_passthrough_option(
            '--domain-desc', dest='domain_desc', help='domain descriptor')

    def steps(self):
        join_id_tvrec = self.mr(
                        mapper_init=self.join_id_tvrec_mapper_configure,
                        mapper=self.join_id_tvrec_mapper,
                        reducer=self.join_id_tvrec_reducer
                        )

        join_pair_tvrec = self.mr(
                        reducer_init=self.join_pair_tvrec_reducer_configure,
                        reducer=self.join_pair_tvrec_reducer)

        group_matches = self.mr(
                        reducer_init=self.group_matches_reducer_configure,
                        reducer=self.group_matches_reducer)

        return [join_id_tvrec, join_pair_tvrec, group_matches]

    def join_id_tvrec_mapper_configure(self):
        """
        break matchList to pairs
        """
        self.myfileName = str(os.environ['map_input_file'])
        print 'input file' + self.myfileName
        self.recType = None
        if isTvrecFile(self.myfileName) == True:
            self.recType = 'TVREC'
        else:
            self.recType = 'MATCHES'

    def join_id_tvrec_mapper(self, key, line):
        if self.recType == 'MATCHES':
            self.increment_counter('join_id_tvrec_mapper', 'input_matches', 1)
            matchList = line
            source_id = matchList.get_sourceid()
            targets = matchList.get_targets()
            cnt = 0
            for target in targets:
                target_id = target.get_targetid()
                self.increment_counter('join_id_tvrec_mapper', 'output_pairs', 2)
                cnt += 1
                yield source_id, (1, (source_id, target_id))
                yield target_id, (1, (source_id, target_id)) 
            self.increment_counter('join_id_tvrec_mapper', 'num_targets_' + str(cnt), 1) 
        elif self.recType == 'TVREC':
            tvrec = line
            self.increment_counter('join_id_tvrec_mapper', 'input_tvrecs', 1)
            yield tvrec.getId(), (2, tvrec)

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
            self.increment_counter('join_id_tvrec_reducer', 'pairSrcTgtList_empty', 1)
            return

        if tvrec == None:
            self.increment_counter('join_id_tvrec_reducer', 'tvrecs_none', 1)
            return

        for pairSrcTgt in pairSrcTgtList:
            self.increment_counter('join_id_tvrec_reducer', 'outputs', 1)
            yield pairSrcTgt, tvrec

    def join_pair_tvrec_reducer_configure(self):
        """If domain descriptor is specified on the command line, take it.
        otherwise, fall back to the mockup"""
        if self.options.domain_desc:
            self.domainDescriptor = DomainDescriptor.from_json(
                                        self.options.domain_desc)
        else:
            self.domainDescriptor = DomainDescriptor.fromDbMockUp(
                            self.options.domain, self.options.dataroot)
        self._model = Model.from_dict(self.domainDescriptor.get_rank_model_dict())
        self._itemSimilarityFunc = ItemSimilarityFunc(self._model)

    def join_pair_tvrec_reducer(self, key, values):
        source_id, target_id = key
        sourceTvrec = None
        targetTvrec = None

        for value in values:
            myId = value.getId()
            if source_id == myId:
                sourceTvrec = value
            elif target_id == myId:
                targetTvrec = value

        if sourceTvrec == None and targetTvrec == None:
            self.increment_counter('join_pair_tvrec_reducer', 'src_tgt_2_none', 1)
            return

        if sourceTvrec == None or targetTvrec == None:
            self.increment_counter('join_pair_tvrec_reducer', 'src_tgt_1_none', 1)
            return

        (score, imp_dict) = self._itemSimilarityFunc.get_similarity_score(sourceTvrec, targetTvrec)
        self.increment_counter('join_pair_tvrec_reducer', 'outputs', 1)
        yield source_id, (score, target_id, imp_dict)

    def group_matches_reducer_configure(self):
        """If domain descriptor is specified on the command line, take it.
        otherwise, fall back to the mockup"""
        if self.options.domain_desc:
            self.domainDescriptor = DomainDescriptor.from_json(
                                        self.options.domain_desc)
        else:
            self.domainDescriptor = DomainDescriptor.fromDbMockUp(
                            self.options.domain, self.options.dataroot)
        self._num_matches = self.domainDescriptor.get_num_matches()
        if self._num_matches is None:
            self._num_matches = DEFAULT_LIMIT

    def group_matches_reducer(self, key, values):
        source_id = key
        self.increment_counter('group_matches_reducer', 'input_groups', 1)
        matches = []
        for value in values:
            self.increment_counter('group_matches_reducer', 'input_recs', 1)
            matches.append(value)
        matches = sorted(matches, reverse=True)

        tgt_list = []
        cnt = 0
        for entry in matches:
            score = entry[0]
            target_id = entry[1]
            imp_dict = entry[2]
            match = Match(target_id, score, imp_dict)
            tgt_list.append(match)
            cnt += 1
            if cnt >= self._num_matches:
                break

        matchList = MatchList(source_id, tgt_list)
        self.increment_counter('group_matches_reducer', 'outputs', 1)
        yield key, matchList.get_matches()


if __name__ == '__main__':
    JobRerankCandidates.run()
