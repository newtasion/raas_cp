#!/usr/local/bin/python
#coding=utf-8 
"""
compute the similarity score between 2 items represented by tvrecs.

@author: zul110
"""

from match_engine.settings import SEPERATOR_IN_FEATURE
from match_engine.datamodel.model import Model
from match_engine.datamodel.model import InteractionModelFeature
from match_engine.functions.field_similarity import FieldSimilarity


class TvRecProcessor:
    @staticmethod
    def ana_dict_key():
        return "-1b3sdfaedbsk@#dslb"

    @staticmethod
    def is_ana_dict_key(key):
        return str(key) == TvRecProcessor.ana_dict_key()

    @staticmethod
    def tvrec_analyzer_init():
        ana_dict = {}
        ana_dict['freq'] = {}
        ana_dict['min'] = {}
        ana_dict['max'] = {}

    @staticmethod
    def tvrec_analyzer(tv_rec, ana_dict, domainDescriptor):
        word_freq_dict = ana_dict['freq']
        min_watcher = ana_dict['min']
        max_watcher = ana_dict['max']

        for tv_field in tv_rec.getFields():
            fieldName = tv_field.get_name()
            fieldType = domainDescriptor.get_type(fieldName)

            # count the frequencies of words
            if fieldType in ['text', 'TEXT']:
                for ele, freq in tv_field.get_term_vector().items():
                    if ele in word_freq_dict:
                        word_freq_dict[ele] += freq
                    else:
                        word_freq_dict[ele] = freq

            # find the min and max of numeric values.
            elif fieldType in ['integer', 'float']:
                minv = float('inf')
                maxv = float('-inf')
                for ele, freq in tv_field.get_term_vector().items():
                    ele = float(ele)
                    if ele < minv:
                        minv = ele
                    if ele > maxv:
                        maxv = ele

                if fieldName not in min_watcher:
                    min_watcher[fieldName] = minv
                else:
                    curr_min = min_watcher[fieldName]
                    min_watcher[fieldName] = min(curr_min, minv)

                if fieldName not in max_watcher:
                    max_watcher[fieldName] = maxv
                else:
                    curr_max = max_watcher[fieldName]
                    max_watcher[fieldName] = max(curr_max, minv)

        return ana_dict


