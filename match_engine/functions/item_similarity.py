#!/usr/local/bin/python
#coding=utf-8 
#item_similarity:h.py

"""
compute the similarity score between 2 items represented by tvrecs.

Created on Jul 01, 2013
@author: zul110
"""

from match_engine.settings import SEPERATOR_IN_FEATURE
from match_engine.datamodel.model import Model
from match_engine.datamodel.model import InteractionModelFeature
from match_engine.functions.field_similarity import FieldSimilarity


class ItemSimilarityFunc:
    def __init__(self, model):
        self._model = model

    def get_features(self, tvrec1, tvrec2):
        """
        given two items' term vector list, return a similarity score.
        A term vector list is a representation of a program.
        """
        features = []

        for sourceField in tvrec1.getFields():
            sourceFieldName = sourceField.get_name()
            sourceFieldTVector = sourceField.get_term_vector()

            for targetField in tvrec2.getFields():
                targetFieldName = targetField.get_name()
                targetFieldTVector = targetField.get_term_vector()

                featureName = InteractionModelFeature.get_feature_name(sourceFieldName, targetFieldName)
                modelFeature = self._model.get_feature(featureName)

                if modelFeature == None:
                    continue
                method = modelFeature.getMethod()
                featureValue = FieldSimilarity.get_similarity(sourceFieldTVector, targetFieldTVector, method)

                feature = {'name': featureName, 'value': featureValue}
                features.append(feature)

        return features

    def get_similarity_score(self, tvrec1, tvrec2):
        """
        return overall score and explanation
        """
        features = self.get_features(tvrec1, tvrec2)
        return self._model.compute_score_exp(features)

