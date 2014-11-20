"""
a python class for scoring a feature vector using a model. 

Created on Jul 12, 2013
@author: zul110
"""

import math


class ModelScorer:
    def __init__(self, model):
        self._model = model
        self._intercept = self._model['intercept']

    def get_model(self):
        return self._model

    def has_feature(self, featureName):
        if featureName in self._model:
            return True
        else:
            return False

    def compute_score(self, featureValues):
        """
        compute the score using the model.
        """
        weightedLinearSum = 0.0
        for f in featureValues:
            featureName = f['name']
            featureValue = f['value']
            weightedLinearSum += self.get_weight(featureName) * featureValue

        weightedLinearSum += self._intercept
        return self.logit(weightedLinearSum)

    def get_weight(self, featureName):
        return self._model[featureName]

    def logit(self, weightedLinearSum):
        return 1.0 / (math.pow(math.e, -weightedLinearSum) + 1.0)
