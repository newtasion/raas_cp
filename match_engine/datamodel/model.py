"""
a python class for parsing a model like the following :  

Created on Jul 12, 2013
@author: zul110
"""

import math
from utils.json_utils import jsonToObj

IMPORTANCE_THRESHOLD = 0.02
NUM_IMPORTANT_FEATURES = 3


class ModelFeature(object):
    def __init__(self, name, weight, method=None):
        if name == None or weight == None or type(weight) is not float:
            raise ValueError("the input %s or %s is not correct" % (name, weight))
        self._name = name
        self._weight = weight
        self._method = method

    def getName(self):
        return self._name

    def getWeight(self):
        return self._weight

    def getMethod(self):
        return self._method

    @classmethod
    def getTypeFromFeatureDict(cls, featureDict):
        fName = featureDict['name']
        sides = fName.split(':')
        if len(sides) == 2 and 'method' in featureDict:
            return InteractionModelFeature
        else:
            return OrdinaryModelFeature 
        
    @classmethod
    def from_dict(cls, fDict):
        """
        input such as :
        featurename': {'weight' : 0.6}
        """
        name = fDict['name']
        weight = fDict['weight']
        method = None
        if 'method' in fDict:
            method = fDict['method']
        return cls(name, weight, method)


class OrdinaryModelFeature(ModelFeature): 
    type = 'ordinary'

    
class InteractionModelFeature(ModelFeature):
    type = 'interaction'   
    
    def __init__(self, name, weight, method):
        super(InteractionModelFeature, self).__init__(name, weight, method)
        sides = self._name.split(':')
        if len(sides) != 2:
            raise ValueError("the feature is not an interaction feature")

        self._sourceField = sides[0]
        self._targetField = sides[1]

    @classmethod
    def get_feature_name(cls, sourceField, targetField):
        return sourceField + ':' + targetField

    def get_source_field(self):
        return self._sourceField

    def get_target_field(self):
        return self._targetField

    def get_method(self):
        return self._method

    @classmethod
    def from_dict(cls, fDict):
        """
        input such as :
        title:title': {'weight' : 0.6, 'method' : 'CONSINE'}
        """
        name = fDict['name']
        weight = fDict['weight']
        method = fDict['method']
        return cls(name, weight, method)


class Model:
    def __init__(self, intercept, features):
        self._intercept = intercept             # a float value
        self._features = features               # a map

    @classmethod
    def from_json_mockup(cls):
        modeljson = """
        {
          'intercept' : 0.1,
          'features' :
          {
              'title:title' : {'weight' : 0.6, 'method' : 'CONSINE'},
              'highlights:highlights': {'weight' : 0.6, 'method' : 'CONSINE'},
              'genre:genre': {'weight' : 0.6, 'method' : 'LIST_OVERLAPPING'},
              'price:price':{'weight' : 0.2, 'method' : 'SUBSTRACT'}
          }
        }"""
        modelobj = Model.from_json(modeljson)
        return modelobj

    @classmethod
    def from_dict(cls, model_dict):
        intercept = model_dict['intercept']
        featuresDict = model_dict['features']

        features = {}
        for fName, fDict in featuresDict.items():
            fDict['name'] = fName
            featureCls = ModelFeature.getTypeFromFeatureDict(fDict)
            feature = featureCls.from_dict(fDict)
            features[fName] = feature
        return cls(intercept, features)

    @classmethod
    def from_json(cls, jsonString):
        model_dict = jsonToObj(jsonString)
        return cls.from_dict(model_dict)

    def get_intercept(self):
        return self._intercept

    def get_features(self):
        return self._features

    def get_feature(self, featureName):
        if self.has_feature(featureName) == False:
            return None
        else:
            return self._features[featureName]

    def has_feature(self, featureName):
        if featureName in self._features:
            return True
        else:
            return False

    def get_feature_weight(self, featureName):
        feature = self.get_feature(featureName)
        return feature.getWeight()

    def logit(self, weightedLinearSum):
        return 1.0 / (math.pow(math.e, -weightedLinearSum) + 1.0)

    def get_important_features(self, contribution_dict, total_contribution):
        contribution_list = sorted(contribution_dict.items(), key=lambda x: x[1])
        score_th = IMPORTANCE_THRESHOLD * total_contribution
        got = 0
        imp_dict = {}
        for fname, contribution in reversed(contribution_list):
            if contribution >= score_th:
                imp_dict[fname] = contribution * 1.0 / total_contribution
                got += 1
                if got >= NUM_IMPORTANT_FEATURES:
                    break
        return imp_dict

    def compute_score_exp(self, featureValues):
        """
        return overall score and explanation
        """
        weightedLinearSum = 0.0
        contribution_dict = {}
        for f in featureValues:
            featureName = f['name']
            featureValue = f['value']
            mycontribution = self.get_feature_weight(featureName) * featureValue
            weightedLinearSum += mycontribution
            contribution_dict[featureName] = mycontribution
            #print featureName + "  mycontribution: " + str(mycontribution)

        imp_dict = self.get_important_features(contribution_dict, weightedLinearSum)
        weightedLinearSum += self._intercept
        return self.logit(weightedLinearSum), imp_dict


def unitTest1():
    modelJson = """
        {
          'intercept' : 0.1,
          'features' :
          {
              'title:title' : {'weight' : 0.6, 'method' : 'CONSINE'},
              'highlights:highlights': {'weight' : 0.6, 'method' : 'CONSINE'},
              'category:category': {'weight' : 0.6, 'method' : 'DATE_DIFFERENCE'}
          }
        }"""
    modelobj = Model.from_json(modelJson)
    print modelobj.get_intercept()
    print modelobj.get_features()
    print modelobj.get_feature('title:title')
    print modelobj.get_feature_weight('title:title')

    featureValues = [
          {
            'name' : 'title:title',
            'value': 0.4
          },
          {
            'name' : 'highlights:highlights',
            'value': 0.1
          }
        ]
    print modelobj.compute_score(featureValues)
    print modelobj.logit(0.26)


def unitTest2():
    modelJson = """
        {
          'intercept' : 0.1, 
          'features' : 
          {
              'length' : {'weight' : 0.6, },
              'highlights:highlights': {'weight' : 0.6, 'method' : 'CONSINE'},
              'category:category': {'weight' : 0.6, 'method' : 'DATE_DIFFERENCE'}
          }
        }"""
    modelobj = Model.from_json(modelJson)
    print modelobj.get_intercept()
    print modelobj.get_features()
    print modelobj.get_feature('title:title')
    print modelobj.get_feature_weight('title:title')

    featureValues = [
          {
            'name' :'title:title',
            'value':0.4
          },
          {
            'name' :'highlights:highlights',
            'value':0.1
          }
        ]
    print modelobj.compute_score(featureValues)
    print modelobj.logit(0.26)


if __name__ == '__main__':
    unitTest1()
    unitTest2()
