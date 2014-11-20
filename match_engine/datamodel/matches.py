"""
a python class for representing a recommendation.

Created on Aug 2013
@author: zul110
"""

from utils.json_utils import objToJson, jsonToObj


class Match:
    """
    Represents a recommendation.
    """
    def __init__(self, targetid, score, exp=None):
        self._targetid = targetid
        self._score = score
        self._exp = exp

    def get_exp(self):
        return self._exp

    def get_targetid(self):
        return self._targetid

    def get_score(self):
        return self._score

    def get_dict(self):
        return {'id': self.get_targetid(),
                'score': self.get_score(),
                'exp': self.get_exp()}

    def to_json(self):
        tdict = {'id': self.get_targetid(),
                 'score': self.get_score(),
                 'exp': self.get_exp()}
        return objToJson(tdict)

    @classmethod
    def from_json(cls, jsonstring):
        tdict = jsonToObj(jsonstring)
        targetid = tdict['id']
        score = tdict['score']
        exp = tdict['exp']
        match = Match(targetid, score, exp)
        return match


class MatchList:
    def __init__(self, sourceid, matches):
        self._sourceid = sourceid
        self._matches = matches

    def get_matches(self):
        listmatchstrs = []
        for match in self.get_targets():
            listmatchstrs.append(match.get_dict())
        return listmatchstrs

    def get_sourceid(self):
        return self._sourceid

    def get_targets(self):
        return self._matches

    def get_targetids(self):
        res = []
        for match in self.get_targets():
            res.append(match.get_targetid())
        return res

    def to_json(self):
        listmatchstrs = []
        for match in self.get_targets():
            listmatchstrs.append(match.to_json())

        tdict = {'id': self.get_sourceid(),
                 'ms': listmatchstrs}
        return objToJson(tdict)

    @classmethod
    def from_json(cls, jsonstring):
        tdict = jsonToObj(jsonstring)
        sourceid = tdict['id']
        listmatchstrs = tdict['ms']
        matches = []
        for matchstr in listmatchstrs:
            match = Match.from_json(matchstr)
            matches.append(match)

        return MatchList(sourceid, matches)


if __name__ == "__main__":
    match1 = Match('testId1', 36.0, None)
    match2 = Match('testId2', 48.0, None)
    sourceid = 'sourceid'
    matchlist = MatchList(sourceid, [match1, match2])
    A = matchlist.to_json()
    matchlist1 = MatchList.from_json(A)
    print matchlist1.to_json()

