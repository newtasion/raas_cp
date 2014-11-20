"""
a python class for representing a UserRate.

Created on Aug 2013
@author: zul110
"""

IGNORED_FIELDS = [u'domain', u'id']

import cPickle
from utils.json_utils import jsonToObj

TSV_DELIMITERS = ['|', '\t']


class UserRate:
    """
    Represents a user rating action: 
        uid: user's id
        iid: item's id
        rate: the rating. can be binary or float.
    """
    def __init__(self, uid, iid, rate):
        self._uid = uid
        self._iid = iid
        self._rate = rate

    @classmethod
    def from_line(cls, line, lineformat='json'):
        lineformat = str(lineformat).lower()
        if lineformat == 'json':
            return cls.fromJson(line)
        elif lineformat == 'tsv':
            return cls.fromTsv(line)
        elif lineformat == 'pickle':
            return cls.fromTsv(line)
        else:
            return cls.fromJson(line)

    @classmethod
    def from_pickle(cls, tsvLine):
        return cPickle.loads(tsvLine.decode('string_escape'))

    @classmethod
    def fromTsv(cls, tsvLine):
        delimiter = None
        for test_delimiter in TSV_DELIMITERS:
            if tsvLine.find(test_delimiter) > 0:
                delimiter = test_delimiter

        uid, iid, rate = tsvLine.split(delimiter)
        return UserRate(uid, iid, rate)

    @classmethod
    def fromJson(cls, descJson):
        try:
            imap = jsonToObj(descJson)
        except:
            return None
        if 'uid' in imap and 'iid' in imap and 'rate' in imap:
            iid = unicode(imap['iid'])
            uid = unicode(imap['uid'])
            rate = unicode(imap['rate'])
            return UserRate(uid, iid, rate)
        else:
            return None

    def getIid(self):
        return self._iid

    def getUid(self):
        return self._uid

    def getRate(self):
        return self._rate


def unitTest():
    pass


if __name__ == '__main__':
    unitTest()
