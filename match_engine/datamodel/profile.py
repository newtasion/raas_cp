"""
a python class for representing a profile.

Created on Aug 2013
@author: zul110
"""

IGNORED_FIELDS = [u'domain', u'id']

import cPickle
import json
from tv_rec import TvRec
from match_engine.datamodel.field_type import Field


class Profile:
    def __init__(self, imap, iid, domain):
        self._imap = imap
        self._iid = iid
        self._domain = domain

    @classmethod
    def fromLine(cls, line, lineformat='json'):
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
    def fromPickle(cls, tsvLine):
        return cPickle.loads(tsvLine.decode('string_escape'))

    @classmethod
    def fromTsv(cls, tsvLine):
        return None

    @classmethod
    def fromJson(cls, descJson):
        imap = json.loads(descJson)
        if 'id' in imap:
            iid = unicode(imap['id'])
            if 'domain' in imap:
                domain = unicode(imap['domain'])
            else:
                domain = unicode('')
            return Profile(imap, iid, domain)
        else:
            return None

    def getIid(self):
        return self._iid

    def getDomain(self):
        return self._domain

    def getImap(self):
        return self._imap

    def toTvRec(self, domainDescriptor):
        """
        convert a profile to a tv_rec
        """
        myid = self._iid
        myfields = []

        for key, value in self._imap.items():
            fieldName = unicode(key).lower()
            if fieldName in IGNORED_FIELDS:
                continue
            fieldType = domainDescriptor.get_type(fieldName)
            if (fieldType == None):
                continue  # if we dont know the fieldType, just skip

            props = {'analyzer': domainDescriptor.get_language()}
            tvField = Field.to_tvrec_field(fieldName, fieldType, value, props)

            if tvField == None:
                continue
            myfields.append(tvField)

        if len(myfields) == 0:
            return None

        return TvRec(myid, myfields)


def unitTest():
    line1 = """{"domain": 0, "PartNumber": "FD9S0.33RI", "Title": \
    "1/3 CT Diamond Stud Earrings 14k Gold (I1-I2 Clarity)", " \
    image": "http://ecx.images-amazon.com/images/I/41LZK7rY2hL.jpg",  \
    "Feature": "Made in USA", "ProductGroup": "Jewelry", "Label": "FineDiamonds9",  \
    "ProductTypeName": "FINEEARRING", "Department": "women",  \
    "EditorialReviews": "This is a beautiful pair of Round Cut Natural Diamond Stud Earrings.  \
    They are made in Solid 14K White Gold and weigh 1/2 Gram (Total Weight).  \
    The 1/3 ctw (Total Weight) Natural Diamonds are I1-I2 in Clarity & H-I in Color. Very well made,  \
    the earring is hallmarked 14K Gold. The dimensions of each earring is 3.2 mm.  \
    These earrings have push-backs. The setting is 4-Prong.", "id": 0, "ListPrice": "", "Brand": "FineDiamonds9", "Studio": "FineDiamonds9", "Manufacturer": "FineDiamonds9", "Publisher": "FineDiamonds9", "ItemDimensions": "", "Model": "FD9S0.33RI", "Creator": "FineDiamonds9", "MPN": "FD9S0.33RI", "Binding": "Jewelry", "link": "http://www.amazon.com/Diamond-Stud-Earrings-I1-I2-Clarity/dp/B0069J0P8I%3FSubscriptionId%3DAKIAJWCMUSAHYSQH6OPQ%26tag%3Dtaotaomom-20%26linkCode%3Dxm2%26camp%3D2025%26creative%3D165953%26creativeASIN%3DB0069J0P8I"}"""
    profileObj = Profile.fromJson(line1)
    print profileObj.getIid()
    print profileObj.getImap()
    print profileObj.getIid()

    from match_engine.datamodel.domain_descriptor import DomainDescriptor
    domainDescriptor = DomainDescriptor.fromDbMockUp('test')
    profileObj.toTvRec(domainDescriptor)

if __name__ == '__main__':
    unitTest()
