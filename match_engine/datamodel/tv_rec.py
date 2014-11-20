"""
A data structure for term vector.

Created on Aug 2013
@author: zul110
"""

from tv_field import TvField
SEPERATOR = '\t'


class TvRec:
    def __init__(self, iid, fields):
        self._id = unicode(iid)
        self._fields = fields

    def getId(self):
        return self._id

    def getFields(self):
        return self._fields

    def toString(self):
        output_list = [str(self._id)]
        for field in self._fields:
            output_list.append(field.to_string())
        return SEPERATOR.join(output_list)

    @classmethod
    def fromString(txtString):
        if txtString is None:
            return None
        inputList = txtString.split(SEPERATOR)
        if len(inputList) <= 0:
            return None

        iid = inputList[0]
        fields = []
        for ele in inputList[1:]:
            ele = str(ele)
            ret = TvField.from_string(ele)
            if ret == None:
                continue
            fields.append(ret)

        return TvRec(iid, fields)
