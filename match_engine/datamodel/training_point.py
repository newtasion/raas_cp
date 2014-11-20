"""
A data structure for training points.

Created on Aug 2013
@author: zul110
"""

SEPERATOR = '\t'


class TrainingPoint:
    def __init__(self, source, target, label):
        self._source = source
        self._target = target
        self._label = label

    @classmethod
    def fromTsv(cls, line, delimiter):
        if not line:
            return None
        rec = line.split(delimiter)
        if len(rec) < 3:
            return None
        return TrainingPoint(rec[0], rec[1], rec[2])
