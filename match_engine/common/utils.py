"""
string utilities. 

Created on Jul 12, 2013
@author: zul110
"""

from match_engine.settings import TAGS_SEPERATOR
import re

NUM_PATTEN = re.compile('\d+(\.\d+)?')


def taglist_to_str(tagList):
    if tagList is None:
        tagList = []
    return TAGS_SEPERATOR.join(tagList)


def is_number(testStr):
    if NUM_PATTEN.match(testStr) is None:
        return False
    else:
        return True
