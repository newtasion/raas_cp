"""
utilities for ranking. 

Created on Jul 12, 2013
@author: zul110
"""

import heapq
import random

class TopNList(object):
    def __init__(self, N):
        self.N = N
        self.h = []

    def add_element(self, v):
        if len(self.h) < self.N:
            heapq.heappush(self.h, v)
        else:
            heapq.heappushpop(self.h, v)

    def get_list(self):
        self.h.sort(reverse=True)
        return self.h


def t_topn_random():
    topn = TopNList(10)
    for i in xrange(100):
        x = random.randint(0, 1e4)
        topn.add_element((x, str(x)))
    res = topn.get_list()
    print res


if __name__ == '__main__':
    t_topn_random()