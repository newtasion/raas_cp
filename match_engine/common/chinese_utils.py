#coding:gbk
"""
utilities for dealing with chinese. 


Created on Jul 12, 2013
@author: zul110
"""

import os
import sys
import math
from collections import Counter
sys.path.append('/usr/local/lib/python2.7/site-packages')
import jieba

# Use this table to filter all the puctuations.
PUNCTUATION_TABLE = [
    (0x2000, 0x206f),  # General Punctuation
    (0x3000, 0x303f),  # CJK Symbols and Punctuation
    (0xff00, 0xffef),  # Halfwidth and Fullwidth Forms
    (0x00, 0x40),      # ascii special charactors
    (0x5b, 0x60),      # ascii special charactors
    (0x7b, 0xff),      # ascii special charactors
]

PUNCTUATION_RANGE = reduce(lambda x, y: x.union(y), [range(start, stop+1) for start, stop in PUNCTUATION_TABLE], set())

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
CHINESE_STOPWORDS = os.path.join(PROJECT_ROOT, "resources/chinese_stopwords.txt")
stopwords = {}.fromkeys([line.rstrip() for line in open(CHINESE_STOPWORDS)])


def remove_stopwords(text):
    important_words = get_important_words(text)
    return ' '.join(important_words)


def get_important_words(text):
    words = jieba.cut(text, cut_all=False)
    important_words = []
    for word in words:
        word = word.encode('gbk')
        if len(word) == 1 and ord(word) in PUNCTUATION_RANGE:
            continue
        if word not in stopwords:
            important_words.append(word)
    return important_words


def text_to_vector(text):
    words = get_important_words(text)
    return Counter(words)


def get_cosine(vec1, vec2):
    intersection = set(vec1.keys()) & set(vec2.keys())
    numerator = sum([vec1[x] * vec2[x] for x in intersection])
    sum1 = sum([vec1[x] ** 2 for x in vec1.keys()])
    sum2 = sum([vec2[x] ** 2 for x in vec2.keys()])
    denominator = math.sqrt(sum1) * math.sqrt(sum2)

    if not denominator:
        return 0.0
    else:
        return float(numerator) / denominator


def unitTest():
    print remove_stopwords('北京附近的租房')

if __name__ == "__main__":
    unitTest()

