"""
utilities for processing text.

Created on Jul 12, 2013
@author: zul110
"""

from datetime import datetime
from nltk.corpus import stopwords
import hashlib
import math
import re


WORD = re.compile(r'\w+')
DATETIME_FORMAT = ['%Y%m%d', '%Y %m %d', '%Y/%m/%d', '%Y-%m-%d', "%Y-%m-%d %H:%M:%S"]


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


def remove_stopwords(text):
        text = text.lower()
        # remove punctuation and split into seperate words
        words = re.findall(r'\w+', text, flags=re.UNICODE | re.LOCALE)
        # This is the simple way to remove stop words
        important_words = []
        for word in words:
            if word not in stopwords.words('english'):
                important_words.append(word)
        return ' '.join(important_words)


def text_split_vectorize(text):
    words = WORD.findall(text)
    return Counter(words)


def norm_dict(d):
    results = {}
    for k, v in d.iteritems():
        kl = k.lower() if isinstance(k, basestring) else k
        if isinstance(v, basestring):
            results[kl] = v.lower()
        elif isinstance(v, dict):
            results[kl] = norm_dict(v)
        else:
            results[kl] = v
    return results


def getMD5hash(string):
    m = hashlib.md5()
    m.update(string.encode('utf-8'))
    return m.hexdigest()


def is_empty(ipt):
    """
    None, '', Null, none
    """
    if ipt is None:
        return True
    iptStr = unicode(ipt).strip()
    if iptStr == '' or iptStr == "null" or iptStr == "none":
        return True
    return False


def is_number(ipt):
    try:
        float(ipt)
        return True
    except ValueError:
        return False


def is_int(ipt):
    try:
        int(ipt)
        return True
    except ValueError:
        return False


def is_datetime(ipt):
    for fmt in DATETIME_FORMAT:
        try:
            datetime.strptime(ipt, fmt)
            return True
        except:
            continue
    return False


def unitTest():
    text1 = 'This is a foo bar sentence .'
    text2 = 'This sentence is similar to a foo bar sentence .'
    print remove_stopwords(text1)
    print text_split_vectorize(remove_stopwords(text2))
    vector1 = text_split_vectorize(remove_stopwords(text1))
    vector2 = text_split_vectorize(remove_stopwords(text2))
    cosine = get_cosine(vector1, vector2)
    print 'Cosine:', cosine

if __name__ == "__main__":
    unitTest()
