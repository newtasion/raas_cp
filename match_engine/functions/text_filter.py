#coding:gbk

"""
  this class provide utilities to generate index files and conduct search.
 
  Created on Jul 05, 2013
  @author: zul110
"""
import os
from collections import Counter

from whoosh.analysis import StemmingAnalyzer
import jieba
from jieba.analyse import ChineseAnalyzer
from jieba.analyse import idf_freq, median_idf, stop_words

EN_ID = 'en'
CN_ID = 'cn'
DEFAULT_LANG = CN_ID
ANALYZER_MAP = {EN_ID: StemmingAnalyzer(), CN_ID: ChineseAnalyzer()}
CNT_TOP_KEYWORDS = 20

html_tags=set([
"span","line","height","size","font","style"
])

note_tags=set([
"<",">",".",",","��",":", "!"
])

_curpath=os.path.normpath(os.path.join( os.getcwd(), os.path.dirname(__file__) )  )
f_name = os.path.join(_curpath,"chinese_stopwords.txt")
content = open(f_name,'rb').read().decode('gbk')
cn_stopwords = set([])
lines = content.split('\n')
for line in lines:
    cn_stopwords.add(line.strip())


class TextFilter():
    def __init__(self, lang_id=None):
        if lang_id is None or lang_id not in ANALYZER_MAP:
            lang_id = DEFAULT_LANG
        self.lang_id = lang_id
        self.analyzer = ANALYZER_MAP[self.lang_id]

    def extract_chinese_tags(self, sentence, topK=20):
        words = jieba.cut(sentence)
        freq = {}
        for w in words:
            if len(w.strip()) < 2:
                continue
            if w.lower() in note_tags or w.lower() in html_tags:
                continue
            if w.lower() in stop_words:
                continue
            if w.lower() in cn_stopwords:
                continue
            freq[w] = freq.get(w,0.0) + 1.0
        total = sum(freq.values())
        normfreq = [(k,v / total) for k,v in freq.iteritems()]

        tf_idf_list = [(v * idf_freq.get(k,median_idf),k) for k,v in normfreq]
        st_list = sorted(tf_idf_list, reverse = True)

        top_tuples = st_list[:topK]
        tags = [a[1] for a in top_tuples]
        #print "\n".join(tags)
        #return dict((word, tfidf) for tfidf, word in top_tuples)
        """
        now we use the tfidf as value.
        The reason is we want to compute cosine similarity
        with tfidf to avoid common words improve score too much.

        we can also consider to use the following:
            return dict((word, tfidf) for tfidf, word in top_tuples)
        or:
            return dict((word, 1) for tfidf, word in top_tuples)
        """
        #print dict((word, round(tfidf)) for tfidf, word in top_tuples)
        #return dict((word, freq[word]) for tfidf, word in top_tuples)
        return dict((word, tfidf) for tfidf, word in top_tuples)

    @staticmethod
    def text_to_vector(text, lang_id=None):
        textFilter = TextFilter(lang_id)
        return Counter(textFilter.to_dict(text))

    @staticmethod
    def text_to_dict(text, lang_id=None):
        textFilter = TextFilter(lang_id)
        return textFilter.to_dict(text)

    def to_dict(self, text):
        if self.lang_id == EN_ID:
            word_dict = self.to_dict_english(text)
        else:
            word_dict = self.to_dict_chinese(text)
        return word_dict

    def to_dict_english(self, text):
        words = []
        for t in self.analyzer(text):
            words.append(t.text)
        return dict(Counter(words))

    def to_dict_chinese(self, text):
        words_tfidf_dict = self.extract_chinese_tags(text, topK=CNT_TOP_KEYWORDS)
        return words_tfidf_dict


def unitTest():
    text1="""�����ĺ�������Լ��һ��Լ130����Ļ�Ȧ,
    �����ļ����������ﻭ�ȣ�����Ȫ������ȡ�ǧ�ҵ���ȵ��������δ塣
    �����ദ��ɽ���ڡ�Ϫˮ�ԣ������ģ��＾ʢ��ʱ�������˫����ʤ�գ�
    ����������ⶼ������Ʊ��ͣ���Ϳ�����������һ��������������ʱ��"""
    print TextFilter.text_to_vector(text1, CN_ID)
    print TextFilter.text_to_dict(text1, CN_ID)

    text2 = u'This sentence is similar to a foo bar sentence .'
    print TextFilter.text_to_vector(text2, EN_ID)

    text3 = u'Ʋ��װ���������ŵ��������ۣ����������ؿ̻����ġ�ÿ���˶���������֣���ʵ��Ҫһ������ġ�һ���ָ���������Ҳ�ܻ�������ʦ������</p><p> <span style="font-size:14px;line-height:24px;">�������滭����Art Jam����Դ��ŷ�������������Ƿ��л����������Ļ��úû�û�������ۡ�û���˴�֣�ֻ��Ҫ����һ�����������ר�Ż��ң����ʡ����Ϻ�ֽ���������뻭ʲô��ʲô��������ؼҡ��������Ҳ��ǻ滭��ѧ�࣬û�п̰����ۣ�Ҫ�ľ��ǡ������������������ġ���</span> <p>  ��ͬ���������ҡ�ȡ�ض����ã������ĺ�ͬСԺ��һ��������ǽ�����ﲻ��������ֽ��ζ�������пɿڵĵ����㡢���㡣���������Ҫ׼���κβ��ϣ�����ʦָ�����滭�ײͰ������ϵ��ģ���������ĩ�����ĺ�ȥ����ע����Ҫ��ǰһ��ԤԼӴ~ </p>     ÿ���˶���������֣���ʵ��Ҫһ������ġ�һ���ָ���������Ҳ�ܻ�������ʦ������'
    print TextFilter.text_to_vector(text3, CN_ID)
    pass


if __name__ == "__main__":
    unitTest()


