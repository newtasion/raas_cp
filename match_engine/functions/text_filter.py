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
"<",">",".",",","。",":", "!"
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
    text1="""延庆四海镇玩遍大约是一个约130公里的环圈,
    包含四季花海，百里画廊，珍珠泉，留香谷、千家店镇等等民俗旅游村。
    景区多处在山谷内、溪水旁，恬静清幽，秋季盛开时节绮丽无双美不胜收，
    除了留香谷外都无需门票，停车就看，现在正是一年里最美最美的时候。"""
    print TextFilter.text_to_vector(text1, CN_ID)
    print TextFilter.text_to_dict(text1, CN_ID)

    text2 = u'This sentence is similar to a foo bar sentence .'
    print TextFilter.text_to_vector(text2, EN_ID)

    text3 = u'撇开装高深卖优雅的艺术理论，安安静静地刻画内心。每个人都有艺术天分，其实需要一点点耐心、一点点指导，零基础也能画出“大师作”！</p><p> <span style="font-size:14px;line-height:24px;">“自助绘画”（Art Jam）起源于欧美，就是无论是否有基础，不关心画得好或差，没有人评价、没有人打分，只需要带着一点点耐心来到专门画室，画笔、颜料和纸张自助，想画什么画什么，画完带回家。自助画室不是绘画教学班，没有刻板理论，要的就是“轻松作画，放松身心”。</span> <p>  “同或自助画室”取地儿够好，安静的胡同小院，一整面颜料墙，屋里不仅有颜料纸张味儿，还有可口的点心香、茶香。来这儿不需要准备任何材料，有老师指导，绘画套餐包涵饮料点心，绝对是周末避世的好去处。注意需要提前一天预约哟~ </p>     每个人都有艺术天分，其实需要一点点耐心、一点点指导，零基础也能画出“大师作”！'
    print TextFilter.text_to_vector(text3, CN_ID)
    pass


if __name__ == "__main__":
    unitTest()


