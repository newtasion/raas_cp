"""
A representation of one field.

Created on Aug 2013
@author: zul110
"""


class TvField(object):
    def __init__(self, name, termVector):
        self._name = name                       # a string
        self._termVector = termVector           # a dictionary.

    def to_dict(self):
        return {'name': self._name, 'termVector': self._termVector}

    def to_string(self):
        tdict = self.to_dict()
        return str(tdict)

    @classmethod
    def from_string(cls, txtString):
        tvrecobj = None
        try:
            dictobj = eval(txtString)
            name = dictobj['name']
            termVector = dictobj['termVector']
            tvrecobj = TvField(name, termVector)
        except:
            pass
        return tvrecobj

    def get_name(self):
        return self._name

    def get_term_vector(self):
        return self._termVector

    def get_terms_as_str(self):
        """only use every term once
        """
        strlist = []
        for name, cnt in self._termVector.items():
            cnt = 1
            # cnt = int(cnt)
            for i in range(cnt):
                try:
                    strlist.append(name.decode())
                except UnicodeEncodeError:
                    strlist.append(unicode(name))
                except Exception, e:
                    raise e
        return u" ".join(strlist)


def unitTest():
    from match_engine.datamodel.field_type import TextField, KeywordsField, IdField
    textField = TextField()
    tf31 = textField.toTvRecField('title', 'pig is a pig language to start processing steps')
    print tf31.get_terms_as_str()

    keywordField = KeywordsField()
    tf32 = keywordField.toTvRecField('genre', 'music,arts,arts')
    print tf32.get_terms_as_str()

    idField = IdField()
    tf32 = idField.toTvRecField('id', 'abcde123')
    print tf32.get_terms_as_str()


if __name__ == "__main__":
    unitTest()
