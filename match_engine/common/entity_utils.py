import nltk

#with open('sample.txt', 'r') as f:
#    sample = f.read()

sample = """
I am trying to use NLTK toolkit to get extract place, date and time from text messages. 
I just installed the toolkit on my machine and I wrote this quick snippet to test it out:
"""
sentences = nltk.sent_tokenize(sample)
tokenized_sentences = [nltk.word_tokenize(sentence) for sentence in sentences]
tagged_sentences = [nltk.pos_tag(sentence) for sentence in tokenized_sentences]
chunked_sentences = nltk.batch_ne_chunk(tagged_sentences, binary=True)


def extract_entity_names(t):
    entity_names = []

    if hasattr(t, 'node') and t.node:
        if t.node == 'NE':
            entity_names.append(' '.join([child[0] for child in t]))
        else:
            for child in t:
                entity_names.extend(extract_entity_names(child))

    return entity_names


entity_names = []
for tree in chunked_sentences:
    entity_names.extend(extract_entity_names(tree))

# Print unique entity names
print set(entity_names)