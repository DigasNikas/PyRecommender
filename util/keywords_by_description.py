"""
# By Diogo Nicolau
"""

import pandas as pd
import time
import sys
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import linear_kernel
from nltk.corpus import stopwords
import collections
import datetime
import re
clean_r = re.compile('[0-9]+|<.*?>')
stops = set(stopwords.words("english"))


def main(data_source,output,number_recs):
    start = time.time()
    logfile = open('log.txt', 'a')

    print('\nProcess started....')
    logfile.write('\nProcess started....\n')

    print('Loading input file....')
    logfile.write('Loading input file....\n')

    ds = pd.read_csv(data_source)
    ds['description'] = ds['description'].apply(clean)
    print("Training data ingested in %s seconds." % (time.time() - start))

    start = time.time()
    _train(ds, output, number_recs, logfile)
    print("Engine trained in %s seconds." % (time.time() - start))


def _train(ds, output, number_recs, logfile):

    """
    Train the engine.

    Create a TF-IDF matrix of unigrams, bigrams, and trigrams
    for each product. The 'stop_words' param tells the TF-IDF
    module to ignore common english words like 'the', etc.

    Then we compute similarity between all products using
    SciKit Leanr's linear_kernel (which in this case is
    equivalent to cosine similarity).

    Iterate through each item's similar items and store the
    100 most-similar. Stops at 100 because well...  how many
    similar products do you really need to show?

    Similarities and their scores are stored in redis as a
    Sorted Set, with one set for each item.

    :param ds: A pandas dataset containing two fields: description & id
    :return: Nothin!
    """

    dt = datetime.datetime
    print('Training started....')
    logfile.write('Training started....\n')

    print('TF-IDF Vectorizing....')
    logfile.write('TF-IDF Vectorizing....\n')

    tf = TfidfVectorizer(analyzer='word',
                         ngram_range=(1, 3),
                         min_df=0,
                         stop_words='english')

    print('Classification of text.... [{}]'.format(dt.now()))
    logfile.write('Classification of text.... [{}]\n'.format(dt.now()))

    tfidf_matrix = tf.fit_transform(ds['description'])

    print('Extract similarity matrix...[{}]'.format(dt.now()))
    logfile.write('Extract similarity matrix...[{}]\n'.format(dt.now()))

    cosine_similarities = linear_kernel(tfidf_matrix, tfidf_matrix)

    print('Finding most similar items started....[{}]'.format(dt.now()))
    logfile.write('Finding most similar items started....[{}]\n'.format(dt.now()))

    with open(output, 'w') as outf:
        for idx, row in ds.iterrows():
            similar_indices = cosine_similarities[idx].argsort()[:-int(number_recs):-1]

            descriptions = [(ds['description'][i]) for i in similar_indices]
            ngrams = count_ngrams(descriptions)
            outf.write('"' + ds['id'][idx] + '"')
            print_most_frequent(ngrams, outf)


def tokenize(string):
    """
    Convert string to lowercase and split into words (ignoring
    punctuation), returning list of words.
    """
    return re.findall(r'\w+', string.lower())


def count_ngrams(lines, min_length=1, max_length=3):
    """
    Iterate through given lines iterator (file object or list of
    lines) and return n-gram frequencies. The return value is a dict
    mapping the length of the n-gram to a collections.Counter
    object of n-gram tuple and number of times that n-gram occurred.
    Returned dict includes n-grams of length min_length to max_length.
    """
    lengths = range(min_length, max_length + 1)
    ngrams = {length: collections.Counter() for length in lengths}
    queue = collections.deque(maxlen=max_length)

    # Helper function to add n-grams at start of current queue to dict
    def add_queue():
        current = tuple(queue)
        for length in lengths:
            if len(current) >= length:
                ngrams[length][current[:length]] += 1

    # Loop through all lines and words and add n-grams to dict
    for line in lines:
        for word in tokenize(line):
            queue.append(word)
            if len(queue) >= max_length:
                add_queue()

    # Make sure we get the n-grams at the tail end of the queue
    while len(queue) > min_length:
        queue.popleft()
        add_queue()

    return ngrams


def print_most_frequent(ngrams, outf, num=10):
    """
    Print num most common n-grams of each length in n-grams dict.
    """
    for n in sorted(ngrams):
        outf.write(",(")
        k = 1
        for gram, count in ngrams[n].most_common(num):
            outf.write('"{0}":{1}'.format(' '.join(gram), count))
            if k != num:
                outf.write(",")
            k += 1
        outf.write(")")
    outf.write("\n")


def clean(raw):
    """
    :param raw: the raw description
    :return result: a new description without any stopword nor html markups
    """
    clean_text = re.sub(clean_r, '', raw)
    result = ""
    for word in clean_text.split():
        if word.lower() not in stops:
            result = result + " " + word.lower()
    return result


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2], sys.argv[3])