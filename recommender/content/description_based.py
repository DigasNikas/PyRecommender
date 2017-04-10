"""
# By Diogo Nicolau
"""

import pandas as pd
import time
import sys
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import linear_kernel
from nltk.corpus import stopwords
import datetime
import re
clean_r = re.compile('<.*?>')
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


def _train(ds, output, number_recs, logfile):

    """
    Train the engine.
    Create a TF-IDF matrix of unigrams, bigrams, and trigrams
    for each product. The 'stop_words' param tells the TF-IDF
    module to ignore common english words like 'the', etc.
    Then we compute similarity between all products using
    SciKit Learn's linear_kernel (which in this case is
    equivalent to cosine similarity).
    Iterate through each item's similar items and store the
    n most-similar.
    Similarities and their scores are stored in txt as a
    Sorted Set, with one set for each item.
    :param ds: A pandas dataset containing two fields: description & id
    :return: txt file with recommendations per item
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
            similar_items = [(cosine_similarities[idx][i], ds['id'][i])
                             for i in similar_indices]

            result_string = '"{}"'.format(ds['id'][idx])
            for similar_item in similar_items:
                if similar_item[1] != ds['id'][idx]:
                    result_string += ',("{}",{})'.format(str(similar_item[1]), str(similar_item[0]))
            outf.write(result_string+"\n")


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2], sys.argv[3])
