# PyRecommender
    Content and Colaborative recommenders based on python and several python libraries.
    The context of this project focus around recommending Android applications.
### Colaborative Filtering
It is available a recommender based on [Spark's **ALS**](https://spark.apache.org/docs/latest/mllib-collaborative-filtering.html). This recommender is a matrix factorization algorithm that uses *Alternating Least Squares* with *Weighted-Lamda-Regularization* (ALS-WR).

The method factors the [user, item] matrix *A* into the [user, feature] matrix *U* and the [item, feature] matrix *M*: It runs the **ALS** algorithm in a parallel fashion.  The **ALS** algorithm should uncover the latent factors that explain the observed [user, item] ratings and tries to find optimal factor weights to minimize the least squares between predicted and actual ratings.

The recommendation job can work in several ways:
  * Item to Item;
  * Item to User;
  * User to User;
  * User to Item;

Both item2item and user2user use [**Annoy**](https://github.com/spotify/annoy) to calculate the nearest neighbors. After running matrix factorization algorithms, every user/item can be represented as a vector in f-dimensional space, matrix *U* and *M*. We simply have to compute the *cosine similarity* between each entry for each matrix. The nearest neighbors from *U* are the most similar users while those from *M* are the most similar items.

Nearest neighbors refers to something that is conceptually very simple, however with billions of entries one needs to use approximated methods and that's what **Annoy** (*Approximate Nearest Neighbors*) stands for.



### Content-Based
It is available a recommender based on item's descriptions. This model uses a simple Natural Language Processing technique called **TF-IDF** (*Term Frequency - Inverse Document Frequency*) to parse through the descriptions, identify distinct n-grams in each item's description, and then find 'similar' products based on those n-grams.

**TF-IDF** works by looking at all uni, bi, and tri-grams that appear multiple times in a description (the "term frequency") and divides them by the number of times those same n-grams appear in all product descriptions. So terms that are 'more distinct' to a particular product get a higher score, and terms that appear often but also appear often in other products get a lower score.

Once we have the **TF-IDF** terms and scores for each item, we'll use *cosine similarity* to identify which items are 'closest' to each other. Python's SciKit Learn has both **TF-IDF** and *cosine similarity*.

#### Key-Words by Description
Based on the previous model there is also an implementation which provides key-words for an item based on the most similar descriptions. This can be used to enhance search results which otherwise would solely be based on the item's description.

**Return example:**<br/>

| App | uni-grams | bi-grams | tri-grams |
| --- | --------- | -------- | --------- |
| [Naked Wing](https://apps.store.aptoide.com/app/market/com.clicknect.games.nakedwing/7/6896801/Naked+Wing) | chicken, game, run, help, cute, ninja, jump, save, catch, fly | chicken run, help chicken, catch chicken, invader jump, ninja chicken, chicken invader, chicken crossing, crossing road, fork knife, jumpy chick | ninja chicken invader, chicken invader jump, help chicken run, chicken crossing road, cute egg laying, chicken falling sky, invader jump up, dash bounce chick, chick maximum time |
| [Angry Girlfriend](https://patddfan7.store.aptoide.com/app/market/com.ihate.myboyfriend/22/7805332/Angry+Girlfriend) | ninja, game, slice, fruit, like, arcade, you, cutting, cut, time | like ninja, ninja warrior, fruit cutter, arcade hopper, animal ninja, ninja game, game play, game you, ninja style | arcade hopper game, don t slice,t slice bombs, lion rhino buffalo, ninja cat monkey, cute animal ninja, fall screen aware, graphics great effects, bombs otherwise explode |