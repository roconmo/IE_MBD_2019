# Necessary Imports
import matplotlib.pyplot as plt
import seaborn as sns
import re
import nltk
import string
from string import punctuation
from nltk.corpus import stopwords
from nltk.tokenize import TweetTokenizer
from nltk.stem import PorterStemmer
import pandas as pd
import nltk
import numpy as np
from nltk.tokenize import word_tokenize
from collections import Counter
from nltk.tag import pos_tag
from nltk.stem import WordNetLemmatizer
from sklearn.feature_extraction.text import CountVectorizer,TfidfVectorizer
from sklearn.model_selection import train_test_split
from sklearn.naive_bayes import MultinomialNB
from sklearn.pipeline import Pipeline
from sklearn.model_selection import GridSearchCV
from sklearn.model_selection import cross_val_score
from collections import Counter

#nltk.download('stopwords')
##nltk.download('punkt')
#nltk.download('wordnet')
#nltk.download('averaged_perceptron_tagger')

# Read the dataset
train = pd.read_csv("data/fake_or_real_news_training.csv")
test = pd.read_csv("data/fake_or_real_news_test.csv")

train.info()

train.shape

train['X1'].value_counts()

test.info()

test.shape


### Pre-Processing
#We checked the values in label column and found out that there are rows that have neither "REAL" or "FAKE"
#as values, which we subsetted below. Since there are additional columns such as X1 and X2 that might contain the
#label values for the "wrongly labeled" rows, we decided to assign the X1 values to those rows.
#After this procedure, there were still two rows with wrong label AND X1 values and these were deleted.###

train['label'].value_counts()

wrong_label = train[(train["label"]!="REAL") & (train["label"]!="FAKE")]
wrong_label

# The X1 value might contain the label value in this cases, so the values are assigned to the label column
train.loc[(train["label"]!="REAL") & (train["label"]!="FAKE"),"label"] = train[(train["label"]!="REAL") & (train["label"]!="FAKE")]["X1"]
# Still remains 2 rows with wrong label and X1 values, and these are deleted

wrong_label2 = train[(train["label"]!="REAL") & (train["label"]!="FAKE")]
wrong_label2

len(wrong_label2)

wrong_label2.index

# Remove rows where label column has irrelevant values
train = train.drop(wrong_label2.index)
train.shape

# Check the value counts for label column
train["label"].value_counts()

# Check if there is any NAs in label column
train[(train["label"].isnull())]

# Now drop X1 and X2 as we have all the information we need in label column
train = train.drop(["X1", "X2"], axis = 1)

# Merge the train and test dataset for pre-processing purpose
df_merged = pd.concat([train,test], sort = True)
df_merged.head()

df_merged.shape

df_merged['label'].value_counts()

#We checked to see if there are rows with missing text values, because text value will be used to train our
# models. We found out there are 36 rows with no text. For these files with no text at all, we input the title
# instead.

Empty_text = df_merged[(df_merged["text"]==" ")]
Empty_text

len(Empty_text)

#There were some files with no text at all, so we decided to input the title instead
df_merged.loc[(df_merged["text"]==" "),"text"] = df_merged[(df_merged["text"]==" ")]["title"]

#For the very first step of text processing, we cleaned the files by removing stopwords, puncutations,
# special characters, hashtags, and etc.

punctuation = string.punctuation
punctuation += " ’"
punctuation += " “"

cache_english_stopwords = stopwords.words('english')

# Clean the files
def row_clean(row):
    # Remove HTML special entities (e.g. &amp;)
    row_no_special_entities = re.sub(r'\&\w*;', '', row)
    # Remove tickers (Clickable stock market symbols that work like hashtags and start with dollar signs instead)
    row_no_tickers = re.sub(r'\$\w*', '',
                            row_no_special_entities)  # Substitute. $ needs to be escaped because it means something in regex. \w means alphanumeric char or underscore.
    # Remove hyperlinks
    row_no_hyperlinks = re.sub(r'https?:\/\/.*\/\w*', '', row_no_tickers)
    # Remove hashtags
    row_no_hashtags = re.sub(r'#\w*', '', row_no_hyperlinks)
    # Remove Punctuation and split 's, 't, 've with a space for filter
    row_no_punctuation = re.sub(r'[' + punctuation.replace('@', '') + ']+', ' ', row_no_hashtags)
    # Remove words with 2 or fewer letters (Also takes care of RT)
    row_no_small_words = re.sub(r'\b\w{1,2}\b', '', row_no_punctuation)  # \b represents a word boundary
    # Remove whitespace (including new line characters)
    row_no_whitespace = re.sub(r'\s\s+', ' ', row_no_small_words)

    row_no_whitespace = row_no_whitespace.lstrip(' ')  # Remove single space left on the left
    # Remove •
    row_no_ball = re.sub(r'•', ' ', row_no_whitespace)
    # Remove characters beyond Basic Multilingual Plane (BMP) of Unicode:
    row_no_emojis = ''.join(c for c in row_no_ball if
                            c <= '\uFFFF')  # Apart from emojis (plane 1), this also removes historic scripts and mathematical alphanumerics (also plane 1), ideographs (plane 2) and more.
    # Tokenize: Reduce length and remove handles
    tknzr = TweetTokenizer(preserve_case=True, reduce_len=True,
                           strip_handles=True)  # reduce_len changes, for example, waaaaaayyyy to waaayyy.
    tw_list = tknzr.tokenize(row_no_emojis)
    # Remove stopwords
    list_no_stopwords = [i for i in tw_list if i not in cache_english_stopwords]
    # Final filtered row
    row_filtered = ' '.join(list_no_stopwords)  # ''.join() would join without spaces between words.
    return row_filtered

df_merged1 = df_merged["text"].transform(lambda x: row_clean(x))

df_merged["text_cleaned"] = df_merged1

#Stemming
#Then we first tried steamming as the process of reducing inflection in words to their root forms such as
# mapping a group of words to the same stem even if the stem itself is not a valid word in the Language.

ps = PorterStemmer()

def stemm_row(row):
    words = word_tokenize(row)

    new_row = ""
    for w in words:
        new_row += ps.stem(w) + " "

    return new_row

df_merged2 = df_merged["text_cleaned"].transform(lambda x: stemm_row(x))
df_merged["Stemm"] = df_merged2

#Lemmatization
#We also tried lemmatization that reduces the inflected words properly ensuring that the root word belongs to
# the language, which is usually a better option, since it relies on correct language data (dictionaries)
# to identify a word with its lemma.

wordnet_lemmatizer = WordNetLemmatizer()


def lemm_row(row):
    sentence_words = nltk.word_tokenize(row)

    new_row = ""
    for word in sentence_words:
        new_row += wordnet_lemmatizer.lemmatize(word, pos="v") + " "

    return new_row

df_merged3 = df_merged["text_cleaned"].transform(lambda x: lemm_row(x))
df_merged["Text_Lemm"] = df_merged3
df_lemm_label = [df_merged["Text_Lemm"], df_merged["label"]]
df_lemm_label

df_new = pd.concat(df_lemm_label, axis=1)
df_merged.head()

#POS Tagging
#We created different columns for counting each part-of-speech and a column called "max_tag", which shows the
# most tagged part-of-speech for each file. By grouping by label with "max_tag" as a value, we discovered
# that the most tagged part_of-speech for REAL news is pronouns and for FAKE news is nouns. With this
# hypothesis in mind, we decided to do POS tagging for proper nouns to see if tagging them will help builidng
# a better model.

import en_core_web_sm
nlp = en_core_web_sm.load()

import spacy
nlp = spacy.load('en_core_web_sm')
df_merged["Text_Lemm_tags"] = df_merged["Text_Lemm"].apply(lambda x: nlp(x))

df_merged["Text_Lemm_tags_t"] =  df_merged["Text_Lemm_tags"].apply(lambda x: [pos.pos_ for pos in x])

df_merged["count_n"] = df_merged["Text_Lemm_tags_t"].apply(lambda x: x.count("NOUN")  )
df_merged["count_v"] = df_merged["Text_Lemm_tags_t"].apply(lambda x: x.count("VERB")  )
df_merged["count_adj"] = df_merged["Text_Lemm_tags_t"].apply(lambda x: x.count("ADJ")  )
df_merged["count_num"] = df_merged["Text_Lemm_tags_t"].apply(lambda x: x.count("NUM")  )
df_merged["count_ppn"] = df_merged["Text_Lemm_tags_t"].apply(lambda x: x.count("PROPN")  )

#type(df_merged["count_verb"].iloc[1])
df_merged['count_n']=df_merged['count_n'].astype('float64')
df_merged['count_v']=df_merged['count_v'].astype('float64')
df_merged['count_adj']=df_merged['count_adj'].astype('float64')
df_merged['count_num']=df_merged['count_num'].astype('float64')
df_merged['count_ppn']=df_merged['count_ppn'].astype('float64')

df_merged["max_tag"] = df_merged[['count_n','count_v', 'count_adj', 'count_num', 'count_ppn']].idxmax(axis=1)

df_merged.head()

df_merged.groupby("label")["max_tag"].count()

#Creates tokens and tags
def proper_nouns(row):
    aux = nltk.word_tokenize(row)
    #tokens_tags = nltk.pos_tag(aux)
    tokens_tags = [Word for Word,pos in nltk.pos_tag(aux) if pos == 'NNP']
    str1 = " "
    return str1.join(tokens_tags)

df_merged4 = df_merged["Text_Lemm"].transform(lambda x: proper_nouns(x))
df_merged["PPN"] = df_merged4
df_merged.head()

#For the visualization, we attempted to visualize the frequency of the most common words in each label.
# Although not successful, we wanted to show what our thought process was like by leaving the codes below.

def word_counter(aux):
    from collections import Counter
    split_it = aux.split()
    Counter = Counter(split_it)
    return Counter.most_common(1)[0]

from collections import Counter
df_merged["most_common_words"] = df_merged["Text_Lemm"].apply(lambda x: word_counter(x)[0])

df_p = df_merged.groupby("label")["most_common_words"].value_counts().groupby(level=[0,0]).nlargest(5)
df_p = pd.DataFrame(df_p)
df_p

#df_p = plot_df.to_frame()
df_p.rename(columns = {'most_common_words':'count'}, inplace = True)
df_p.shape

plt.figure(figsize=(15,5))
sns.countplot("count", data = df_p, order =pd.value_counts(df_p["count"]).iloc[:5].index)

# Split into train and test again
df_train = df_merged.iloc[0:3997]
df_test = df_merged.iloc[3997:6319]

###########################################################################
#Bag of Words
# Here, we tried both TF and TF-IDF for each feature. Initially we included TfidfTransformer() in the
# NLP pipeline; however, it gave us an error. So instead, we decided to use TfidVectorizer for TF-IDF instead.

# TF for Stemm
tf_weighting = CountVectorizer()
tf_news = tf_weighting.fit_transform(df_train["Stemm"])
#pd.DataFrame(tf_news.A, columns=tf_weighting.get_feature_names())
tf_news.shape

# TF for Lemm
tf_weighting = CountVectorizer()
tf_news_lemm = tf_weighting.fit_transform(df_train["Text_Lemm"])
#pd.DataFrame(tf_news_lemm.A, columns=tf_weighting.get_feature_names())
tf_news_lemm.shape

# TF for PPN
tf_weighting = CountVectorizer()
tf_news_ppn = tf_weighting.fit_transform(df_train["PPN"])
#pd.DataFrame(tf_news_ppn.A, columns=tf_weighting.get_feature_names())
tf_news_ppn.shape

# TF for text_cleaned
tf_weighting = CountVectorizer()
tf_news_clean = tf_weighting.fit_transform(df_train["text_cleaned"])
#pd.DataFrame(tf_news_clean.A, columns=tf_weighting.get_feature_names())
tf_news_clean.shape

# TfidTransformer did not work in the models, so instead we tried with TfidVectorizer.
tfidf_vect= TfidfVectorizer(stop_words='english', ngram_range=(1,2),token_pattern=r'\b[^\d\W]+\b')

tfidf_stemm = tfidf_vect.fit_transform(df_train['Stemm'])
tfidf_lemm = tfidf_vect.fit_transform(df_train['Text_Lemm'])
tfidf_ppn = tfidf_vect.fit_transform(df_train['PPN'])
tfidf_clean = tfidf_vect.fit_transform(df_train['text_cleaned'])

################################################################################################
# NLP Modelling
# In the modelling part, we built pipelines for and trained 8 models in total with different combinations
# of text cleaning, stemming, lemmatization, proper nouns, N-grams (uni and bi), TF, and TF-IDF to see which
# model works the best. Two classifiers were applied - Naive Bayes and SVM, and both were trained with
# GridSearchCV to find the optimal parameters as well.
##############################################################################################

#X_train, X_test, y_train, y_test = train_test_split(train['Text_Lemm'], train['label'], test_size=0.3,random_state=109)

# 1. Modelling with Text Cleaning, Stemming & TF-IDF

# train1 = df_train.drop(columns = ['ID','text', 'title', 'text_cleaned', 'Text_Lemm'])
# test1 = df_test.drop(columns = ['ID','text', 'title', 'text_cleaned', 'Text_Lemm'])
# clf_nb1 = MultinomialNB().fit(tfidf_stemm, train1['label'])

# NLP Pipeline
# text_clf_nb1 = Pipeline([('tfidf', tfidf_vect), ('clf', clf_nb1)])
# text_clf_nb1 = text_clf_nb1.fit(train1['Stemm'], train1['label'])

# Performance of NB Classifier
# predicted_nb = text_clf1.predict(train1["Stemm"])
# np.mean(predicted_nb == train1["label"])

### Performance of SVM Classifier
# from sklearn.linear_model import SGDClassifier
# text_clf_svm1 = Pipeline([('tfidf', tfidf_vect),
#                          ('clf-svm', SGDClassifier(loss='hinge', penalty='l2',alpha=1e-3, random_state=42))])
# text_clf_svm1 = text_clf_svm1.fit(train1["Stemm"], train1["label"])
# predicted_svm = text_clf_svm1.predict(train1["Stemm"])
# np.mean(predicted_svm == train1["label"])

### Performance of NB Classifier with GridSearchCV

# parameters = {'tfidf__use_idf': (True, False), 'clf__alpha': (1e-2, 1e-3)}
# gs_clf_nb1 = GridSearchCV(text_clf_nb1, parameters, n_jobs=-1)
# gs_clf_nb1 = gs_clf_nb1.fit(train1["Stemm"], train1["label"])
# predicted_nb_gs = gs_clf_nb1.predict(train1["Stemm"])
# np.mean(predicted_nb_gs == train1["label"])

### Performance of SVM Classificer with GridSearchCV
# parameters_svm = {'tfidf__use_idf': (True, False),'clf-svm__alpha': (1e-2, 1e-3)}
# gs_clf_svm1 = GridSearchCV(text_clf_svm1, parameters_svm, n_jobs=-1)
# gs_clf_svm1 = gs_clf_svm1.fit(train1["Stemm"], train1["label"])
# predicted_svm_gs = gs_clf_svm1.predict(train1["Stemm"])
# np.mean(predicted_svm_gs == train1["label"])

####### 1.1. Modelling with Text Cleaning, Stemming & TF¶
# clf_nb2 = MultinomialNB().fit(tf_news, train1['label'])

### NLP Pipeline
# text_clf_nb2 = Pipeline([('vect', CountVectorizer()), ('clf', clf_nb2)])
# text_clf_nb2 = text_clf_nb2.fit(train1['Stemm'], train1['label'])

### Performance of NB Classifier
# predicted_nb2 = text_clf_nb2.predict(train1["Stemm"])
# np.mean(predicted_nb2 == train1["label"])

### Performance of SVM Classifier
# from sklearn.linear_model import SGDClassifier
# text_clf_svm2 = Pipeline([('vect', CountVectorizer()),
#                          ('clf-svm', SGDClassifier(loss='hinge', penalty='l2',alpha=1e-3, random_state=42))])
# text_clf_svm2 = text_clf_svm2.fit(train1["Stemm"], train1["label"])
# predicted_svm2 = text_clf_svm2.predict(train1["Stemm"])
# np.mean(predicted_svm2 == train1["label"])

### Performance of NB Classifier with GridSearchCV
# parameters = {'vect__ngram_range': [(1, 1), (1, 2)], 'clf__alpha': (1e-2, 1e-3)}
# gs_clf_nb2 = GridSearchCV(text_clf_nb2, parameters, n_jobs=-1)
# gs_clf_nb2 = gs_clf_nb2.fit(train1["Stemm"], train1["label"])
# predicted_nb_gs2 = gs_clf_nb2.predict(train1["Stemm"])
# np.mean(predicted_nb_gs2 == train1["label"])

### Performance of SVM Classificer with GridSearchCV
# parameters_svm = {'vect__ngram_range': [(1, 1), (1, 2)], 'clf-svm__alpha': (1e-2, 1e-3)}
# gs_clf_svm2 = GridSearchCV(text_clf_svm2, parameters_svm, n_jobs=-1)
# gs_clf_svm2 = gs_clf_svm2.fit(train1["Stemm"], train1["label"])
# predicted_svm_gs2 = gs_clf_svm2.predict(train1["Stemm"])
# np.mean(predicted_svm_gs2 == train1["label"])

####################################################################################
#### BEST MODEL
### 2. Modelling with Text Cleaning, Lemmatization & TF-IDF

train2 = df_train.drop(columns = ['ID','text', 'title', 'text_cleaned', 'Stemm'])
test2 = df_test.drop(columns = ['ID','text', 'title', 'text_cleaned', 'Stemm'])

clf_nb3 = MultinomialNB().fit(tfidf_lemm, train2['label'])

### NLP Pipeline
text_clf_nb3 = Pipeline([('tfidf', tfidf_vect), ('clf', clf_nb3)])
text_clf_nb3 = text_clf_nb3.fit(train2['Text_Lemm'], train2['label'])

### Performance of NB Classifier
predicted_nb3 = text_clf_nb3.predict(train2["Text_Lemm"])
np.mean(predicted_nb3 == train2["label"])

### Performance of SVM Classifier
from sklearn.linear_model import SGDClassifier
text_clf_svm3 = Pipeline([('tfidf', tfidf_vect),
                         ('clf-svm', SGDClassifier(loss='hinge', penalty='l2',alpha=1e-3, random_state=42))])
text_clf_svm3 = text_clf_svm3.fit(train2["Text_Lemm"], train2["label"])
predicted_svm3 = text_clf_svm3.predict(train2["Text_Lemm"])
np.mean(predicted_svm3 == train2["label"])

### Performance of NB Classifier with GridSearchCV
parameters = {'tfidf__use_idf': (True, False), 'clf__alpha': (1e-2, 1e-3)}
gs_clf_nb3 = GridSearchCV(text_clf_nb3, parameters, n_jobs=-1)
gs_clf_nb3 = gs_clf_nb3.fit(train2["Text_Lemm"], train2["label"])
predicted_nb_gs3 = gs_clf_nb3.predict(train2["Text_Lemm"])
np.mean(predicted_nb_gs3 == train2["label"])

### Performance of SVM Classificer with GridSearchCV
# parameters_svm = {'tfidf__use_idf': (True, False),'clf-svm__alpha': (1e-2, 1e-3)}
# gs_clf_svm3 = GridSearchCV(text_clf_svm3, parameters_svm, n_jobs=-1)
# gs_clf_svm3 = gs_clf_svm3.fit(train2["Text_Lemm"], train2["label"])
# predicted_svm_gs3 = gs_clf_svm3.predict(train2["Text_Lemm"])
# np.mean(predicted_svm_gs3 == train2["label"])

###########################################################################
### 2.1. Modelling with Text Cleaning, Lemmatization & TF¶
# clf_nb4 = MultinomialNB().fit(tf_news_lemm, train2['label'])

### NLP Pipeline
# text_clf_nb4 = Pipeline([('vect', CountVectorizer()), ('clf', clf_nb4)])
# text_clf_nb4 = text_clf_nb4.fit(train2['Text_Lemm'], train2['label'])

### Performance of NB Classifier
# predicted_nb4 = text_clf_nb4.predict(train2["Text_Lemm"])
# np.mean(predicted_nb4 == train2["label"])

### Performance of SVM Classifier
# from sklearn.linear_model import SGDClassifier
# text_clf_svm4 = Pipeline([('vect', CountVectorizer()),
#                          ('clf-svm', SGDClassifier(loss='hinge', penalty='l2',alpha=1e-3, random_state=42))])
# text_clf_svm4 = text_clf_svm4.fit(train2["Text_Lemm"], train2["label"])
# predicted_svm4 = text_clf_svm4.predict(train2["Text_Lemm"])
# np.mean(predicted_svm4 == train2["label"])

### Performance of NB Classifier with GridSearchCV
# parameters = {'vect__ngram_range': [(1, 1), (1, 2)], 'clf__alpha': (1e-2, 1e-3)}
# gs_clf_nb4 = GridSearchCV(text_clf_nb4, parameters, n_jobs=-1)
# gs_clf_nb4 = gs_clf_nb4.fit(train2["Text_Lemm"], train2["label"])
# predicted_nb_gs4 = gs_clf_nb4.predict(train2["Text_Lemm"])
# np.mean(predicted_nb_gs4 == train2["label"])

### Performance of SVM Classificer with GridSearchCV
# parameters_svm = {'vect__ngram_range': [(1, 1), (1, 2)], 'clf-svm__alpha': (1e-2, 1e-3)}
# gs_clf_svm4 = GridSearchCV(text_clf_svm4, parameters_svm, n_jobs=-1)
# gs_clf_svm4 = gs_clf_svm4.fit(train2["Text_Lemm"], train2["label"])
# predicted_svm_gs4 = gs_clf_svm4.predict(train2["Text_Lemm"])
# np.mean(predicted_svm_gs4 == train2["label"])

########################################################################################
### 3. Modelling with Text Cleaning, PPN & TF-IDF¶
########################################################################################
# train3 = df_train.drop(columns = ['ID','text', 'title', 'text_cleaned', 'Stemm', 'Text_Lemm'])
# test3 = df_test.drop(columns = ['ID','text', 'title', 'text_cleaned', 'Stemm', 'Text_Lemm'])
# clf_nb5 = MultinomialNB().fit(tfidf_ppn, train3['label'])

### NLP Pipeline
# text_clf_nb5 = Pipeline([('tfidf', tfidf_vect), ('clf', clf_nb5)])
# text_clf_nb5 = text_clf_nb5.fit(train3['PPN'], train3['label'])

### Performance of NB Classifier
# predicted_nb5 = text_clf_nb5.predict(train3["PPN"])
# np.mean(predicted_nb5 == train3["label"])

### Performance of SVM Classifier
# from sklearn.linear_model import SGDClassifier
# text_clf_svm5 = Pipeline([(('tfidf', tfidf_vect)),
#                          ('clf-svm', SGDClassifier(loss='hinge', penalty='l2',alpha=1e-3, random_state=42))])
# text_clf_svm5 = text_clf_svm5.fit(train3["PPN"], train3["label"])
# predicted_svm5 = text_clf_svm5.predict(train3["PPN"])
# np.mean(predicted_svm5 == train3["label"])

### Performance of NB Classifier with GridSearchCV
# parameters = {'tfidf__use_idf': (True, False), 'clf__alpha': (1e-2, 1e-3)}
# gs_clf_nb5 = GridSearchCV(text_clf_nb5, parameters, n_jobs=-1)
# gs_clf_nb5 = gs_clf_nb5.fit(train3["PPN"], train3["label"])
# predicted_nb_gs5 = gs_clf_nb5.predict(train3["PPN"])
# np.mean(predicted_nb_gs5 == train3["label"])

### Performance of SVM Classificer with GridSearchCV
# parameters_svm = {'tfidf__use_idf': (True, False), 'clf-svm__alpha': (1e-2, 1e-3)}
# gs_clf_svm5 = GridSearchCV(text_clf_svm5, parameters_svm, n_jobs=-1)
# gs_clf_svm5 = gs_clf_svm5.fit(train3["PPN"], train3["label"])
# predicted_svm_gs5 = gs_clf_svm5.predict(train3["PPN"])
# np.mean(predicted_svm_gs5 == train3["label"])

########################################################################################
### 3.1. Modelling with Text Cleaning, Proper Nouns, TF
########################################################################################
# clf_nb6 = MultinomialNB().fit(tf_news_ppn, train3['label'])

### NLP Pipeline
# text_clf_nb6 = Pipeline([('vect', CountVectorizer()), ('clf', clf_nb6)])
# text_clf_nb6 = text_clf_nb6.fit(train3['PPN'], train3['label'])

### Performance of NB Classifier
# predicted_nb6 = text_clf_nb6.predict(train3["PPN"])
# np.mean(predicted_nb6 == train3["label"])

### Performance of SVM Classifier
# from sklearn.linear_model import SGDClassifier
# text_clf_svm6 = Pipeline([('vect', CountVectorizer()),
#                          ('clf-svm', SGDClassifier(loss='hinge', penalty='l2',alpha=1e-3, random_state=42))])
# text_clf_svm6 = text_clf_svm6.fit(train3["PPN"], train3["label"])
# predicted_svm6 = text_clf_svm6.predict(train3["PPN"])
# np.mean(predicted_svm6 == train3["label"])

### Performance of NB Classifier with GridSearchCV
# parameters = {'vect__ngram_range': [(1, 1), (1, 2)], 'clf__alpha': (1e-2, 1e-3)}
# gs_clf_nb6 = GridSearchCV(text_clf_nb6, parameters, n_jobs=-1)
# gs_clf_nb6 = gs_clf_nb6.fit(train3["PPN"], train3["label"])
# predicted_nb_gs6 = gs_clf_nb6.predict(train3["PPN"])
# np.mean(predicted_nb_gs6 == train3["label"])

### Performance of SVM Classificer with GridSearchCV
# parameters_svm = {'vect__ngram_range': [(1, 1), (1, 2)], 'clf-svm__alpha': (1e-2, 1e-3)}
# gs_clf_svm6 = GridSearchCV(text_clf_svm6, parameters_svm, n_jobs=-1)
# gs_clf_svm6 = gs_clf_svm6.fit(train3["PPN"], train3["label"])
# predicted_svm_gs6 = gs_clf_svm6.predict(train3["PPN"])
# np.mean(predicted_svm_gs6 == train3["label"])

#####################################################################################
### 4. Modelling with Text Cleaning & TF-IDF only¶
#########################################################################################
# train4 = df_train.drop(columns = ['ID','text', 'title', 'Stemm', 'Text_Lemm', 'PPN'])
# test4 = df_test.drop(columns = ['ID','text', 'title', 'Stemm', 'Text_Lemm', 'PPN'])
# clf_nb7 = MultinomialNB().fit(tfidf_clean, train4['label'])

### NLP Pipeline
# text_clf_nb7 = Pipeline([('tfidf', tfidf_vect), ('clf', clf_nb7)])
# text_clf_nb7 = text_clf_nb7.fit(train4['text_cleaned'], train4['label'])

### Performance of NB Classifier
# predicted_nb7 = text_clf_nb7.predict(train4["text_cleaned"])
# np.mean(predicted_nb7 == train4["label"])

### Performance of SVM Classifier
# from sklearn.linear_model import SGDClassifier
# text_clf_svm7 = Pipeline([(('tfidf', tfidf_vect)),
#                          ('clf-svm', SGDClassifier(loss='hinge', penalty='l2',alpha=1e-3, random_state=42))])
# text_clf_svm7 = text_clf_svm7.fit(train4["text_cleaned"], train4["label"])
# predicted_svm7 = text_clf_svm7.predict(train4["text_cleaned"])
# np.mean(predicted_svm7 == train4["label"])

### Performance of NB Classifier with GridSearchCV
# parameters = {'tfidf__use_idf': (True, False), 'clf__alpha': (1e-2, 1e-3)}
# gs_clf_nb7 = GridSearchCV(text_clf_nb7, parameters, n_jobs=-1)
# gs_clf_nb7 = gs_clf_nb7.fit(train4["text_cleaned"], train4["label"])
# predicted_nb_gs7 = gs_clf_nb7.predict(train4["text_cleaned"])
# np.mean(predicted_nb_gs7 == train4["label"])

### Performance of SVM Classificer with GridSearchCV
# parameters_svm = {'tfidf__use_idf': (True, False), 'clf-svm__alpha': (1e-2, 1e-3)}
# gs_clf_svm7 = GridSearchCV(text_clf_svm7, parameters_svm, n_jobs=-1)
# gs_clf_svm7 = gs_clf_svm7.fit(train4["text_cleaned"], train4["label"])
# predicted_svm_gs7 = gs_clf_svm7.predict(train4["text_cleaned"])
# np.mean(predicted_svm_gs7 == train4["label"])

##############################################################################################
### 4.1. Modelling with Text Cleaning & TF only
##############################################################################################
# clf_nb8 = MultinomialNB().fit(tf_news_clean, train4['label'])

### NLP Pipeline
# text_clf_nb8 = Pipeline([('vect', CountVectorizer()), ('clf', clf_nb8)])
# text_clf_nb8 = text_clf_nb8.fit(train4['text_cleaned'], train4['label'])

### Performance of NB Classifier
# predicted_nb8 = text_clf_nb8.predict(train4["text_cleaned"])
# np.mean(predicted_nb8 == train4["label"])

### Performance of SVM Classifier
# from sklearn.linear_model import SGDClassifier
# text_clf_svm8 = Pipeline([('vect', CountVectorizer()),
#                          ('clf-svm', SGDClassifier(loss='hinge', penalty='l2',alpha=1e-3, random_state=42))])
# text_clf_svm8 = text_clf_svm8.fit(train4["text_cleaned"], train4["label"])
# predicted_svm8 = text_clf_svm8.predict(train4["text_cleaned"])
# np.mean(predicted_svm8 == train4["label"])

### Performance of NB Classifier with GridSearchCV
# parameters = {'vect__ngram_range': [(1, 1), (1, 2)], 'clf__alpha': (1e-2, 1e-3)}
# gs_clf_nb8 = GridSearchCV(text_clf_nb8, parameters, n_jobs=-1)
# gs_clf_nb8 = gs_clf_nb8.fit(train4["text_cleaned"], train4["label"])
# predicted_nb_gs8 = gs_clf_nb8.predict(train4["text_cleaned"])
# np.mean(predicted_nb_gs8 == train4["label"])

### Performance of SVM Classificer with GridSearchCV
# parameters_svm = {'vect__ngram_range': [(1, 1), (1, 2)], 'clf-svm__alpha': (1e-2, 1e-3)}
# gs_clf_svm8 = GridSearchCV(text_clf_svm8, parameters_svm, n_jobs=-1)
# gs_clf_svm8 = gs_clf_svm8.fit(train4["text_cleaned"], train4["label"])
# predicted_svm_gs8 = gs_clf_svm8.predict(train4["text_cleaned"])
# np.mean(predicted_svm_gs8 == train4["label"])

### Surprisingly, although the difference was miniscule, TF had a better performance than TF-IDF in general.
# we hypothesize that this may be that words that appear in multiple documents are helpful in distinguishing
# between classes. For example, function words like pronouns are very common in documents such as news and
# would be downweighted in tf-idf, but given equal weight to rare words in countvectorizer( = TF).

############################################################################
### Cross Validation
#After the modelling, there were 4 models with the exact same accuracy (0.994996247185389) of the train set.
# Although the models were tested on the train set due to target variable being not available in the test set,
# 99.9% accuracy seems very overfitting. Therefore, in order to assess the effectivenss of our models and
# overfitting problem, we ran cross validation for those 4 models and an additional model (text_clf_svm8),
# because it has the highest accuracy score without GridSearchCV.
###############################################################################

### 1. CV Score for Text Cleaning + TF Model with SVM Classifier

## print(cross_val_score(text_clf_svm8, train4['text_cleaned'], train4['label'], cv=5))
# import numpy as np
# print(np.mean(cross_val_score(text_clf_svm8, train4['text_cleaned'], train4['label'], cv=5)))

### 2. CV Score for Lemmatization + TF-IDF Model with NB Classifier with GridSearchCV
print(cross_val_score(gs_clf_nb3, train2['Text_Lemm'], train2['label'], cv=5))
import numpy as np
print(np.mean(cross_val_score(gs_clf_nb3, train2['Text_Lemm'], train2['label'], cv=5)))

### 3. CV Score for Text Cleaning + TF-IDF Model with NB Classifier with GridSearchCV
# # print(cross_val_score(gs_clf_nb7, train4['text_cleaned'], train4['label'], cv=5))
# import numpy as np
# print(np.mean(cross_val_score(gs_clf_nb7, train4['text_cleaned'], train4['label'], cv=5)))

### 4. CV Score for Stemming + TF-IDF Model with NB Classifier with GridSearchCV
# print(cross_val_score(gs_clf_nb1, train1['Stemm'], train1['label'], cv=5))
# import numpy as np
# print(np.mean(cross_val_score(gs_clf_nb1, train1['Stemm'], train1['label'], cv=5)))

### 5. CV Score for Lemmatization + TF Model with NB Classifier with GridSearchCV
# print(cross_val_score(gs_clf_nb4, train2['Text_Lemm'], train2['label'], cv=5))
# import numpy as np
# print(np.mean(cross_val_score(gs_clf_nb4, train2['Text_Lemm'], train2['label'], cv=5)))

###########################################################################################
### Model Selection
# After the cross validation, the best model was model #2, which was trained with lemmatization and
# TF-IDF applying the NB classifier with GridSearchCV, with a score of 90.39%. This final model was used
# to make predictions on the test set below.
##########################################################################################
final_pred = gs_clf_nb3.predict(df_test["Text_Lemm"])
df_predictions = pd.DataFrame(final_pred)
df_predictions.columns = ['label']
df_predictions.head()
df_predictions.to_csv('final_pred.csv')
