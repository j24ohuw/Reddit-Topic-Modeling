import pymongo
import pandas as pd
import re
from pymongo import MongoClient
from nltk.corpus import stopwords
from nltk import word_tokenize
from gensim import corpora

import pickle

client = MongoClient()
db = client.redditCrawler
collection = db.data_test1

def remove_posts(data, index_list):
    data = data.drop(index_list)
    return data.reset_index(drop=True)

data = pd.DataFrame(list(collection.find()))
mod_posts = [i for i in range(len(data)) if 'moronic Monday' in data['title'][i]]

#remove all the mod posts that include 'moronic Monday'
data = remove_posts(data, mod_posts)
titles = data['title']
content = data['post']
comments = data['comments']

# collect only the comments without vote scores, dates, etc
comments_in_thread = []
for index, thread in enumerate(comments):
    aggregate = []
    for comment in thread:
        if type(comment['comment_reply']) == str:
            aggregate.append(comment['comment_reply'].lower())
    comments_in_thread.append(aggregate)

comments = comments_in_thread
#number of titles and post need to be the same
assert len(titles) == len(content) 
assert len(comments) == len(content)


#preprocess
stop_words = stopwords.words('english')
stop_words.extend(['would',
                   'people',
                   'money',
                   'think',
                   'thinks',
                   'thanks',
                   'thing',
                   'things',
                   'ok',
                   'nt',
                   'actually',
                   'like',
                   'get',
                   'even',
                   'could',
                   'also',
                   ])

#Function to clean off each dataset item; stop words (what, if, is, where, how, I, she)

def preprocess(text):
    #no content/nan/len of 0
    #text = [re.sub('[^a-zA-Z0-9]+', ' ', word) for word in text]
    text = text.lower()
    text = text.replace('$', ' ')
##    text = text.replace('-', ' ')
##    text = text.replace("/", ' ')
    text = word_tokenize(text)
##    text = [re.sub('[^a-zA-Z0-9]+', '', word) for word in text]
    text = [word for word in text if word not in stop_words] 
    text = [word for word in text if word.isalpha()]
    return text

#pass titles and comments through pre-processor
titles = [preprocess(title) for title in titles]
posts = [preprocess(text) for text in content]

# process comments
##comments = [[preprocess(comment) for comment in thread] for thread in comments]
temp = []
for i, thread in enumerate(comments):
    temp_thread = []
    temp_thread.extend(titles[i])
    for comment in thread:
        temp_thread.extend(preprocess(comment))
    temp.append(temp_thread)

comments = temp

# form a list of dictionaries for each title, compile
# each word and its corresponding frequencies in the post's comment section
list_of_dict = []
for index, title in enumerate(titles):
##    text = ''
    bag_of_words = set(title)
##    text = ' '.join(comments_in_thread[index])
    text = comments[index]
    dictionary = {word:text.count(word) for word in bag_of_words if text.count(word) > 0}
    list_of_dict.append(dictionary)

title_keywords = [list(Dict.keys()) if len(Dict) > 0 else [0] for Dict in list_of_dict]
title_keywords = [word for sublist in title_keywords for word in sublist if word != 0 ]
title_keywords = set(title_keywords)
##title_keywords = set(title_keywords)

##count the number of keywords in the comment section
def count_keywords(comments, keywords):
##    sample = ' '.join(comments).split()
    return {word: comments.count(word) for word in keywords if comments.count(word) > 0}

keyword_dict = [count_keywords(comment, title_keywords) for comment in comments]
for index, thread in enumerate(keyword_dict):
    #normalize each keyword by the number of words present
    df = pd.DataFrame()
    df['word'] = thread.keys()
    df['count'] = thread.values()
    df = df.sort_values('count', ascending = False)
    #dividing by number of words in each thread
##    df['frequency'] = df['count']/(len(comments[index]))
    df['frequency'] = df['count']/(1+len(comments_in_thread[index]))
    df['count'] = df['count']/(len(comments[index]))**0.5
    keyword_dict[index] = df.reset_index(drop=True)

#save varialbes
variables = [data['title'], titles, posts, comments, comments_in_thread,
             list_of_dict, title_keywords, keyword_dict]

with open('variables.txt', 'wb') as fp:
    pickle.dump(variables, fp)





