import json
import re
import sys
import random
import pandas as pd
import nltk
from nltk.corpus import stopwords
from sklearn.naive_bayes import ComplementNB
from sklearn.pipeline import Pipeline
from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer
import spacy
nlp = spacy.load('en_core_web_sm')
stopwords = set(stopwords.words('english'))






answers = {
    'greetings' : 'hi!',
    'goodbye': 'happy to be of service!',
    'origin': 'I was created by Mitchel, Ben and Fergus!',
    'go-from': 'you want to go from <__GPE__>?',
    'go-to':  'you want to go to <__GPE__>?'
}


training_data = json.load(open('./training_data.json', 'r'))
print(list(training_data.keys()))

punct_re_escape = re.compile('[%s]' % re.escape('!"#$%&()*+,./:;<=>?@[\\]^_`{|}~'))
# this is a constructor that allows us to put our chatbout data (intents, phrases + responses) into a neat dataframe

class ChatbotData:
    def __init__(self, json_object, text, answers):
        dataframes = []
        for i, (intent, data) in enumerate(json_object.items()):
            # we index every key in the dictionary, and assign it to patterns
            patterns = data[text].copy()
            # we now make it lowercase and remove punctuation
            for i, p in enumerate(patterns):
                p = p.lower()
                p = self.remove_punctuation(p)
                patterns[i] = p
                answer = answers[intent]
            df = pd.DataFrame(list(zip([intent]*len(patterns), patterns, [answer]*len(patterns))), \
                            columns=['intent', 'phrase', 'answer'])
            dataframes.append(df)
            self.df = pd.concat(dataframes)

    def remove_punctuation(self,text):
        return punct_re_escape.sub('',text)

    def show_batch(self, size=5):
        return self.df.head(size)

    def get_intents(self):
        return list(pd.unique(self.df['intent']))

    def get_phrases(self, intent):
        return list(self.df[self.df['intent'] == intent]['phrase'])

    def get_answer(self, intent):
        return list(self.df[self.df['intent'] == intent]['answer'])[0]

chatbot_data = ChatbotData(training_data, 'patterns', answers)
print(chatbot_data.show_batch(10))

#this removes the stopwords from the input, tokenizes, and rejoins
def tokenize(text):
    doc = nlp(text.lower())
    return " ".join(token.text for token in doc if token.text.strip() not in stopwords)

#simply retrieving x and y from df; x the value we feed, y being the value we predict (intent patterns and answers)
def get_x_y(training_data):
    x, y = [], []
    intents = chatbot_data.get_intents()
    for i in intents:
        phrases = chatbot_data.get_phrases(i)
        x += [tokenize(phrase) for phrase in phrases]
        y += [i]*len(phrases)

    return x, y


def train(x,y):
    #turns everything to an id and counts frequency of word appearances
    vector = CountVectorizer(ngram_range=(1,2),max_features=None)
    nb = Pipeline([('vector',vector),('clf', ComplementNB(alpha=1.0, norm=False))])
    nb.fit(x,y)
    return nb



x, y = get_x_y(training_data)
nb_model = train(x, y)

#using tokenized query without stopwords, we predict the label with sklearn
def nb_predict(query):
    tokenized_query = tokenize(query)
    predct = nb_model.predict([tokenized_query])[0]
    return chatbot_data.get_answer(predct)



MODEL_NAME = 'en_core_web_sm'

class SpacyModel(object):
    #this is where we will keep our loaded model
    spacy_model = None

    #defining a method for the class
    @classmethod
    def get_spacy_model(cls):
        #getting the base model for entity recog
        if not cls.spacy_model:
            cls.spacy_model = spacy.load(MODEL_NAME)
        return cls.spacy_model

    @classmethod
    def replace_entities(cls, text):
        #method to replace named entities with the appropriate label
        spacy_model = cls.get_spacy_model()
        doc = spacy_model(text)
        entity_replaced_text = text
        #loopings through entities in doc
        for e in reversed(doc.ents):
            start = e.start_char
            end = start + len(e.text)
            entity_replaced_text = entity_replaced_text[:start] + f'<__{e.label_}__>' + entity_replaced_text[end:]

        return e

def get_response(intents_list, intents_json):
    tag = intents_list[0]['intent']
    list_of_intents = intents_json['intents']
    for i in list_of_intents:
        if i['tag'] == tag:
            result = random.choice(i['responses'])
            break
    return result

print(SpacyModel.replace_entities("can i go from london?"))

print("Chatbot Online!")

while True:
    message = input("")
    spacy_model = spacy.load('en_core_web_sm')
    if message in spacy_model:
        location_str = SpacyModel.replace_entities(message)
        #we swap the tag for the actual location in the predicted message
        string_to_swap = nb_predict(message)
        string_wo_tag = string_to_swap.rsplit('',1)[0]
        appended_string = ''.join(string_wo_tag,location_str)
        print(appended_string)
    else:
        print(nb_predict(message))

        #had issue with the PANDAS dataframe I created, for some reason, it would not add every item in my json file.



