import random
import json
import pickle
import numpy as np
#pip install -U tensorflow
import nltk
from nltk.stem import WordNetLemmatizer
import os
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'

from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Dropout, Activation

from tensorflow.keras.optimizers import SGD



lemmatizer = WordNetLemmatizer()
# used to to reduce words to their stems

intents = json.loads(open('intents.json').read())
#load our dictionary of static responses from JSON to python

ignore_list = ['!', '?', '.', ',']
#letters which will not be taken into account
words = []
documents = []
#will store both the words and the intent
classes = []
#details the class of the particular intent, e.g 'greetings'



for intent in intents['intents']:
    #iterating over our 'intents' dictionary in JSON
    for pattern in intent['patterns']:
    #looks in the 'patterns' list, within intents
        word_list = nltk.word_tokenize(pattern)
        words.extend(word_list)
        # turns the words in a text into individual tokens and
        # appends them to our words list
        documents.append((word_list, intent['tag']))
        #allows us to know the tag of the words appended to our wordlist
        if intent['tag'] not in classes:
            classes.append(intent['tag'])



words = [lemmatizer.lemmatize(word) for word in words if word not in ignore_list]
words = sorted(set(words))
#returns a list of lemmatized words, removing duplicates
classes = sorted(set(classes))

pickle.dump(words, open('words.pkl', 'wb'))
pickle.dump(classes, open('classes.pkl', 'wb'))
#saving these lemmatized words into a file

#training our neural network
training = []
output_empty = [0] * len(classes)
#a template of 0's, we need as many 0s as classes

for document in documents:
    bag = []
    #for every combination of word and class, we need an empty bag of words
    word_patterns = document[0]
    word_patterns = [lemmatizer.lemmatize(word.lower()) for word in word_patterns]
    for word in words:
        bag.append(1) if word in word_patterns else bag.append(0)

    output_row = list(output_empty)
    output_row[classes.index(document[1])] = 1
    training.append([bag, output_row])

random.shuffle(training)
training = np.array(training, dtype=object)

train_x = list(training[:, 0])
train_y = list(training[:, 1])


#BUILDING NEURAL NETWORK
model = Sequential()
model.add(Dense(128, input_shape=(len(train_x[0]),), activation='relu'))
#densely-connected neural network layer with 128 neurons
model.add(Dropout(0.5))
model.add(Dense(64, activation='relu'))
model.add(Dropout(0.5))
model.add(Dense(len(train_y[0]), activation='softmax'))
#we want as many neurons as there are training data inputs
#softmax scales the results of the output to 1, leaving us with a percentage of likelihood of a particular output

sgd = SGD(learning_rate=0.01, decay=1e-6, momentum=0.9, nesterov=True)
model.compile(loss='categorical_crossentropy', optimizer=sgd, metrics=['accuracy'])

var = model.fit(np.array(train_x), np.array(train_y), epochs=200, batch_size=5, verbose=1)
#we feed the same data to the neural network 200 times with a batch size of 5
model.save('chatbotmodel.h5',var)
print("Done")