from itertools import combinations, product
from pyspark.sql import DataFrame
import nltk
from nltk.stem import WordNetLemmatizer
from nltk.tokenize import word_tokenize
import string
import pandas as pd
nltk.download('punkt')
nltk.download('stopwords')
nltk.download('wordnet')

class Preprocess:
  """
  Class that performs the pre-processing of textual data in order to perform the Apriori algorithm.
  ...
  Methods
  -------
  create_vocab(rdd)
      from a spark rdd of sentences, creates the vocabulary that maps words to numbers and numbers to words
  encoder(rdd)
      encodes words to numbers
  preprocess(sentence)
      preprocesses the sentence, which is the atomic unit of my textual data
  """
  def create_vocab(self, rdd):
    """
    Method that creates the vocabulary that maps words to numbers and numbers to words
    ...
    Parameters
    ----------
    rdd: spark rdd 
         rdd of preprocessed sentences
    """
    #find unique words
    input_words = rdd.flatMap(lambda line:line)\
                    .distinct().collect()
    #create vocabulary that maps words to numbers
    word2num = {word:i for i, word in enumerate(input_words)}
    #create vocabulary that maps numbers to words
    num2word = {key:word for word,key in word2num.items()}

    return word2num, num2word
  
  def encoder(self, rdd):
    """
    Method that encodes words to numbers and returns a new rdd with the encoded sentences
    ...
    Parameters
    ----------
    rdd: spark rdd
         rdd of preprocessed sentences
    """
    vocab1, vocab2 = self.create_vocab(rdd)
    new_rdd = rdd.map(lambda sentence: [vocab1[word] for word in sentence])
    return new_rdd, vocab2
  
  
  @staticmethod
  def preprocess(sentence):
    """
    Method that preprocesses the sentence, which is the atomic unit of my textual data
    ...
    Parameters
    ----------
    sentence: string
         sentence to be processed
    """
    #remove punctuation
    sentence = sentence.translate(str.maketrans('', '', string.punctuation))

    #tokenize sentence
    words = word_tokenize(sentence.lower())
    from nltk.corpus import stopwords
    #define list of stop words
    stop_words = set(stopwords.words('english'))

    #remove stop words
    clean_words = [word for word in words if word not in stop_words]
    #sort out similar words
    #define lemmatizer object
    lemmatizer = WordNetLemmatizer()
    def lemm(word):
        return lemmatizer.lemmatize(word)

    final_words = map(lemm,clean_words)


    return list(set(final_words))
  

class apriori:
  """
  Class that performs the Apriori algorithm and generates association rules
  ...
  Parameters
  ----------
  minSupportpercent: float
         minimum support percent(default 0.1)
  total_transactions: int
	 total count of records in data
  maximumBasketSize: int
         maximum basket size
  min_confidence: float
         minimum confidence
  vocab: dictionary
              dict that maps numbers to words
  Methods
  -------
  Apriori(supportRdd, rdd)
      performs the Apriori algorithm
  getCombinations(previousfrequents,k)
      creates all possible combinations of size k 
  getSupport(results)
      gets the support from the result of the Apriori algorithm
  generate_association_rules(results, to_decode)
      generates the association rules which are support and confidence
  """
  def __init__(self, minSupportpercent=0.1,total_transactions=0, maximumBasketSize=3, min_confidence=0.5,vocab=None):
        self.minSupportpercent = minSupportpercent
        self.total_transactions = total_transactions
        self.maximumBasketSize = maximumBasketSize
        self.min_confidence = min_confidence
        self.vocab = vocab
        self.minSupport = self.total_transactions*self.minSupportpercent
  
  
  def getCombinations(self, previousfrequents=None ,k=3):
        """
        Method that creates all possible combinations of size k and returns a list of candidates of size k
        ...
        Parameters
        ----------
        previousfrequents: spark rdd
             rdd of frequent items of the previous iteration
        k: int
             size of candidate list

        """
        #[((1,2),3),...]--->[((1,2)),...]
        previousfrequents = previousfrequents.map(lambda x: list(x[0]))

        singleton = previousfrequents.flatMap(list)\
                                    .map(lambda item:(item,1))\
                                    .reduceByKey(lambda x,y: x+y)\
                                    .map(lambda item:[item[0]])
        candidates = []

        for combination in list(product(previousfrequents.toLocalIterator(),singleton.toLocalIterator())):
            #[(item,singleton),...]
            combination = combination[0]+combination[1]

            combination_set = sorted(set().union(combination))

            if(len(combination_set)==k) and combination_set not in candidates:
                candidates.append(combination_set)

        return candidates
  def Apriori(self, support, rdd):
          """
          Method that performs the Apriori algorithm and returns the frequent items rdd of size maximumBasketSize
          ...
          Parameters
          ----------
          support: spark rdd
              empty spark rdd
          rdd: spark rdd
              rdd of preprocessed sentences
          """
          #cloning rdd
          allItemsRdd = rdd.map(lambda line:line)

          print('---set up complete---')
          
          #trovo la frequenza dei singletones
          supportRdd = rdd.flatMap(list)\
                          .map(lambda word: (word,1))\
                          .reduceByKey(lambda y,x: x+y)\
                          .filter(lambda t: t[1]>= self.minSupport)

          #add frequent singletones
          support = support.union(supportRdd)

          singleton = supportRdd.map(lambda x: (x[0]))
          print('---singletones found!---')

          #find frequent baskets for k=2
          candidates = list(combinations(singleton.toLocalIterator(),2))

          print('---first candidates found!---')


          #number of items per basket
          k=3

          while (len(candidates)>0):


              print(f'starting {k-1} items in basket loop')
              minSupport = self.minSupport/(k+1)
              #filtering phase to select real frequent items
              combined_k = allItemsRdd.flatMap(lambda sentence: [(tuple(candidate),1) for candidate in candidates if set(list(candidate)).issubset(set(sentence))])\
                                      .reduceByKey(lambda y,x:x+y)\
                                      .filter(lambda item : item[1]>= minSupport)

              support = support.union(combined_k)
              print(f'added frequent baskets with {k-1} items')

              if k == (self.maximumBasketSize + 1):
                  break
              #compute candidates for next iteration
              print(f'computing candidates for next iteration')
              candidates = self.getCombinations(combined_k,k)

              print(f'found candidates for {k} items in basket')
              k+=1


          return support
      
  def getSupport(self, results):
          """
          Method that gets the support from the result of the Apriori algorithm
          ...
          Parameters
          ----------
          results: spark rdd
              rdd of frequent items of maximumBasketSize
          
          
          """
          
          support_results = []
          for itemset, count in results:

              support = count / self.total_transactions
              support_results.append((itemset, support))

          return support_results

  def generate_association_rules(self, results, to_decode=False):
          """
          Method that generates the association rules which are support, confidence and interest as a pandas dataframe.
          ...
          Parameters
          ----------
          freq_results: list
              list of frequent items
          to_decode: boolean
              True if the results are to be decoded
          
          """
          if to_decode:
              results_decoded = [(self.vocab[t[0]],t[1]) if type(t[0])==int else (tuple([self.vocab[element] for element in t[0]]),t[1]) for t in results]
          else:
              results_decoded = results
	        #get results with support/total_transactions
          results = self.getSupport(results_decoded)
          #dictionary of all possible frequent itemsets
          dictio = {t[0]:t[1] for t in results}

	        #define sizes to be considered by the algorithm
          sizes = [size for size in range(2,self.maximumBasketSize+1)]

          association_rules = set()
    
          for itemset, support in results:
          #consider only itemsets > 1
              if len(itemset) in sizes :
                  for i in range(1, len(itemset)):
                    for comb in combinations(itemset,i):
                      ant = tuple(sorted(comb))
                      post = tuple(sorted(set(itemset)-set(ant)))
                      if len(ant)==1 :
                        confidence = dictio.get(tuple(itemset), 0) / dictio.get(ant[0],1)
            
                      else:
                        confidence = dictio.get(tuple(itemset), 0) / dictio.get(ant,1)
                        rapporto = dictio.get(post, 0)
                        interest = confidence - rapporto
                      if len(post)==1:
                        rapporto = dictio.get(post[0], 1)
                        interest = confidence - rapporto
                      else:
                        rapporto = dictio.get(post, 0)
                        interest = confidence - rapporto

                      if confidence>=0.5 and confidence!=interest:
                        association_rules.add((ant, post, support, confidence,interest))
          association_rules = list(association_rules)
	        #order the rules by descending confidence
          sorted_rules = sorted(association_rules, key=lambda rule: rule[3], reverse=True)

          columns = ['antecedent', 'consequent', 'support', 'confidence', 'interest']

          # Create the DataFrame
          df = pd.DataFrame(sorted_rules, columns=columns)

          # Print the DataFrame
          df = df.drop_duplicates(['support'],keep= 'first')
          
          return df
