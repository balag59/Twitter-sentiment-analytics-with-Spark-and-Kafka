from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import operator
import numpy as np
import matplotlib.pyplot as plt


def main():
    conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, 10)   # Create a streaming context with batch interval of 10 sec
    ssc.checkpoint("checkpoint")

    pwords = load_wordlist("positive.txt")
    nwords = load_wordlist("negative.txt")




    counts = stream(ssc, pwords, nwords, 100)
    #stream(ssc, pwords, nwords, 100)
    make_plot(counts)


def make_plot(counts):
    """
    Plot the counts for the positive and negative words for each timestep.
    Use plt.show() so that the plot will popup.
    """
    # YOUR CODE HERE
    plt.xlabel('Time step')
    plt.ylabel('Word count')
    xpos_values = [x[0][1] for x in counts[1:]]
    xneg_values = [x[1][1] for x in counts[1:]]
    positive, = plt.plot(range(11),xpos_values,'bo-',label = 'positive')
    negative, = plt.plot(range(11),xneg_values,'go-',label = 'negative')
    plt.legend(handles=[positive, negative])
    plt.show()




def load_wordlist(filename):
    """
    This function should return a list or set of words from the given filename.
    """
    # YOUR CODE HERE
    f_read = open(filename,'r')
    word_list = []
    for word in f_read:
        word_list.append(word.strip())
    return word_list


def stream(ssc, pwords, nwords, duration):
    kstream = KafkaUtils.createDirectStream(
        ssc, topics = ['twitterstream'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})
    tweets = kstream.map(lambda x: x[1].encode("ascii","ignore"))


    # Each element of tweets will be the text of a tweet.
    # You need to find the count of all the positive and negative words in these tweets.
    # Keep track of a running total counts and print this at every time step (use the pprint function).
    # YOUR CODE HERE
    words = tweets.flatMap(lambda tweet: tweet.split(" "))
    #pairs = words.map(lambda word: ('positive', 1) if(word in pwords) else ('negative', 1) if(word in nwords) else None)
    pos_pairs = (words.filter(lambda word: word in pwords)).map(lambda word: ('positive',1))
    neg_pairs = (words.filter(lambda word: word in nwords)).map(lambda word: ('negative',1))
    pairs = pos_pairs.union(neg_pairs)
    count = pairs.reduceByKey(lambda x, y: x + y)
    #count.pprint()

    def updateFunction(newValues, runningCount):
     if runningCount is None:
        runningCount = 0
     return sum(newValues, runningCount)

    runningCounts = pairs.updateStateByKey(updateFunction)
    runningCounts.pprint()



    # Let the counts variable hold the word counts for all time steps
    # You will need to use the foreachRDD function.
    # For our implementation, counts looked like:
    #   [[("positive", 100), ("negative", 50)], [("positive", 80), ("negative", 60)], ...]
    counts = []
    # YOURDSTREAMOBJECT.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))
    #pairs.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))
    count.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))
    ssc.start()                         # Start the computation
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop(stopGraceFully=True)

    return counts


if __name__=="__main__":
    main()
