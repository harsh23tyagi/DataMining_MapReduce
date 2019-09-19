from pyspark.context import SparkContext, SparkConf
import json
import sys
from collections import OrderedDict

sc = None
spark = None
items = None

inputfile = sys.argv[1]
outputfile = sys.argv[2]


def initialize():
    global sc, spark, items, inputfile
    print("Initializing...")
    sc_conf = SparkConf()
    sc_conf.setAppName("Task1")
    sc_conf.setMaster('local[*]')
    sc_conf.set("spark.driver.bindAddress", "127.0.0.1")
    sc = SparkContext(conf=sc_conf)
    sc.setLogLevel("ERROR")
    jsonread = sc.textFile(inputfile)
    items = jsonread.map(json.loads)


def functionUseful(useful):
    if(useful > 0):
        return (1, 1)
    else:
        return (0, 0)


def functionRating(rating):
    if(rating == 5):
        return (1, 1)
    else:
        return (0, 1)


def taskA():
    global items
    print("Beginning Task A...")
    rdmap = items.map(lambda s: functionUseful(s['useful']))
    countsGlobal = rdmap.reduceByKey(lambda a, b: a + b)
    varf = countsGlobal.collectAsMap()
    return varf[1]


def taskB():
    global items
    print("Beginning Task B...")
    rdmap = items.map(lambda s: functionRating(s['stars']))
    countsGlobal = rdmap.reduceByKey(lambda a, b: a + b)
    varf = countsGlobal.collectAsMap()
    return varf[1]


def taskC():
    global items
    print("Beginning Task C...")
    rdmap = items.map(lambda s: (len(s['text']), s['text']))
    dicti = rdmap.max()
    return dicti[0]


def taskD():
    global items
    print("Beginning Task D...")
    rdmap = items.map(lambda s: (s['user_id'], 1))
    countsGlobal = rdmap.reduceByKey(lambda a, b: a + b)
    return (countsGlobal.count())


def taskE():
    global items
    print("Beginning Task E...")
    rdmap = items.map(lambda s: (s['user_id'], 1))
    countsGlobal = rdmap.reduceByKey(lambda a, b: a + b)
    bSorted = countsGlobal.sortBy(lambda a: a[1])
    top20 = (bSorted.takeOrdered(20, key=lambda x: (-x[1], x[0])))
    mylist = []
    for i in range(0, 20):
        listtemp = []
        strhgy = top20[i][0]
        listtemp.append(str(strhgy))
        listtemp.append(top20[i][1])
        mylist.append(listtemp)
    return mylist


def taskF():
    global items
    print("Beginning Task F...")
    rdmap = items.map(lambda s: (s['business_id'], 1))
    countsGlobal = rdmap.reduceByKey(lambda a, b: a + b)
    return countsGlobal.count()


def taskG():
    global items
    print("Beginning Task G...")
    rdmap = items.map(lambda s: (s['business_id'], 1))
    countsGlobal = rdmap.reduceByKey(lambda a, b: a + b)
    bSorted = countsGlobal.sortBy(lambda a: a[1])
    top20 = (bSorted.takeOrdered(20, key=lambda x: (-x[1], x[0])))
    mylist = []
    for i in range(0, 20):
        listtemp = []
        strhgy = top20[i][0]
        listtemp.append(str(strhgy))
        listtemp.append(top20[i][1])
        mylist.append(listtemp)
    return mylist


def createJsonDict(taskA, taskB, taskC, taskD, taskE, taskF, taskG):
    jsonStr = []
    print("Creating output Json...")
    jsonStr.append(('n_review_useful', taskA))
    jsonStr.append(('n_review_5_star', taskB))
    jsonStr.append(('n_characters', taskC))
    jsonStr.append(('n_user', taskD))
    jsonStr.append(('top20_user', taskE))
    jsonStr.append(('n_business', taskF))
    jsonStr.append(('top20_business', taskG))
    with open(outputfile, 'w') as json_file:
        json.dump(OrderedDict(jsonStr), json_file, indent=2)


def main():
    initialize()
    a = taskA()
    b = taskB()
    c = taskC()
    d = taskD()
    e = taskE()
    f = taskF()
    g = taskG()
    createJsonDict(a, b, c, d, e, f, g)
    sc.stop()
    print("Completed")


if __name__ == "__main__":
    print("started")
    print(inputfile)
    print(outputfile)
    main()
    pass
