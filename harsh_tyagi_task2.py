from pyspark.context import SparkContext, SparkConf
import json
import sys
import time
from collections import OrderedDict

sc = None
spark = None
items1 = None
items2 = None

inputfile1 = sys.argv[1]
inputfile2 = sys.argv[2]
outputfile1 = sys.argv[3]
outputfile2 = sys.argv[4]
taskb = None


def initialize():
    global sc, spark, items1, items2, inputfile1, inputfile2
    print("Initializing...")
    sc_conf = SparkConf()
    sc_conf.setAppName("Task1")
    sc_conf.setMaster('local[*]')
    sc_conf.set("spark.driver.bindAddress", "127.0.0.1")
    sc = SparkContext(conf=sc_conf)
    sc.setLogLevel("ERROR")
    jsonread1 = sc.textFile(inputfile1)
    items1 = jsonread1.map(json.loads)
    jsonread2 = sc.textFile(inputfile2)
    items2 = jsonread2.map(json.loads)


def taskA():
    global sc, spark, items1, items2, inputfile1, inputfile2, outputfile1, taskb
    print("Begininng Task A...")
    rdmap1 = items1.map(lambda s: (s['business_id'], s['stars']))
    rdmap2 = items2.map(lambda s: (s['business_id'], s['state']))

    joined = rdmap2.join(rdmap1)
    mainmap = joined.map(lambda s: s[1])

    sumReduce = mainmap.reduceByKey(lambda a, b: (a+b))
    countMap = mainmap.map(lambda s: (s[0], 1))
    countReduce = countMap.reduceByKey(lambda a, b: (a+b))
    joinedAvg = sumReduce.join(countReduce)
    jignes = joinedAvg.mapValues(
        lambda x: x[0]/x[1]).sortBy(lambda a: (-a[1], a[0]))
    taskb = jignes
    print("Writing Output for task A...")
    with open(outputfile1, 'w') as taskaFile:
        taskaFile.write("state,stars \n")
        for i in jignes.collect():
            taskaFile.write(str(i[0])+',' + str(i[1])+'\n')


def taskB():
    global sc, spark, items1, items2, inputfile1, inputfile2, outputfile1, taskb
    print("Beginning Task B...")
    rdmap = taskb
    start1 = time.time()
    top5 = rdmap.collect()
    for i in range(0, 5):
        print(top5[i][0])
    end1 = time.time()

    start2 = time.time()
    method2 = taskb.take(5)
    print(method2)
    end2 = time.time()
    # print('Method1: '+str(end1-start1))
    # print('Method2: '+str(end2-start2))
    storingForTaskb(end1-start1, end2-start2)


def storingForTaskb(m1, m2):
    print("Writing Output JSON for task B...")
    with open(outputfile2, 'w') as json_file:
        jsonObj = []
        explanation = "The first function collects the entire RDD and transforms into list which is a huge chunk of data. The second method reads the top 5 rows of the RDD and then convert the five rows as a list of tuples. Hence the first method takes more time than the second method"
        jsonObj.append(("m1", m1))
        jsonObj.append(("m2", m2))
        jsonObj.append(("explanation", explanation))
        json.dump(OrderedDict(jsonObj), json_file, indent=2)


def main():
    print("Started")
    initialize()
    taskA()
    taskB()
    print("Completed")
    pass


if __name__ == "__main__":
    main()
    pass
