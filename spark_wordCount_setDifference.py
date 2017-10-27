from random import random
from pyspark import SparkContext

#punctuation remover for Word count #not using this code, but this can be used in flatMap for word count like this: removePunctuations(line).split(" ")
def removePunctuations(line):
    return line.replace(";", " ").replace(",", " ").replace(".", " ").replace(":", " ").replace("*", " ").replace("-", " ").replace("?", " ").replace("'", " ").replace("`", " ").replace("(", " ").replace(")", " ").replace("[", " ").replace("]", " ").replace("!", " ").replace('"', " ").replace("{", " ").replace("}", " ").replace('/', " ").replace("  ", " ")


#mapper for SetDifference
def mapSet(set):
    valueKeyMapping = {}
    key = set[0]
    for attribute in set[1]:
        try:
            valueKeyMapping[attribute].append(key)
        except KeyError:
            valueKeyMapping[attribute] = key
    return valueKeyMapping.items()

if __name__ == "__main__":
    sc = SparkContext.getOrCreate()
    
    #WordCount Difference Implementation below
    data = [(1, "The horse raced past the barn fell"),
            (2, "The complex houses married and single soldiers and their families"),
            (3, "There is nothing either good or bad, but thinking makes it so"),
            (4, "I burn, I pine, I perish"),
            (5, "Come what come may, time and the hour runs through the roughest day"),
            (6, "Be a yardstick of quality."),
            (7, "A horse is the projection of peoples' dreams about themselves - strong, powerful, beautiful"),
            (8, "I believe that at the end of the century the use of words and general educated opinion will have altered so much that one will be able to speak of machines thinking without expecting to be contradicted."),
            (9, "The car raced past the finish line just in time."),
            (10, "Car engines purred and the tires burned.")]
    rdd1 = sc.parallelize(data)
    wordCountResult = rdd1.values().flatMap(lambda line: line.split(" ")).map(lambda word: (word.lower(), 1)).reduceByKey(lambda a,b: a+b).sortByKey()
    print("\n\n*****************\n Word Count Result \n*****************\n")
    print(wordCountResult.collect())
    #WordCount Difference Implementation end
    
    #SetDifference Difference Implementation below
    data1 = [('R', ['apple', 'orange', 'pear', 'blueberry']), ('S', ['pear', 'orange', 'strawberry', 'fig', 'tangerine'])]
    data2 = [('R', [x for x in range(50) if random() > 0.5]), ('S', [x for x in range(50) if random() > 0.75])]
    rdd2 = sc.parallelize(data1)
    rdd3 = sc.parallelize(data2)
    reduceResult1 = rdd2.flatMap(lambda x: mapSet(x)).groupByKey().mapValues(list).filter(lambda a: len(a[1]) == 1 and a[1][0] == 'R').sortByKey().keys()
    print("\n\n*****************\n Set Difference Result 1 \n*****************\n")
    print(reduceResult1.collect())

    reduceResult2 = rdd3.flatMap(lambda x: mapSet(x)).groupByKey().mapValues(list).filter(lambda a: len(a[1]) == 1 and a[1][0] == 'R').sortByKey().keys()
    print("\n\n*****************\n Set Difference Result 2 \n*****************\n")
    print(reduceResult2.collect())
    #SetDifference Difference Implementation end

