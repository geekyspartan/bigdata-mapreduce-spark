from pyspark import SparkContext
import sys

def blogsMapper(blog, allIndustries):
    dateTags = blog.split("</date>")
    postTags = blog.split("</post>")
    counts = dict()
    for idx, post in enumerate(postTags):
        if "<post>" in post:
            postData = post[post.find("<post>")+len("<post>"):]
            updatedPostData = postData.lower().replace(";", " ").replace(",", " ").replace(".", " ").replace(":", " ").replace("*", " ").replace("-", " ").replace("?", " ").replace("'", " ").replace("`", " ").replace("(", " ").replace(")", " ").replace("[", " ").replace("]", " ").replace("!", " ").replace('"', " ").replace("{", " ").replace("}", " ").replace('/', " ").split(" ")
            date = dateTags[idx]
            if "<date>" in date:
                dateData = date[date.find("<date>")+len("<date>"):]
                splitDate = dateData.split(",")
                yearMonth = splitDate[-1] + "-" + splitDate[-2]
                for industry in allIndustries:
                    count = updatedPostData.count(industry.lower())
                    if count > 0:
                        try:
                            if counts[industry]:
                                try:
                                    counts[industry][yearMonth] += count
                                except KeyError:
                                    counts[industry][yearMonth] = count
                        except KeyError:
                            counts[industry] = {}
                            counts[industry][yearMonth] = count
    return counts.items()

def blogsReducer(key1, key2):
    dict1 = dict(key1)
    dict2 = dict(key2)
    unionOfSet = set(dict1).union(set(dict2))
    dictCombined = {key: dict1.get(key, 0) + dict2.get(key, 0) for key in unionOfSet}
    if len(dictCombined) > 0:
        return tuple([(k, v) for k, v in dictCombined.items()])
    return ()

#part 2
class BlogCorpus():
    def runTasks(self, directoryPath):
        pairRdd = sc.wholeTextFiles(directoryPath)
        

        possibleIndustries = pairRdd.keys().map(lambda x: x[x.rfind('/') + 1:]).map(lambda x: x.split(".")[3].encode('ascii', 'ignore').decode('UTF-8')).collect()
        piBC = sc.broadcast(set(possibleIndustries))
        print("\n\n*****************\n Broadcast Industries Names and print \n*****************\n")
        print(piBC.value)

        frequencies = pairRdd.values().map(lambda blog: blogsMapper(blog, piBC.value)).flatMap(lambda x: x).reduceByKey(lambda dict1, dict2: blogsReducer(dict1, dict2)).collect()
        print("\n\n*****************\n Search for industry names in posts, recording by year-month and print \n*****************\n")
        print(frequencies)

if __name__ == "__main__":
    sc = SparkContext.getOrCreate()
    if len(sys.argv) > 1:
        directoryPath = sys.argv[1]
    else:
        directoryPath = "/Users/anuragarora/Desktop/blogs-sample/*"

    mrobject = BlogCorpus()
    mrobject.runTasks(directoryPath)

