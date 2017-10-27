import sys
from abc import ABCMeta, abstractmethod
from multiprocessing import Process, Manager
from pprint import pprint
import numpy as np
from random import random


##########################################################################
##########################################################################
# PART I. MapReduce

class MyMapReduce:
    __metaclass__ = ABCMeta
    
    def __init__(self, data, num_map_tasks=5, num_reduce_tasks=3):
        self.data = data  #the "file": list of all key value pairs
        self.num_map_tasks=num_map_tasks #how many processes to spawn as map tasks
        self.num_reduce_tasks=num_reduce_tasks # " " " as reduce tasks

    ###########################################################   
    #programmer methods (to be overridden by inheriting class)

    @abstractmethod
    def map(self, k, v):
        print("Need to override map")

    
    @abstractmethod
    def reduce(self, k, vs):
        print("Need to override reduce")
        

    ###########################################################
    #System Code: What the map reduce backend handles

    def mapTask(self, data_chunk, namenode_m2r):
        #runs the mappers and assigns each k,v to a reduce task
        for (k, v) in data_chunk:
            #run mappers:
            mapped_kvs = self.map(k, v)
            #assign each kv pair to a reducer task
            for (k, v) in mapped_kvs:
                namenode_m2r.append((self.partitionFunction(k), (k, v)))


    def partitionFunction(self,k):
        #given a key returns the reduce task to send it
        #add code to convert k to string, just in case k is an integer
        if type(k) is int:
            k = str(k)
        asciiSum = 0
        for char in k:
            asciiSum = asciiSum + ord(char)
        return (asciiSum % self.num_reduce_tasks) + 1

    def reduceTask(self, kvs, namenode_fromR):
        #sort all values for each key (can use a list of dictionary)
        kvs.sort()
        group = dict()
        for kv in kvs:
            try:
                group[kv[0]].append(kv[1])
            except KeyError:
                group[kv[0]] = [kv[1]]
        #call reducers on each key with a list of values
        #and append the result for each key to namenode_fromR
        for key,values in group.items():
            reducerOutput = self.reduce(key, values)
            if reducerOutput is not None:
                namenode_fromR.append(reducerOutput)
    
        return namenode_fromR
		
    def runSystem(self):
        #runs the full map-reduce system processes on mrObject

        #the following two lists are shared by all processes
        #in order to simulate the communication
        #[DONE]
        namenode_m2r = Manager().list() #stores the reducer task assignment and 
                                          #each key-value pair returned from mappers
                                          #in the form: [(reduce_task_num, (k, v)), ...]
        namenode_fromR = Manager().list() #stores key-value pairs returned from reducers
                                        #in the form [(k, v), ...]
        
        #divide up the data into chunks accord to num_map_tasks, launch a new process
        #for each map task, passing the chunk of data to it. 

        chunkSize = int(len(self.data) / self.num_map_tasks)
        remainingChunks = len(self.data) - chunkSize*self.num_map_tasks
        remainingChunksDistributed = 0
        allMapProcess = []
        for i in range(0, self.num_map_tasks):
            chunkStart = chunkSize*i + remainingChunksDistributed
            if remainingChunks > 0:
                chunkEnd = chunkSize*(i+1) + 1 + remainingChunksDistributed
                remainingChunks = remainingChunks - 1
                remainingChunksDistributed = remainingChunksDistributed + 1
            else:
                chunkEnd = chunkSize*(i+1) + remainingChunksDistributed
            process = Process(target=self.mapTask, args=(self.data[chunkStart:chunkEnd], namenode_m2r))
            process.start()
            allMapProcess.append(process)
    
        #join map task processes back
        for p in allMapProcess:
            p.join()
		
        #print output from map tasks
        print("namenode_m2r after map tasks complete:")
        pprint(sorted(list(namenode_m2r)))

        #"send" each key-value pair to its assigned reducer by placing each 
        #into a list of lists, where to_reduce_task[task_num] = [list of kv pairs]
        to_reduce_task = [[] for i in range(self.num_reduce_tasks)]
        for task in namenode_m2r:
            to_reduce_task[task[0] - 1].append(task[1])

        #launch the reduce tasks as a new process for each.
        allReduceProcess = []
        for i in range(0, self.num_reduce_tasks):
            #take care of the remaining chunks
            process = Process(target=self.reduceTask, args=(to_reduce_task[i], namenode_fromR))
            process.start()
            allReduceProcess.append(process)


        #join the reduce tasks back
        for p in allReduceProcess:
            p.join()
        
        #print output from reducer tasks
        print("namenode_m2r after reduce tasks complete:")
        pprint(sorted(list(namenode_fromR)))

        #return all key-value pairs:
        return namenode_fromR


##########################################################################
##########################################################################
##Map Reducers:
            
class WordCountMR(MyMapReduce):
    #the mapper and reducer for word count
    def map(self, k, v):
        counts = dict()
        for w in v.split():
            w = w.lower() #makes this case-insensitive
            try:  #try/except KeyError is just a faster way to check if w is in counts:
                counts[w] += 1
            except KeyError:
                counts[w] = 1
        return counts.items()
    
    def reduce(self, k, vs):
        return (k, np.sum(vs))        
    

class SetDifferenceMR(MyMapReduce):
	#contains the map and reduce function for set difference
	#Assume that the mapper receives the "set" as a list of any primitives or comparable objects
    def map(self, k, v):
        valueKeyMapping = {}
        for attribute in v:
            try:
                valueKeyMapping[attribute].append(k)
            except KeyError:
                valueKeyMapping[attribute] = [k]
        return valueKeyMapping.items()

    def reduce(self, k, vs):
        if len(vs) == 1 and vs[0][0] == 'R':
             return k
        else:
            return None

##########################################################################
##########################################################################


if __name__ == "__main__":
    ###################
    ##run WordCount:
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
    mrObject = WordCountMR(data, 4, 3)
    mrObject.runSystem()

    ####################
    ##run SetDifference
    print("\n\n*****************\n Set Difference\n*****************\n")
    data1 = [('R', ['apple', 'orange', 'pear', 'blueberry']),
              ('S', ['pear', 'orange', 'strawberry', 'fig', 'tangerine'])]
    data2 = [('R', [x for x in range(50) if random() > 0.5]),
              ('S', [x for x in range(50) if random() > 0.75])]
    mrObject = SetDifferenceMR(data1, 2, 2)
    mrObject.runSystem()
    mrObject = SetDifferenceMR(data2, 2, 2)
    mrObject.runSystem()

      
