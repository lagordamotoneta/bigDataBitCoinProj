from mrjob.job import MRJob
import re
import time

#This line declares the class Coursework, that extends the MRJob format.
class Coursework(MRJob):

# this class will define two additional methods: the mapper method goes here
#Fields contains line as follows.
#VOUT
       #  0      1       2         3
       #hash , value ,   n  , publicKey
#Implement a filter to get Wikileaks related vout lines (where Wikileaks was the recipient of the transaction)
    def mapper(self,_, line):
        fields=line.split(",")
        try:
            if(len(fields)==4):
                wikipediaid = "{1HB5XMLmzFVj8ALj6mfBsbifRoD4miY36v}"
                if fields[3]==wikipediaid:
                    yield(None,(line))
        except:
            pass


#this part of the python script tells to actually run the defined MapReduce job. Note that Coursework is the name of the class
if __name__ == '__main__':
#    Coursework.JOBCONF= { 'mapreduce.job.reduces': '3' }
    Coursework.run()
