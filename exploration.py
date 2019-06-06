#import needed libraries
from mrjob.job import MRJob
import re
import time

#This line declares the class Coursework, that extends the MRJob format.
class Coursework(MRJob):

# this class will define two additional methods: the mapper method goes here
#Transactions Fields contains line as follows.
       #  0            1        2         3           4
       #tx_hash , blockhash , time , tx_in_count , tx_out_count

    def mapper(self,_, line):
        fields=line.split(",")
        try:
            if(len(fields)==5):
                time_epoch = int(fields[2])
                btc_dates = time.strftime("%m-%Y",time.gmtime(time_epoch))
                yield(btc_dates,1)
        except:
            pass
            #no need to do anything, just ignore the line, as it was malformed
# combiner
    def combiner(self,btc_dates,counts):
        sums=sum(counts)
        yield(btc_dates,sums)
#and the reducer method goes after this line
    def reducer(self,btc_dates,counts):
        sums=sum(counts)
        yield(btc_dates,sums)

#this part of the python script tells to actually run the defined MapReduce job. Note that Coursework is the name of the class
if __name__ == '__main__':
#    Coursework.JOBCONF= { 'mapreduce.job.reduces': '3' }
    Coursework.run()
