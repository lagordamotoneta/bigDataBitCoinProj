#Lab 1. Basic wordcount

from mrjob.job import MRJob
from mrjob.step import MRStep

class v_out_v_in_join(MRJob):

    v_out_table = {}
#v_out
# 0       1     2       3
#hash,  value , n,  publicKey
    def mapper_join_init(self):
        # load companylist into a dictionary
        # run the job with --file input/companylist.tsv
        with open("voutFilter.txt") as f:
            for line in f:
                v_out_list = line.split("\t")
                v_out_array= v_out_list[1]
                v_out_string=v_out_array[1:-1]
                v_out_final_list=v_out_string.split(",")
                v_index=v_out_final_list[0]
                v_value=v_out_final_list[1]
                self.v_out_table[v_index] = v_value
#v_in
#  0        1      2
#txid,  tx_hash, vout

    def mapper_repl_join(self, _, line):
        fields=line.split(",")
        try:
            #if(len(fields)==2):
                v_in_txid=fields[0]
                v_in_tx_hash=fields[1]
                v_in_vout=fields[2]
                if v_in_txid in self.v_out_table:
                    v_out_amount=self.v_out_table[v_in_txid]
                    yield(None,(v_in_txid,v_in_tx_hash,v_in_vout,v_out_amount))
        except:
            pass


    def steps(self):
        return [MRStep(mapper_init=self.mapper_join_init,
                        mapper=self.mapper_repl_join)]#,
                # MRStep(mapper=self.mapper_length,
                #         reducer=self.reducer_sum)]
                    #reducer=self.reducer_sum)]
#This line declares the class Lab4, that extends the MRJob format.
#this part of the python script tells to actually run the defined MapReduce job. Note that Lab4 is the name of the class
if __name__ == '__main__':
#    Lab4.JOBCONF= { 'mapreduce.reduce.memory.mb': 4096 }
   v_out_v_in_join.run()
