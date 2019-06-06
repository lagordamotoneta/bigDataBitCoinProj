from mrjob.job import MRJob
from mrjob.step import MRStep

class v_top_ten(MRJob):

    v_outin_table = {}
#v_outin
#      0           1            2             3
#v_in_txid,  v_in_tx_hash , v_in_vout,  v_out_amount
    def mapper_join_init(self):
        # load companylist into a dictionary
        # run the job with --file input/companylist.tsv
        with open("vout-vin-JoinedData.txt") as f:
            for line in f:
                v_outin_list = line.split("\t")
                v_outin_array= v_outin_list[1]
                v_outin_string=v_outin_array[1:-1]
                v_outin_final_list=v_outin_string.split(",")
                v_index=v_outin_final_list[1][2:-1]
                v_value=v_outin_final_list[2][2:-1]
                self.v_outin_table[v_index] = v_value

#v_out
#  0     1    2      3
#hash,value, n, publicKey

    def mapper_repl_join(self, _, line):
        fields=line.split(",")
        try:
            #if(len(fields)==2):
                v_out_hash=fields[0]
                v_out_value=float(fields[1])
                v_out_n=fields[2]
                v_out_publicKey=fields[3]
                if v_out_hash in self.v_outin_table:
                    if v_out_n==self.v_outin_table[v_out_hash]:
                        yield(v_out_publicKey,v_out_value)
        except:
            pass

    def reducer_sums(self,row,value):
        sum_values = sum(value)
        yield(row,sum_values)

    def mapper_top(self,row,value):
        yield(None,(row,value))

    def reducer_top(self,row,value):
        sorted_values= sorted(value,reverse=True,key=lambda x :x[1])[:10]
        for sorted_value in sorted_values:
            wallet=sorted_value[0]
            value=sorted_value[1]
            yield(wallet,value)

    def steps(self):
        return [MRStep(mapper_init=self.mapper_join_init,
                        mapper=self.mapper_repl_join,
                        reducer=self.reducer_sums),
                MRStep(mapper=self.mapper_top,
                        reducer=self.reducer_top)]

#This line declares the class Lab4, that extends the MRJob format.
#this part of the python script tells to actually run the defined MapReduce job. Note that Lab4 is the name of the class
if __name__ == '__main__':
#    Lab4.JOBCONF= { 'mapreduce.reduce.memory.mb': 4096 }
   v_top_ten.run()
