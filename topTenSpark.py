import pyspark
import re

sc = pyspark.SparkContext()

def is_good_vout_line(line):
    try:
        fields = line.split(',')
        if len(fields)!=4:
            return False
        return True
    except:
        return False

def is_wikiLeack_wallet(line):
    try:
        fields = line.split(',')
        public_key = fields[3]
        wikipediaid = "{1HB5XMLmzFVj8ALj6mfBsbifRoD4miY36v}"
        if (public_key!=wikipediaid):
            return False
        return True
    except:
        return False

def is_good_vin_line(line):
    try:
        fields = line.split(',')
        if len(fields)!=3:
            return False
        return True
    except:
        return False

# VOUT
#  0      1       2         3
#hash , value ,   n  , publicKey

# VIN
#  0        1      2
#txid,  tx_hash, vout

#Part B1
#First: We need to clean vout lines
vout_lines = sc.textFile("/data/bitcoin/vout.csv")
#"/data/bitcoin/vout.csv"
clean_vout_lines = vout_lines.filter(is_good_vout_line)
#Second: filter clean vout lines to obtain only lines related to Wikileaks
wiki_vout_lines = clean_vout_lines.filter(is_wikiLeack_wallet)
#################################################################
#Part B2
#Once we have obtained this much smaller version of the v_out file,
#the next step is to perform a replication join between it and /data/bitcoin/vin.csvself.
#First: We need to clean vin lines
vin_lines = sc.textFile("/data/bitcoin/vin.csv")
#"/data/bitcoin/vin.csv"
clean_vin_lines = vin_lines.filter(is_good_vin_line)
#Second: Tranform wiki_vout_lines to get what will be acting as the key for our join
wiki_vout_lines_key=wiki_vout_lines.map(lambda l: (l.split(',')[0],l.split(',')[1]))
#Third: Transform clean_vin_lines to get what will be acting as the key for our join. Also we obtain the fields we need from vin for the next step.
vin_lines_key=clean_vin_lines.map(lambda l: (l.split(',')[0],(l.split(',')[1],l.split(',')[2])))
#Fourth : The join between vout Wikileaks related lines and vin lines.
vin_vout_joined_lines= vin_lines_key.join(wiki_vout_lines_key)
#vin_vout_joined_lines.saveAsTextFile("vout-vin-JoinedData")
#################################################################
#Part B3
#Second join. As a v_in only specifies the previous transaction where the bitcoins originated from, previous output must be rejoined with /data/bitcoin/vout.csv.
vout_lines_key=clean_vout_lines.map(lambda l: ((l.split(',')[0],l.split(',')[2]),(l.split(',')[3],l.split(',')[1])))
#vout_vin_lines_key=vin_vout_joined_lines.map(lambda l: ((l[1][0][0],l[1][0][1]),"null"))
vout_vin_lines_key=vin_vout_joined_lines.map(lambda l: (l[1]))
#Join between
vinvout_vout_join = vout_lines_key.join(vout_vin_lines_key)
key_to_reduce=vinvout_vout_join.map(lambda l : (l[1][0][0],float(l[1][0][1])))
reduced_keys=key_to_reduce.reduceByKey(lambda x,y: x+y)
topTen=reduced_keys.takeOrdered(10, key = lambda x: -x[1])
topTenFinal=sc.parallelize(topTen)
topTenFinal.saveAsTextFile("TopTen")
