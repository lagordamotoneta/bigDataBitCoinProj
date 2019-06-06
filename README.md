# Wikileaks' Top Ten Bitcoin donors.
As part of Big Data Processing module in Msc. Big Data Science course, a final coursework was delivered. The goal was to apply the techniques covered in Big Data Processing to analyse a subset of bitcoin blockchain, ranging from the first mined bitcoin in 2009 to December 2014. Several jobs were created to perform multiple types of computation to obtain the Top Ten Wikileaks' bitcoin donors.

Hadoop MapReduce and Spark we used in this exercice as an aim to make a comparisson between both techniques.

## Dataset Overview

A subset of the blockchain (Blocks 1 to ~330000) was collected and saved to HDFS server. These blocks were changed from their original raw json format by splitting them into five comma delimited csv files: blocks.csv; transactions.csv; coingen.csv (Coin Generations/Mined coins); vin.csv; and vout.csv. To explain what these files contain a description and schema can be found below:

## Dataset schema

### BLOCKS.CSV
height: The block number

hash: The unique ID for the block

time: The time at which the block was created (Unix Timestamp in seconds)

difficulty: The complexity of the math problem associated with the block

Sample entry:

    0, 000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f,  1231006505,  1
### TRANSACTIONS
tx_hash: The unique id for the transaction

blockhash: The block this transaction belongs to

time: The time when the transaction occurred

tx_in_count: The number of transactions with outputs coming into this transaction

tx_out_count: The number of wallets receiving bitcoins from this transaction

Sample entry:

    0e3e2357e806b6cdb1f70b54c3a3a17b6714ee1f0e68bebb44a74b1efd512098,00000000839a8e6886ab5951d76f411475428afc90947ee320161bbf18eb6048,1231469665,1,1

### COINGEN
db_id: The block the coin generation was found within (maps to block height not hash)

tx_hash: Transaction id for the Coin Generation

coinbase: Input of a generation transaction. As a generation transaction has no parent and creates new coins from nothing, the coinbase can contain any arbitrary data (or perhaps a hidden meaning).

sequence: Allows unconfirmed time-locked transactions to be updated before being finalised

Sample entry:


    1,  0e3e2357e806b6cdb1f70b54c3a3a17b6714ee1f0e68bebb44a74b1efd512098,  04ffff001d0104,  4294967295

### VIN
txid: The associated transaction the coins are going into

tx_hash: The transaction the coins are coming from

vout: The ID of the output from the previous transaction - the value equals n in vout below

Sample entry:

    f4184fc596403b9d638783cf57adfe4c75c605f6356fbc91338530e9831e9e16,0437cd7f8525ceed2324359c2d0ba26006d92d856a9c20fa0241106ee5a597c9,0

### VOUT
hash: The associated transaction

value: The amount of bitcoins sent

n: The id for this output within the transaction. This will equal the vout field within the vin table above, if the coins have been respent

publicKey: The id for the wallet where the coins are being sent

Sample entry:

    0e3e2357e806b6cdb1f70b54c3a3a17b6714ee1f0e68bebb44a74b1efd512098, 50, 0, {12c6DSiU4Rq3P4ZxziKxzrL5LmMBrzjrJX}

## Highlevel development process.

To extract the top ten a join between the v_in and v_out files to get the wallet ID's and BTC amount coming in and out of relevant transactions. However, these are both large files so joining them straight away would be a very resource intensive job.

1. ***Pashe 1:Initial Filtering:*** In order to speed up the join operation, first filter /data/bitcoin/vout.csv to only rows containing the wallet of interest, {1HB5XMLmzFVj8ALj6mfBsbifRoD4miY36v} (make sure to include the curly brackets).
2. ***Phase 2:First Join*** Once you have obtained this much smaller version of the v_out file, the next step is to perform a replication join between it and /data/bitcoin/vin.csv
3. ***Phase 3:Second Join and Top Ten:*** As a v_in only specifies the previous transaction where the bitcoins originated from, this second output must be rejoined with /data/bitcoin/vout.csv. Stored the tx_hash and vout values from the v_in file into a dictionary and matched these to the hash and n fields (respectively) within the full v_out file. Once there is a match on both, yield the publicKey (wallet) and value in a pair to the reducer. Finally, sort these pairs via a top ten reducer.

## Developed files.

File | Method| Description
---|---|---
exploration.py | MapReduce | Exploration: Number of transactions which occurred every month between the start and end of the dataset. As the dataset spans multiple years and aggregation is done by month, the year was included in the analysis.
topTenPhase1.py | MapReduce | Top Ten Phase 1: Filter in order to speed up the join operation, first filter for /data/bitcoin/vout.csv to obtain rows containing the Wikileaks wallet id, {1HB5XMLmzFVj8 ALj6mfBsbifRoD4miY36v}.
topTenPhase2.py| MapReduce | Top Ten Phase 2: First Join, once a smaller version of the v_out file is obtained, the next step is to perform a replication join between it and /data/bitcoin/vin.csv.
topTenPhase3.py  | MapReduce | Top Ten Phase 3: Second join and Top Ten, Second join. As a v_in only specifies the previous transaction where the bitcoins originated from, this second output must be rejoined with /data/bitcoin/vout.csv. This is the last file created to obtain the top ten donators. This file was planned to run two MRStep jobs: the first one for the mapper in charge of the join and the first reducer to obtain the summed values of donato rs and the second one to obtain the final top ten.
topTenSpark.py | Spark |  Top Ten: Same exercise as the MapReduce jobs, but this time using Spark and a single python file.

##Top Ten contributors (wallets)

Number| Wallet Id | Value |Value in £ | Wallet owner
---|---|---|---|---
1 |17B6mtZr14VnCKaHkvzqpkuxMYKTvezDcp |46515.1894803|145,809,303.86
2 |19TCgtx62HQmaaGy8WNhLvoLXLr7LvaDYn | 5770.0 | 18,088,661.5
3 |14dQGpcUhejZ6QhAQ9UGVh7an78xoDnfap | 1931.482 | 6,052,067.07
4 |1LNWw6yCxkUmkhArb2Nf2MPw6vG7u5WG7q |1894.3741862399997| * | Mt. Gox
5 |1L8MdMLrgkCQJ1htiGRAcP11eJs662pYSS |806.13402728| 2,534, 533 .75
6 |1ECHwzKtRebkymjSnRKLqhQPkHCdDn6NeK |648.5199788 |2,037,299.57
7 |18pcznb96bbVE1mR7Di3hK7oWKsA1fDqhJ |637.04365574 |2,000, 641. 97
8 |19eXS2pE5f1yBggdwhPjauqCjS8YQCmnXa |576.835|1,811, 556 .09
9 |1B9q5KG69tzjhqq3WSz3H7PAxDVTAwNdbV |556.7|1,747,152 85
10|1AUGSxE5e8yPPLGd7BM2aUxfzbokT6ZYSq |500.0|1,569,205. 00

For the value in £ highlighted in the top ten donators, information from https://www.blockchain.com was taken. A search by wallet id will displayed the corresponding transactions and the functionality to change between Bitcoin value a nd British
Pound value is available. However, the wallet ranked as #4 has thousands of transactions and to go thru all of them by click ing next would have ta ken a lot of time.
Also was found that only the names of wallet id owner will appear if they have made their identity as public, as in the case of Mt. Gox.
