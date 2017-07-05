# Description
This is the Insight Data Engineering Coding Challenge solution.

# Summary
src/main.py drives the ConsumerNetwork class implemented in src/ConsumerNetwork.py. ConsumerNetwork has a network object from the networkx package and a dictionary called userData which contains the purchase history of each user. 

# Requirements
* Python 3.4.3
* networkx
* json
* numpy
* timeit
* sys

# Usage
`python3 ./src/main.py ./log_input/batch_log.json ./log_input/stream_log.json ./log_output/flagged_purchases.json`
