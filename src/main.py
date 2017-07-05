import timeit
import sys
import ConsumerNetwork as cn
#
# This is the main script for the Insight Data Engineering Coding Challenge.
#
# Usage:
#
#   python3 process_log.py batch_log.json stream_log.json flagged_purchases.json
#
# Nicholas Brawand - nicholasbrawand@gmail.com

batchLog = sys.argv[1]
streamLog = sys.argv[2]
flaggedPurchases = sys.argv[3]

#startTime = timeit.default_timer()

# init network
net = cn.ConsumerNetwork(batchLog)
# process stream
net.ProcessLogFile(streamLog, lookForAnomaly=True, outFilName=flaggedPurchases)

#elapsed = timeit.default_timer() - startTime
#print(elapsed)
