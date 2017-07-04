import networkx as nx
import json
import sys
import abc
import numpy as np
#
# This is the main script for the Insight Data Engineering Coding Challenge.
#
#
# Usage:
#
#   python3 ./src/process_log.py ./log_input/batch_log.json ./log_input/stream_log.json ./log_output/flagged_purchases.json
#
#
# Notes:
#
#   The variables 'T' and 'D' have been named numTrac and degrees respectively. (Variable
#   names consisting of single letters normally leads to headaches.)
#
# todo:
#   check timestamps are in correct order while reading
#   check degrees greater than 0
#   check cast from str to int for T and D
#
#
# Nicholas Brawand - nicholasbrawand@gmail.com

# what you were doing:

batchLog = sys.argv[1]
streamLog = sys.argv[2]
flaggedPurchases = sys.argv[3]


class ConsumerNetwork:
    """Implements the consumer network object"""

    def __init__(self, batchLog):
        """Initializes a network and userData from batchLog json file.

        network - networkx graph of user connections
        userData - dictionary containing user network's latest purchases
        degrees - int user's network consists of neighbors up to degree = degrees
        numTrac - int number of purchases to keep in user's network history"""

        logFil = open(batchLog, 'r')
        header = json.loads(logFil.readline())

        self.network = nx.Graph()
        self.userData = {}
        self.degrees = int(header['D'])
        self.numTrac = int(header['T'])

        # read each line of batchLog and
        # add the event to the this ConsumerNetwork
        for line in logFil:
            eventInfo = json.loads(line)
            self.ProcessEvent(eventInfo)

    def UserDataUpdate(self, id):
        """Add empty entry with id to consumerNetwork userData"""
        self.userData.update({id: []})

    def CheckAndAddUser(self, event):
        """Check if users in event are in self.userData and self.network if not add them."""

        try:
            # if user is given by 'id' key
            # check self.userData
            if not event['id'] in self.userData:
                self.UserDataUpdate(event['id'])
            # check self.network
            if not event['id'] in self.network.nodes():
                self.network.add_node(event['id'])
        except:
            # if users are given by 'id1' and 'id2' keys
            for ID in ['id1', 'id2']:
                # check self.userData
                if not event[ID] in self.userData:
                    self.UserDataUpdate(event[ID])
                # check self.network
                if not event[ID] in self.network.nodes():
                    self.network.add_node(event[ID])

    def PurchaseEvent(self, event, lookForAnomaly=False, outFil=None):
        """Update self.userData and self.network with new event purchase"""

        # update userData with latest purchase
        if len(self.userData[event['id']]) == self.numTrac:
            self.userData[event['id']].pop(0)

        self.userData[event['id']].append(float(event['amount']))


        # get list of neighbors
        neighbors = set()

        if self.degrees > 0:
            neighbors = set(self.network.neighbors(event['id']))

            # search through neighbor's neighbors up to 'self.degrees' times
            for i in range(1, self.degrees):
                tmp = set(neighbors)

                for neighbor in neighbors:
                    tmp = tmp.union(self.network.neighbors(neighbor))

                # copy new tmp set but remove starting node (we only want
                # neighbors)
                neighbors = set(tmp)-set(event['id'])

        # check for anomaly and update userData
        # compile network purchases
        networkPurchases = []
        for neighbor in neighbors:
            networkPurchases = networkPurchases + self.userData[neighbor]

            # check for anomaly
            if lookForAnomaly:
                if len(networkPurchases) > 1:

                    std = round(np.std(networkPurchases), 2)
                    mean = round(np.mean(networkPurchases), 2)

                    if float(event['amount']) > mean + (3.0 * std):

                        tmp = '"event_type":"{}", "timestamp":"{}", "id": "{}", "amount": "{}", '.format(
                            event['event_type'], event['timestamp'], event['id'], event['amount'])+'"mean": "{0:.2f}", "sd": "{1:.2f}"'.format(mean, std)
                        tmp = "{"+tmp+"}"

                        if not outFil is None:
                            outFil.write(tmp+'\n')
                        else:
                            print(tmp)


    def Befriend(self, event):
        """Update self.userData and self.network with new event befriend"""
        self.network.add_edge(event['id1'], event['id2'])

    def Unfriend(self, event):
        """Update self.userData and self.network with new event unfriend"""
        self.network.remove_edge(event['id1'], event['id2'])

    def ProcessEvent(self, event, lookForAnomaly=False, outFil=None):
        """Update userData and network information from event"""

        # check if user exists, if not add them
        self.CheckAndAddUser(event)

        if event['event_type'] == 'purchase':
            self.PurchaseEvent(
                event,
                lookForAnomaly=lookForAnomaly,
                outFil=outFil)

        elif event['event_type'] == 'befriend':
            self.Befriend(event)

        elif event['event_type'] == 'unfriend':
            self.Unfriend(event)

        else:
            print('Warning unrecognized event: {}'.format(event['event_type']))

    def ProcessLogFile(self, logFilName, lookForAnomaly=False, outFilName=None):

        logFil = open(logFilName, 'r')

        if not outFilName is None:
            outFil = open(outFilName, 'w')

        # read each line of logFil and
        # add the event to the ConsumerNetwork
        for line in logFil:
            eventInfo = json.loads(line)
            if not outFilName is None:
                self.ProcessEvent(
                    eventInfo,
                    lookForAnomaly=lookForAnomaly,
                    outFil=outFil)
            else:
                self.ProcessEvent(eventInfo, lookForAnomaly=lookForAnomaly)

cs = ConsumerNetwork(batchLog)

# process stream
#cs.ProcessLogFile(streamLog, lookForAnomaly=True)
cs.ProcessLogFile(streamLog, lookForAnomaly=True, outFilName=flaggedPurchases)


#network, userData=cs.network, cs.userData
#print(network.nodes(), network.edges(), userData)
