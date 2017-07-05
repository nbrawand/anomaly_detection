import networkx as nx
import json
import abc
import numpy as np
#
# ConsumerNetwork class
#
# Nicholas Brawand - nicholasbrawand@gmail.com


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
        logFil.close()

        # read each line of batchLog and
        # add the event to the this ConsumerNetwork dont look for naomaly
        self.ProcessLogFile(
            batchLog,
            lookForAnomaly=False,
            outFilName=None,
            skipRows=1)

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

        # get list of neighbors
        # to calculate statistics
        neighbors = set()
        if self.degrees > 0:
            neighbors = set(self.network.neighbors(event['id']))

            # create neighbor list
            for i in range(1, self.degrees):
                tmp = set(neighbors)

                for neighbor in neighbors:
                    tmp = tmp.union(self.network.neighbors(neighbor))

                # remove self from neighbor list
                neighbors = set(tmp)-set(event['id'])

        # check for anomaly and update userData
        # compile network purchases
        networkPurchases = []
        for neighbor in neighbors:
            networkPurchases = networkPurchases + self.userData[neighbor]

        # check for anomaly and write to file
        if lookForAnomaly:
            if len(networkPurchases) > 1:

                std = round(np.std(networkPurchases), 2)
                mean = round(np.mean(networkPurchases), 2)

                if float(event['amount']) > mean + (3.0 * std):

                    tmp = '"event_type":"{}", "timestamp":"{}", "id": "{}", "amount": "{}", '.format(
                        event['event_type'],
                        event['timestamp'],
                        event['id'],
                        event['amount'])+'"mean": "{0:.2f}", "sd": "{1:.2f}"'.format(
                        mean,
                        std)
                    tmp = "{"+tmp+"}"

                    if not outFil is None:
                        outFil.write(tmp+'\n')
                    else:
                        print(tmp)

        # update userData with latest purchase
        if len(self.userData[event['id']]) == self.numTrac:
            self.userData[event['id']].pop(0)

        self.userData[event['id']].append(float(event['amount']))

    def Befriend(self, event):
        """Update self.userData and self.network with new event befriend"""
        self.network.add_edge(event['id1'], event['id2'])

    def Unfriend(self, event):
        """Update self.userData and self.network with new event unfriend"""
        self.network.remove_edge(event['id1'], event['id2'])

    def ProcessEvent(self, eventLine, lookForAnomaly=False, outFil=None):
        """Update userData and network information from event"""

        # skip if empty
        if eventLine == '\n':
            return

        # load line into dict
        event = json.loads(eventLine)

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

    def ProcessLogFile(
        self,
        logFilName,
        lookForAnomaly=False,
        outFilName=None,
     skipRows=0):

        logFil = open(logFilName, 'r')

        # skip lines
        for i in range(0, skipRows):
            logFil.readline()

        # open output file
        if not outFilName is None:
            outFil = open(outFilName, 'w')

        # process all events in logFil
        if not outFilName is None:
            for line in logFil:
                self.ProcessEvent(
                    line,
                    lookForAnomaly=lookForAnomaly,
                    outFil=outFil)
        else:
            for line in logFil:
                self.ProcessEvent(line, lookForAnomaly=lookForAnomaly)
