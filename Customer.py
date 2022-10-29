import grpc
import example_pb2
import example_pb2_grpc
import time

class Customer:
    def __init__(self, id, events):
        # unique ID of the Customer
        self.id = id

        # events from the input
        self.events = events

        # a list of received messages used for debugging purpose
        self.recvMsg = list()

        # pointer for the stub
        self.stub = None

    # TODO: students are expected to create the Customer stub
    def createStub(self):
        """
        Customers use the Customer.createStub to communicate with the Branch
        process with the ID identical to the Customer.
        """
        pass

    # TODO: students are expected to send out the events to the Bank
    def executeEvents(self):
        """
        Customer.executeEvents processes the events from the list of events
        stored in the Customer class and submits the request to the Branch process.
        """
        pass