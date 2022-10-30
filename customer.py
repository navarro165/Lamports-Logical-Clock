import time
import logging
import grpc
import banking_pb2
import banking_pb2_grpc


class Customer:
    def __init__(self, _id: int, events: list):
        # unique ID of the Customer
        self.id = _id

        # events from the input
        self.events = events

        # a list of received messages used for debugging purpose
        self.recvMsg = list()

        # pointer for the stub
        self.stub = None

    def create_stub(self) -> None:
        """Helper to facilitate communication between customers and a branch process with matching ID"""
        with grpc.insecure_channel(f'localhost:5005{self.id}') as channel:
            self.stub = banking_pb2_grpc.BranchStub(channel)
            self.execute_events()

    def execute_events(self) -> None:
        """Processes the events from the list of events and submits the request to the Branch process"""
        logging.info(f"\n\nExecuting customer {self.id} events...")

        for event in self.events:
            logging.info(f"\n\n\t###################")
            logging.info(f"\t##### {event['interface'].upper()} #####")
            logging.info(f"\t###################")

            request = banking_pb2.BranchRequest(
                interface=event['interface'],
                money=event.get('money'),
                type="customer",
                id=self.id
            )
            response = self.stub.MsgDelivery(request)
            time.sleep(0.1)

            logging.info(f"\t^ customer {self.id} confirms that branch {self.id} "
                         f"has{' ' if event['interface'] == 'query' else ' new '}"
                         f"balance of {response.balance}".upper())
