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

        # keep track of the local clock
        self.local_clock = 0

    def create_stub(self) -> None:
        """Helper to facilitate communication between customers and a branch process with matching ID"""
        with grpc.insecure_channel(f"localhost:5005{self.id}") as channel:
            self.stub = banking_pb2_grpc.BranchStub(channel)
            self.execute_events()

    def execute_events(self) -> None:
        """Processes the events from the list of events and submits the request to the Branch process"""
        for event in self.events:
            request = banking_pb2.BranchRequest(
                interface=event["interface"],
                money=event.get("money"),
                type="customer",
                id=self.id,
                event_id=event.get("id"),
                clock=self.local_clock,
            )
            response = self.stub.MsgDelivery(request)
            self.update_local_clock(response.clock)

    def update_local_clock(self, *args: int) -> None:
        self.local_clock = max(self.local_clock, *args) + 1
