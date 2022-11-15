import copy
import logging
from collections import defaultdict
from typing import Union, Literal, Any, Optional

import grpc
import banking_pb2
import banking_pb2_grpc
#TODO: add comments everywhere

class Event:
    """Helper class for organizing subevents"""

    def __init__(self):
        # keep track of the local clock
        self.local_clock = 0

        # replica of the Branch's balance
        self.balance = 0

        # will keep track of branch events as they come in
        self.branch_events = []

        # will keep track of sub-events organized by event id
        self.event_tracker = defaultdict(list)

    def _propagate_to_branches(
        self,
        amount: Union[int, float],
        propagate_type: Literal["deposit", "withdraw"],
        event_id: int,
    ) -> None:
        """Place holder for propagate to branches"""
        pass

    def update_branch_balance(self, interface: Literal["deposit", "withdraw"], amount: Union[int, float]) -> None:
        if interface == "withdraw":
            self.balance -= amount
        elif interface == "deposit":
            self.balance += amount
        else:
            raise ValueError("Invalid interface")

    def update_local_clock(self, remote_clock: int = None) -> None:
        if remote_clock:
            self.local_clock = max(self.local_clock, remote_clock) + 1
        else:
            self.local_clock += 1

    def log_event(self, event: dict, method_order_number: int, add_to_branch_events: bool = True) -> None:
        """TODO"""
        logging.debug(f"event {method_order_number}: {event}")
        if add_to_branch_events:
            self.branch_events.append(event)

        event = copy.copy(event)
        event_id = event.pop("id")
        self.event_tracker[event_id].append(event)

    # todo: maybe expalin the numbering and how its meant to make the logical clock easier to follow
    def event_request_1(
        self,
        event_id: int,
        interface: Literal["deposit", "withdraw"],
        remote_clock: int,
    ) -> None:
        """
        This subevent happens when the Branch process receives a request from the Customer process.
        The Branch process selects the larger value between the local clock and the remote clock from the message,
        and increments one from the selected value.
        """
        self.update_local_clock(remote_clock)
        event = {"id": event_id, "name": f"{interface}_request", "clock": self.local_clock}
        self.log_event(event, 1)

    def event_execute_2(
        self,
        event_id: int,
        interface: Literal["deposit", "withdraw"],
        amount: Union[int, float],
    ) -> None:
        """
        This subevent happens when the Branch process executes the event after the subevent “Event_Request”.
        The Branch process increments one from its local clock.
        """
        self.update_local_clock()
        event = {"id": event_id, "name": f"{interface}_execute", "clock": self.local_clock}
        self.log_event(event, 2)
        self.update_branch_balance(interface=interface, amount=amount)
        self._propagate_to_branches(amount=amount, propagate_type=interface, event_id=event_id)

    def event_propagate_request_3(
        self,
        event_id: int,
        interface: Literal["deposit", "withdraw"],
        remote_clock: int,
    ) -> None:
        """
        This subevent happens when the Branch process sends the propagation request to its fellow branch processes.
        The Branch process increments one from its local clock.
        """
        self.update_local_clock(remote_clock)
        event = {"id": event_id, "name": f"{interface}_propagate_request", "clock": self.local_clock}
        self.log_event(event, 3)

    def event_propagate_execute_4(
        self,
        event_id: int,
        interface: Literal["deposit", "withdraw"],
        amount: Union[int, float],
    ) -> None:
        """
        This subevent happens when the Branch process executes the event after the subevent “Propogate_Request”.
        The Branch process increments one from its local clock.
        """
        self.update_local_clock()
        event = {"id": event_id, "name": f"{interface}_propagate_execute", "clock": self.local_clock}
        self.log_event(event, 4)
        self.update_branch_balance(interface=interface, amount=amount)

    def event_propagate_response_5(
        self,
        event_id: int,
        interface: Literal["deposit", "withdraw"],
        remote_clock: int,
    ) -> None:
        """
        This subevent happens when the Branch receives the result of the subevent “Propogate_Execute” from its
        fellow branches. The Branch process selects the biggest value between the local clock and the remote clock
        from the message, and increments one from the selected value.
        """
        self.update_local_clock(remote_clock)
        event = {"id": event_id, "name": f"{interface}_propagate_response", "clock": self.local_clock}
        self.log_event(event, 5)

    def event_response_6(
            self,
            event_id: int,
            interface: Literal["deposit", "withdraw"],
    ) -> None:
        """
        This subevent happens after all the propagate responses are returned from the branches.
        The branch returns success / fail back to the Customer process.
        The Branch process increments one from its local clock.
        """
        self.update_local_clock()
        event = {"id": event_id, "name": f"{interface}_response", "clock": self.local_clock}
        self.log_event(event, 6, add_to_branch_events=False)


class Branch(banking_pb2_grpc.BranchServicer, Event):
    def __init__(self, _id: int, balance: int, branches: list):
        super().__init__()

        # keep track of the local clock
        self.local_clock = 0

        # unique ID of the Branch
        self.id = _id

        # replica of the Branch's balance
        self.balance = balance

        # the list of process IDs of the branches (excluding current one)
        self.branches = branches

        # the list of Client stubs to communicate with the branches
        self.stubList = list()

        # keep track of successful customer event
        self.processed_customer_events = []

        # will keep track of branch events as they come in
        self.branch_events = []

    def MsgDelivery(
        self, request: Any, context: Any, request_status: Optional[str] = None
    ) -> Any:
        """Processes the requests received from other processes and returns results to requested process."""

        if request.interface in ["deposit", "withdraw"]:
            if request.type == "customer":
                self.deposit_or_withdraw(request)
            elif request.type == "branch":
                self.deposit_or_withdraw_propagate(request)

        elif request.interface == "query":
            logging.info(f"\t> branch {self.id} balance is ${self.balance}")

        return banking_pb2.BranchReply(
            balance=self.balance,
            id=self.id,
            event_id=request.event_id,
            interface=request.interface,
            clock=self.local_clock,
            request_status=request_status,
        )

    def _link_to_branch(
        self,
        _id: int,
        receiver: int,
        interface: str,
        money: Union[int, float],
        clock: int,
        event_id: int,
    ) -> None:
        """Helper that creates a gRPC channel to a specific branch"""
        with grpc.insecure_channel(f"localhost:5005{receiver}") as channel:
            stub = banking_pb2_grpc.BranchStub(channel)
            request = banking_pb2.BranchRequest(
                interface=interface,
                money=money,
                type="branch",
                id=_id,
                clock=clock,
                event_id=event_id,
            )
            response = stub.MsgDelivery(request)
            self.event_propagate_response_5(
                event_id=response.event_id,
                interface=response.interface,
                remote_clock=response.clock,
            )

    def _propagate_to_branches(
        self,
        amount: Union[int, float],
        propagate_type: Literal["deposit", "withdraw"],
        event_id: int,
    ) -> None:
        """Helper that propagates deposits or withdrawals to other branches"""
        for target_branch in self.branches:
            logging.debug("")
            self._link_to_branch(
                _id=self.id,
                receiver=target_branch,
                interface=propagate_type,
                money=amount,
                clock=self.local_clock,
                event_id=event_id,
            )

    def deposit_or_withdraw(self, request: Any) -> None:
        """Initiate either a deposit or withdraw action on current branch"""
        # TODO add comments explaining sub events
        # branch to customer interface

        # Invoke request
        self.event_request_1(
            event_id=request.event_id,
            interface=request.interface,
            remote_clock=request.clock,
        )
        # Execute and propagate request
        self.event_execute_2(
            event_id=request.event_id,
            interface=request.interface,
            amount=request.money,
        )

        # Getting to this point means that no errors were observed and the customer request was successful
        self.event_response_6(
            event_id=request.event_id,
            interface=request.interface
        )


    def deposit_or_withdraw_propagate(self, request: Any) -> None:
        """TODO"""
        # branch to branch interface

        # Invoke propagate request
        self.event_propagate_request_3(
            event_id=request.event_id,
            interface=request.interface,
            remote_clock=request.clock,
        )

        # Execute request
        self.event_propagate_execute_4(
            event_id=request.event_id,
            interface=request.interface,
            amount=request.money,
        )

    def get_processed_customer_events(self) -> dict:
        """Retrieve customer related events processed in this branch"""
        processed_events = {"id": self.id, "recv": []}
        for event in self.processed_customer_events:
            details = {"interface": event, "result": "success"}
            if event == "query":
                details["money"] = self.balance

            processed_events["recv"].append(details)

        return processed_events


class BranchDebugger:
    """Helper class for debugging branch processes"""

    def __init__(self, branches: list):
        self.branches = branches

    def log_balances(self, note: str) -> None:
        logging.info(f"\n\n\n\nBranch balances ({note}):")
        for b in self.branches:
            logging.info(f"\t- id: {b.id}, balance: {b.balance}")

        logging.debug("\n")

    def list_branch_transactions(self) -> list:
        return [b.get_processed_customer_events() for b in self.branches]

    def log_events(self) -> None:
        # TODO fix these logs
        import pprint
        for b in self.branches:
            print(f"id: {b.id}")
            pprint.pprint(b.branch_events)
            print("")
            logging.debug("\n")

    def log_events2(self) -> None:
        events = defaultdict(list)
        for b in self.branches:
            for event_id, sub_events in b.event_tracker.items():
                events[event_id] += sub_events
                
        for event_id, sub_events in events.items():
            events[event_id] = sorted(sub_events, key=lambda x: x["clock"])
            
        import pprint
        pprint.pprint(events)
        logging.debug("\n")
