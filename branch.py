import logging
from typing import Union, Literal, Any

import grpc
import banking_pb2
import banking_pb2_grpc


class Branch(banking_pb2_grpc.BranchServicer):

    def __init__(self, _id: int, balance: int, branches: list):
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

    def MsgDelivery(self, request: Any, context: Any) -> Any:
        """Processes the requests received from other processes and returns results to requested process."""

        logging.debug(
            f"\t> branch {self.id} received {request.interface} from "
            f"{request.type} {request.id}"
            f"{' for the amount of: $ ' + str(request.money) if request.interface != 'query' else ''}"
        )

        if request.type == "branch":
            if request.interface == "deposit":
                self.balance += request.money

            elif request.interface == "withdraw":
                self.balance -= request.money

            logging.debug(f"\t> branch {self.id} balance is ${self.balance}")
            return banking_pb2.BranchReply(balance=self.balance, id=self.id)

        if request.type == "customer":
            try:
                if request.interface == "deposit":
                    self.deposit(request.money)

                elif request.interface == "withdraw":
                    self.withdraw(request.money)

                elif request.interface == "query":
                    logging.debug(f"\t> branch {self.id} balance is ${self.balance}")

            except Exception as e:
                logging.error(f"Customer transaction failed with error: {e}")

            else:
                self.processed_customer_events.append(request.interface)

        return banking_pb2.BranchReply(balance=self.balance, id=self.id)

    @staticmethod
    def _link_to_branch(
        _id: int, receiver: int, interface: str, money: Union[int, float]
    ) -> None:
        """Helper that creates a gRPC channel to a specific branch"""

        with grpc.insecure_channel(f"localhost:5005{receiver}") as channel:
            stub = banking_pb2_grpc.BranchStub(channel)
            request = banking_pb2.BranchRequest(
                interface=interface, money=money, type="branch", id=_id
            )
            response = stub.MsgDelivery(request)
            logging.debug(
                f"\t< branch {_id} confirms that branch {receiver} "
                f"has new balance of {response.balance}"
            )

    def _propagate_to_branches(
        self, amount: Union[int, float], propagate_type: Literal["deposit", "withdraw"]
    ) -> None:
        """Helper that propagates deposits or withdrawals to other branches"""
        logging.debug(f"\n\t******************************************************************")
        logging.debug(
            f"\t*** Propagating branch {self.id} {propagate_type} "
            f"of ${amount} to branches {self.branches} ***"
        )

        for target_branch in self.branches:
            logging.debug("")
            self._link_to_branch(
                _id=self.id,
                receiver=target_branch,
                interface=propagate_type,
                money=amount,
            )

        logging.debug(f"\t******************************************************************")

    def deposit(self, amount: Union[int, float]) -> None:
        """Initiate branch deposit"""
        self.balance += amount
        self._propagate_to_branches(amount=amount, propagate_type="deposit")

    def withdraw(self, amount: Union[int, float]) -> None:
        """Initiate branch withdraw"""
        self.balance -= amount
        self._propagate_to_branches(amount=amount, propagate_type="withdraw")

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
