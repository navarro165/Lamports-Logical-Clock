import time
import logging
from typing import Union, Literal, Any

import grpc
import banking_pb2
import banking_pb2_grpc


logging.basicConfig(level=logging.INFO, format='%(message)s')


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

    def MsgDelivery(self, request: Any, context: Any) -> Any:
        """Processes the requests received from other processes and returns results to requested process."""

        logging.info(f"\t> branch {self.id} received {request.interface} from "
                     f"{request.type} {request.originator_id}"
                     f"{' for the amount of: $ ' + str(request.money) if request.interface != 'query' else ''}")

        if request.type == "branch":
            if request.interface == "deposit":
                self.balance += request.money

            elif request.interface == "withdraw":
                self.balance -= request.money

            logging.info(f"\t> branch {self.id} balance is ${self.balance}")
            return banking_pb2.BranchReply(balance=self.balance, originator_id=self.id)

        if request.type == "customer":
            if request.interface == "deposit":
                self.deposit(request.money)

            elif request.interface == "withdraw":
                self.withdraw(request.money)

            elif request.interface == "query":
                logging.info(f"\t> branch {self.id} balance is ${self.balance}")

        return banking_pb2.BranchReply(balance=self.balance, originator_id=self.id)

    @staticmethod
    def _link_to_branch(originator_id: int, receiver: int, interface: str, money: Union[int, float]) -> None:
        """Helper that creates a gRPC channel to a specific branch"""

        with grpc.insecure_channel(f'localhost:5005{receiver}') as channel:
            stub = banking_pb2_grpc.BranchStub(channel)
            request = banking_pb2.BranchRequest(
                interface=interface,
                money=money,
                type="branch",
                originator_id=originator_id
            )
            response = stub.MsgDelivery(request)
            logging.info(f"\t< branch {originator_id} confirms that branch {receiver} "
                         f"has new balance of {response.balance}")

    def _propagate_to_branches(self, amount: Union[int, float], propagate_type: Literal["deposit", "withdraw"]) -> None:
        """Helper that propagates deposits or withdrawals to other branches"""
        logging.info(f"\n\t******************************************************************")
        logging.info(f"\t*** Propagating branch {self.id} {propagate_type} "
                     f"of ${amount} to branches {self.branches} ***")

        for target_branch in self.branches:
            logging.info('')
            self._link_to_branch(
                originator_id=self.id,
                receiver=target_branch,
                interface=propagate_type,
                money=amount,
            )
            time.sleep(0.1)

        logging.info(f"\t******************************************************************")

    def deposit(self, amount: Union[int, float]) -> None:
        """Initiate branch deposit"""
        self.balance += amount
        self._propagate_to_branches(amount=amount, propagate_type="deposit")

    def withdraw(self,  amount: Union[int, float]) -> None:
        """Initiate branch withdraw"""
        self.balance -= amount
        self._propagate_to_branches(amount=amount, propagate_type="withdraw")


class BranchDebugger:
    """Helper class that debugs branch processes"""

    def __init__(self, branches: list):
        self.branches = branches

    def log_balances(self, note: str) -> None:
        logging.info(f"\nBranch balances ({note}):")
        for b in self.branches:
            logging.info(f"\t- id: {b.id}, balance: {b.balance}")
