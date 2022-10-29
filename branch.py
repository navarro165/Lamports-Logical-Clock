import time
import grpc
import logging
import banking_pb2
import banking_pb2_grpc
from typing import Union, Literal, Any


logging.basicConfig(level=logging.INFO, format='%(message)s')


class Branch(banking_pb2_grpc.BranchServicer):

    def __init__(self, id: int, balance: int, branches: list):
        # unique ID of the Branch
        self.id = id

        # replica of the Branch's balance
        self.balance = balance

        # the list of process IDs of the branches (excluding current one)
        self.branches = branches

        # the list of Client stubs to communicate with the branches
        self.stubList = list()

        # TODO: ask about this todo
        # TODO: students are expected to store the processID of the branches

    # TODO: students are expected to process requests from both Client and Branch
    def MsgDelivery(self, request, context):
        """Processes the requests received from other processes and returns results to requested process."""
        print("zzzzz")
        if request.type == "branch":
            logging.info(f"\t> branch {self.id} received {request.interface} propagation from "
                         f"branch {request.originator_id} for the amount of: ${request.money}")
            print("aaaa")
            if request.interface in {"deposit", "withdraw"}:
                if request.interface == "deposit":
                    print("bbbb")
                    self.balance += request.money
                elif request.interface == "withdraw":
                    self.balance -= request.money
                print("cccc")
                logging.info(f"\t> branch {self.id} balance is now ${self.balance}")
                return banking_pb2.BranchReply(balance=self.balance, originator_id=self.id)

    @staticmethod
    def _link_to_branch(originator_id: int, receiver: int, interface: str, money: Union[int, float]) -> None:
        """Helper that creates a gRPC channel to a specific branch"""

        with grpc.insecure_channel(f'localhost:5005{receiver}') as channel:
            print(f">>>> : {f'localhost:5005{receiver}'}")
            stub = banking_pb2_grpc.BranchStub(channel)
            request = banking_pb2.BranchRequest(
                interface=interface,
                money=money,
                type="branch",
                originator_id=originator_id
            )
            response = stub.MsgDelivery(request)
            logging.info(f"\t< branch {originator_id} confirms that branch {receiver} "
                         f"has new balance of {response.balance}\n")

    def _propagate_to_branches(self, amount: Union[int, float], propagate_type: Literal["deposit", "withdraw"]) -> None:
        """Helper that propagates deposits or withdrawals to other branches"""

        logging.info(f"\nPropagating branch {self.id} {propagate_type} of ${amount} to branches {self.branches}...")

        for target_branch in self.branches:
            self._link_to_branch(
                originator_id=self.id,
                receiver=target_branch,
                interface=propagate_type,
                money=amount,
            )
            time.sleep(0.5)

    def deposit(self, amount: Union[int, float]) -> None:
        """Initiate branch deposit"""
        self.balance += amount
        self._propagate_to_branches(amount=amount, propagate_type="deposit")

    def withdraw(self,  amount: Union[int, float]) -> None:
        """Initiate branch withdraw"""
        self.balance -= amount
        self._propagate_to_branches(amount=amount, propagate_type="withdraw")
