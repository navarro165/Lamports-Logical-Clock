import sys
import time
import grpc
import logging
import threading
import banking_pb2_grpc
from concurrent import futures

from customer import Customer
from branch import Branch, BranchDebugger
from test_input_output import input_test


class Main:
    def __init__(self, input_data: list) -> None:
        logging.info("Collecting input data...")
        self.input_data = input_data

        # collect branch and customer data from input
        self.branch_processes = []
        self.customer_processes = []
        self.parse_processes()

        # for debugging purposes
        self.list_processes()

    def parse_processes(self) -> None:
        """Extract branch and customer event information from the input data"""
        for process in self.input_data:
            if process["type"] == "customer":
                self.customer_processes.append(process)
            if process["type"] == "branch":
                self.branch_processes.append(process)

    def list_processes(self) -> None:
        """Log processes to execute to facilitate with debugging"""
        logging.debug("\nBranch Processes:")
        for p in self.branch_processes:
            logging.info(f"\t{p}")

        logging.info("\nCustomer Processes:")
        for p in self.customer_processes:
            logging.info(f"\t{p}")

    def execute_customer_events(self) -> None:
        """Execute customer events in parallel"""

        threads = []
        for p in self.customer_processes:
            customer = Customer(p["id"], p["events"])
            threads.append(threading.Thread(target=customer.create_stub))  # create stub and process events

        # start threads
        for t in threads:
            t.start()
            time.sleep(0.2)

        # wait until the threads complete execution
        for t in threads:
            t.join()

    def run(self) -> None:
        logging.info("\nStarting branch processes...")

        # will keep track of running branch server threads (will get closed once input data has been processed)
        branch_server_procs = []
        branch_objs = []
        branch_debugger = BranchDebugger(branch_objs)

        try:
            # collect branch ids from input
            branch_process_ids = set(p["id"] for p in self.branch_processes)

            # start up branch servers
            for p in self.branch_processes:
                port = f"5005{p['id']}"
                branch = Branch(
                    _id=p["id"],
                    balance=p["balance"],
                    branches=list(branch_process_ids.difference({p["id"]})),
                )
                branch_objs.append(branch)

                server = grpc.server(futures.ThreadPoolExecutor(max_workers=3))
                banking_pb2_grpc.add_BranchServicer_to_server(branch, server)
                server.add_insecure_port(f"[::]:{port}")
                server.start()
                branch_server_procs.append(server)
                logging.info(f"\t- Server started, listening on {port}")

            # log initial branch balances (should all be the same or in sync)
            branch_debugger.log_balances("initial balance")

            # initialize customer processes and execute events
            logging.info("\n... STARTING CUSTOMER EVENTS ...")
            self.execute_customer_events()
            logging.info("\n... FINISHED CUSTOMER EVENTS ...")

            # allow any lingering transaction to be completed
            logging.debug("\nWaiting 3 sec before wrapping up...")
            time.sleep(3)

        except Exception as e:
            logging.error(f"\n\n!!! Failed with error: {e}\n\n")

        else:
            # if no errors are raised
            # log final balances and output detailed customer events
            branch_debugger.log_balances("final balance")

            # log branch events (organized by both branch and event ids)
            branch_debugger.output_logger()

        finally:
            # stop/release branch servers
            for p in branch_server_procs:
                p.stop(grace=None)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format="%(message)s")
    main = Main(input_data=input_test)
    main.run()
