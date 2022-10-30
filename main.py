import time
import grpc
import logging
import banking_pb2_grpc
from pprint import pprint
from concurrent import futures

from customer import Customer
from branch import Branch, BranchDebugger
from test_input_output import input_test


logging.basicConfig(level=logging.INFO, format='%(message)s')


class Main:

    def __init__(self, input_data: list) -> None:
        logging.info('Collecting input data...')

        self.input_data = input_data

        # collect branch and customer data from input
        self.branch_processes = []
        self.customer_processes = []
        self.parse_processes()

        self.list_processes()  # for debugging purposes

    def parse_processes(self) -> None:
        for process in self.input_data:
            if process['type'] == 'customer':
                self.customer_processes.append(process)
            if process['type'] == 'branch':
                self.branch_processes.append(process)

    def list_processes(self) -> None:
        logging.info('\nBranch Processes:')
        for p in self.branch_processes:
            logging.info(f'\t{p}')

        logging.info('\nCustomer Processes:')
        for p in self.customer_processes:
            logging.info(f'\t{p}')

    def run(self) -> None:
        logging.info('\nStarting branch processes...')

        # will keep track of running branch server threads (will get closed once input data has been processed)
        branch_server_procs = []
        branch_objs = []
        branch_debugger = BranchDebugger(branch_objs)

        try:
            # collect branch ids from input
            branch_process_ids = set(p['id'] for p in self.branch_processes)

            # start up branch servers
            for p in self.branch_processes:
                port = f"5005{p['id']}"
                branch = Branch(
                    _id=p['id'],
                    balance=p['balance'],
                    branches=list(branch_process_ids.difference({p['id']}))
                )
                branch_objs.append(branch)

                server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
                banking_pb2_grpc.add_BranchServicer_to_server(branch, server)
                server.add_insecure_port(f"[::]:{port}")
                server.start()
                branch_server_procs.append(server)
                logging.info(f"\t- Server started, listening on {port}")

            # log initial branch balances (should all be the same or in sync)
            branch_debugger.log_balances('initial balance')

            # initialize customer processes and execute events
            for p in self.customer_processes:
                customer = Customer(p['id'], p['events'])
                customer.create_stub()  # create stub and process events
                time.sleep(0.1)

            # allow any lingering transaction to be completed
            logging.info("\n\nWaiting 1 sec before wrapping up...")
            time.sleep(1)

        except Exception as e:
            logging.error(f"\n\n!!! Failed with error: {e}\n\n")

        else:
            # if no errors are raised
            # log final balances and output detailed customer events
            branch_debugger.log_balances('final balance')

            logging.info("\n\n########################")
            logging.info("\tOUTPUT:")
            logging.info("########################\n")
            pprint(branch_debugger.list_branch_transactions())
            logging.info("\n")

        finally:
            # stop/release branch servers
            for p in branch_server_procs:
                p.stop(grace=None)


if __name__ == '__main__':
    main = Main(input_data=input_test)
    main.run()
