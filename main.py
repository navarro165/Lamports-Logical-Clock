import time
import grpc
import logging
import banking_pb2
import banking_pb2_grpc

from branch import Branch
from concurrent import futures
from contextlib import contextmanager
from test_input_output import input_test


logging.basicConfig(level=logging.INFO, format='%(message)s')


class Main:

    def __init__(self, input_data: list) -> None:
        logging.info('Collecting input data...')

        self.input_data = input_data
        self.branch_processes = []
        self.client_processes = []

        self.parse_processes()
        self.list_processes()

    def parse_processes(self) -> None:
        for process in self.input_data:
            if process['type'] == 'customer':
                self.client_processes.append(process)
            if process['type'] == 'branch':
                self.branch_processes.append(process)

    def list_processes(self) -> None:
        logging.info('\nBranch Processes:')
        for p in self.branch_processes:
            logging.info(f'\t{p}')

        logging.info('\nCustomer Processes:')
        for p in self.client_processes:
            logging.info(f'\t{p}')

    @contextmanager
    def initialize_branch_processes(self) -> None:
        port = 50051  # starting port
        branch_process_ids = set(p['id'] for p in self.branch_processes)

        # will keep track of the last server to be instantiated in order to hold processes running
        last_server = None
        branch_server_procs = []
        branch_objs = []

        try:
            for p in self.branch_processes:
                port_str = str(port)
                branch = Branch(
                    id=p['id'],
                    balance=p['balance'],
                    branches=list(branch_process_ids.difference({p['id']}))
                )
                branch_objs.append(branch)

                server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
                banking_pb2_grpc.add_BranchServicer_to_server(branch, server)
                server.add_insecure_port(f'[::]:{port_str}')
                server.start()
                logging.info(f"\t- Server started, listening on {port_str}")

                branch_server_procs.append(server)
                last_server = server
                port += 1

            print(branch_objs[0].balance)
            print(branch_objs[1].balance)
            print(branch_objs[2].balance)

            branch_objs[0].deposit(100)
            # branch_objs[1].deposit(10)
            # branch_objs[2].deposit(10)
            # # branch_objs[0].deposit(10)
            # #
            # # time.sleep(1)
            # #
            print(branch_objs[0].balance)
            print(branch_objs[1].balance)
            print(branch_objs[2].balance)

            yield
            last_server.wait_for_termination()

        except KeyboardInterrupt:
            pass

        finally:
            for p in branch_server_procs:
                p.stop(grace=None)

    def run(self) -> None:
        logging.info('\nStarting branch processes...')

        with self.initialize_branch_processes():
            pass # maybe this context manager is not needed


if __name__ == '__main__':
    main = Main(input_data=input_test)
    main.run()
