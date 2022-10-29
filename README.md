# gRPCDistributedBankingSimulator
gRPC Distributed Banking Simulator

 Simulate a distributed banking system that allows multiple customers to withdraw or deposit money from multiple branches in the bank. We assume that there are no concurrent updates on the same resources (money) in the bank, and no customer accesses multiple branches. Each branch maintains a replica of the money that needs to be consistent with the replicas in other branches. The customer communicates with only a specific branch that has the same unique ID as the customer. Although each customer independently updates a specific replica, the replicas stored in each branch need to reflect all the updates made by the customer.
