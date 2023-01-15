import math
import pickle
import sys
from mpi4py import MPI

TAG_0 = 0
TAG_1 = 1
TAG_2 = 2
TAG_3 = 3

def bcast(data, comm, tag, comm_num):

    root = 0
    rank = MPI.UNDEFINED
    size = MPI.UNDEFINED

    if comm != MPI.COMM_NULL:
        rank = comm.Get_rank()
        size = comm.Get_size()

    if rank == 0:
        data = pickle.dumps(data)
        for dest in range(1, size):
            comm.send(data, dest, tag)
    elif rank != MPI.UNDEFINED:
        comm.recv(source=root, tag=tag)
        data = pickle.loads(data)
        print('COMM: ', comm_num, 'rank: ', rank, ', message: ', data)


def is_prime(number):

    if number < 2:
        return False
    elif (number % 2) == 0:
        return False
    elif (number % 3) == 0:
        return False
    else:
        for k in range(1, int(math.sqrt(number))):
            if (number % (6*k+1)) == 0 and number != (6*k+1):
                return False
            elif (number % (6*k-1)) == 0 and number != (6*k-1):
                return False

    return True
    

def create_comm(world_comm, partitions):

    ranks = [0, 1]

    group = world_comm.group.Incl(ranks)
    if len(partitions) > 1:
        group = world_comm.group.Incl(partitions)
    comm = world_comm.Create(group)

    return comm

 
def sort_partitions(partitions):

    n = len(partitions)
    for i in range(n-1):
        for j in range(0, n-i-1):
            if partitions[j] > partitions[j + 1]:
                partitions[j], partitions[j + 1] = partitions[j + 1], partitions[j]
         
    return partitions


def get_partitions(partitions):

    partitions = partitions[1:len(partitions)-1]    # Get rid of [ ]
    if partitions == '':
        return []

    partitions_str = partitions.split(',')
    partitions = []
    for part in partitions_str:
        partitions.append(int(part))

    return sort_partitions(partitions)


def main():

    # PARTITIONS

    partitions0 = []
    partitions1 = []
    partitions2 = []
    partitions3 = []

    if len(sys.argv) > 1:
        partitions0 = get_partitions(sys.argv[1])
    if len(sys.argv) > 2:
        partitions1 = get_partitions(sys.argv[2])
    if len(sys.argv) > 3:
        partitions2 = get_partitions(sys.argv[3])
    if len(sys.argv) > 4:
        partitions3 = get_partitions(sys.argv[4])


    # WORLD COMMUNICATOR
    world_comm = MPI.COMM_WORLD
    world_rank = world_comm.Get_rank()

    # COMMUNICATORS
    comm0 = MPI.UNDEFINED
    comm1 = MPI.UNDEFINED
    comm2 = MPI.UNDEFINED
    comm3 = MPI.UNDEFINED

    # RANKS
    rank0 = MPI.UNDEFINED
    rank1 = MPI.UNDEFINED
    rank2 = MPI.UNDEFINED
    rank3 = MPI.UNDEFINED

    # TOPIC 0

    comm0 = create_comm(world_comm, partitions0)
    if comm0 != MPI.COMM_NULL:
        rank0 = comm0.Get_rank()

    # TOPIC 1

    comm1 = create_comm(world_comm, partitions1)
    if comm1 != MPI.COMM_NULL:
        rank1 = comm1.Get_rank()

    # TOPIC 2

    comm2 = create_comm(world_comm, partitions2)
    if comm2 != MPI.COMM_NULL:
        rank2 = comm2.Get_rank()

    # TOPIC 3

    comm3 = create_comm(world_comm, partitions3)
    if comm3 != MPI.COMM_NULL:
        rank3 = comm3.Get_rank()

    # Imitation of data stream
    for i in range(0, 10):
        data = i
        root = 0
        sent = False

        if (i % 3) == 0 and len(partitions0) > 1:
            if rank0 != MPI.UNDEFINED:
                comm0.bcast(data, root)
                print('Rank: ', world_rank, ', message: ', data)
            sent = True

        if (i % 5) == 0 and len(partitions1) > 1:
            if rank1 != MPI.UNDEFINED:
                comm1.bcast(data, root)
                print('Rank: ', world_rank, ', message: ', data)
            sent = True

        if is_prime(i) and len(partitions2) > 1:
            if rank2 != MPI.UNDEFINED:
                comm2.bcast(data, root)
                print('Rank: ', world_rank, ', message: ', data)
            sent = True

        if sent == False and len(partitions3) > 1:
            if rank3 != MPI.UNDEFINED:
                comm3.bcast(data, root)
                print('Rank: ', world_rank, ', message: ', data)


if __name__ == '__main__':
    main()
