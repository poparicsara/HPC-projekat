# HPC-projekat

Pokretanje:
  mpiexec -np <broj_procesa> --oversubscribe python3 kafka.py [<rangovi procesa za topic0>] [<rangovi procesa za topic1>] [<rangovi procesa za topic2>] <rangovi procesa za topic3>]
  
  primer: 
    mpiexec -np 16 --oversubscribe python3 kafka.py [3,4] [] [1,10,11] []
