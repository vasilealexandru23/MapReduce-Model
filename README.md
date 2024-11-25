# MapReduce: Simplified Data Processing on Large Clusters

### This directory contains the MapReduce Model tested on finding an inverted index for a set of input files.

## `Structure of the directory:`
  * `checker/` -> Directory containing the testing files to check correctness.
  * `src/` -> Directory with implementation of the model directly applied on the inverted index task.
  * `docker*` -> Files for testing the program with docker.
    
## Implementation:

First step is reading the input data, number of mapper threads, number of reducer threads and the input
file. We start the total number of threads, and each thread will have it's own special task. To start
reducer threads do the work, we have to wait for the mapper threads to finish their job. This is done
by a barrier (when the count in it will be equal to the total number of threads, means that all mapper
threads have done their job, and all reducer threads started).

### Note: Read more about the model in this [paperwork](https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf) done by Jeffrey Dean and Sanjay Ghemawat Google Engineers.

### Copyright 2024 Vasile Alexandru-Gabriel (vasilealexandru37@gmail.com)
