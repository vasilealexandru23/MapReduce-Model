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

Each mapper thread, will take some index of a file to compute. The index is choose via an synchronized
block (using a mutex, only one thread will do the job and there will not be race conditions). After
opening the coresponding index file, we read each word. Each thread will have some local memory, used
to keep all the words in a file (via an unordered set), and a set keeping for each word the pair
{word, index_file}. After computing all data, we close the file and use another synchronized region
to move the local data into the global data that keeps the information about words (do this also with
a mutex, that means that each time in that block, only one thread will push it's computation in the
global memory). After that, we check if there is another task (another file that we can process) and
we repeat the process. After the thread's work is done, we signal it with the barrier.

Each reducer thread process the global data (lists with format {word, index_file}). For that, each thread
will get an equally distributed range of data to work with (size of all lists / number of reducer threads),
and each range will differ by the index of the thread. Then we enter into a synchronized block, where each
reduce thread will post it's range of data in the global lists (type of {word, {file_x, file_y, ...}})
(done with a mutex also). After this we wait for all the other reducer threads to finish their job, with
a barrier. After all threads are ready for next step, we equally distribute the alphabet letters for each
thread (26 / number of reducer threads), and each of them will go for the coresponding letters, create a file
and upload it's data in them (first we put all the data in a local vector, and sort it by the number of
files that have the current word and then write it in the file).

### Note: Read more about the model in this [paperwork](https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf) done by Jeffrey Dean and Sanjay Ghemawat Google Engineers.

### Copyright 2024 Vasile Alexandru-Gabriel (vasilealexandru37@gmail.com)
