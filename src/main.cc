#include <iostream>
#include <fstream>
#include <unordered_map>
#include <unordered_set>
#include <set>
#include <vector>
#include <atomic>
#include <algorithm>
#include "pthread_barrier_mac.h"

int number_files;
std::vector<std::string> files;
int mapper_threads, reducer_threads;

/* Set with all the words used. */
std::unordered_set<std::string> all_words;

/* The lists with all words and file's index from. */
std::set<std::pair<std::string, int> > mapper_output;
/* Mutex for the mapper output. */
pthread_mutex_t mutex_mapper_output;
pthread_mutex_t mutex_mapper_parse_file;

/* The list with the reducer operation output. */
std::unordered_map<std::string, std::vector<int> > reducer_output;
/* Mutex for the reducer output. */
pthread_mutex_t mutex_reducer_output;

/* Map with words mapped by alphabet letters. */
std::unordered_map<char, std::vector<std::string> > words_by_first_letter;

/* Atomic variable to know when each reduce thread to start working. */
int mapper_index;

/* Barrier used to know when each reduce thread to start construct the output files. */
pthread_barrier_t barrier;

pthread_barrier_t finish_mapper_barrier;

std::string remove_non_letters(std::string &word)
{
    std::string word_alpha;
    for (char c : word) {
        if (c >= 'a' && c <= 'z') {
            word_alpha.push_back(c);
        }
    }
    return word_alpha;
}

void* mapper(void *arg)
{
    int id_file;

    while (true) {
        pthread_mutex_lock(&mutex_mapper_parse_file);
        if (mapper_index == number_files) {
            pthread_mutex_unlock(&mutex_mapper_parse_file);
            break;
        } else {
            id_file = mapper_index;
            mapper_index++;
        }
        pthread_mutex_unlock(&mutex_mapper_parse_file);

        std::string word;
        std::unordered_set<std::string> words_in_file;
        std::set<std::pair<std::string, int> > mapper_output_curr_file;
        std::ifstream fin(files[id_file]);

        /* Check if file exists. */
        if (!fin.good()) {
            std::cerr << "File " << files[id_file] << " does not exist.\n";
            exit(1);
        }

        /* Process the current file. */
        while (fin >> word) {
            /* Transform the word into lowercase alphabet. */
            std::transform(word.begin(), word.end(), word.begin(), ::tolower);
            std::string word_alpha = remove_non_letters(word);

            words_in_file.insert(word_alpha);
            mapper_output_curr_file.insert(std::make_pair(word_alpha, id_file + 1));
        }

        /* Close the current file. */
        fin.close();

        /* Put data in the common buffer. */
        pthread_mutex_lock(&mutex_mapper_output);
        for (auto word : words_in_file) {
            if (all_words.find(word) == all_words.end()) {
                words_by_first_letter[word[0]].push_back(word);
            }
            all_words.insert(word);
        }
        for (auto word : mapper_output_curr_file) {
            mapper_output.insert(word);
        }
        pthread_mutex_unlock(&mutex_mapper_output);
    }
    
    pthread_barrier_wait(&finish_mapper_barrier);

    pthread_exit(NULL);
}

void* reducer(void *arg)
{
    pthread_barrier_wait(&finish_mapper_barrier);

    int id = *(int*)arg;
    int quantity = (mapper_output.size() + reducer_threads - 1) / reducer_threads;

    int start = id * quantity;
    int end = std::min((id + 1) * quantity, (int)mapper_output.size());

    auto itr = mapper_output.begin();
    std::advance(itr, start);

    pthread_mutex_lock(&mutex_reducer_output);
    for (; itr != mapper_output.end() && start < end; itr++, start++) {
        std::string word = itr->first;
        int file_index = itr->second;

        reducer_output[word].push_back(file_index);
    }
    pthread_mutex_unlock(&mutex_reducer_output);

    pthread_barrier_wait(&barrier);

    /* Can start to construct output files. */
    int letters_handle = (26 + reducer_threads - 1) / reducer_threads;
    int start_letter = id * letters_handle;
    int end_letter = std::min((id + 1) * letters_handle, 26);

    for (int i = start_letter; i < end_letter; i++) {
        char letter = 'a' + i;
        std::string file_name = std::string(1, letter) + ".txt";

        std::vector<std::pair<std::string, std::vector<int> > > output_in_file;
        for (auto word : words_by_first_letter[letter]) {
            std::vector<int> reducer_output_curr_word = reducer_output[word];
            std::sort(reducer_output_curr_word.begin(), reducer_output_curr_word.end());
            output_in_file.push_back(std::make_pair(word, reducer_output_curr_word));
        }

        /* Sort them in descending order by the size of the vector with indexes size. */
	    std::sort(output_in_file.begin(), output_in_file.end(),
		    [](const std::pair<std::string, std::vector<int>> &a,
               const std::pair<std::string, std::vector<int>> &b)
            {
			    if (a.second.size() == b.second.size()) {
				    return a.first < b.first;
			    }
			    return a.second.size() > b.second.size();
		    });

	std::ofstream fout(file_name);
        for (auto word : output_in_file) {
            fout << word.first << ":[";
            bool first = true;
            for (auto file_index : word.second) {
                if (!first) {
                    fout << " ";
                } else {
                    first = false;
                }
                fout << file_index;
            }
            fout << "]\n";
        }
        fout.close();
    }

    pthread_exit(NULL);
}

void read_input_file(std::string &file_input)
{
    std::ifstream fin(file_input);

    /* Check if file exists. */
    if (!fin.good()) {
        std::cerr << "File " << file_input << " does not exist.\n";
        exit(1);
    }

    /* Read it's data. */
    fin >> number_files;
    for (int i = 0; i < number_files; i++) {
        std::string file_name;
        fin >> file_name;
        files.push_back(file_name);
    }

    fin.close();
}

int main(int argc, char **argv)
{
    if (argc != 4) {
        std::cerr << "Usage: " << argv[0] << " <mapper_threads> <reducer_threads> <file_input>\n";
        return 1;
    }

    /* Extract parameters. */
    mapper_threads = std::atoi(argv[1]);
    reducer_threads = std::atoi(argv[2]);
    std::string file_input = argv[3];

    /* Read the input file. */
    read_input_file(file_input);

    mapper_index = 0;

    pthread_mutex_init(&mutex_mapper_output, NULL);
    pthread_mutex_init(&mutex_reducer_output, NULL);
    pthread_mutex_init(&mutex_mapper_parse_file, NULL);
    pthread_barrier_init(&barrier, NULL, reducer_threads);
    pthread_barrier_init(&finish_mapper_barrier, NULL, mapper_threads + reducer_threads);

    pthread_t threads[mapper_threads + reducer_threads];
    int thread_ids[mapper_threads + reducer_threads];

    for (int i = 0; i < mapper_threads + reducer_threads; i++) {
        if (i < mapper_threads) {
            thread_ids[i] = i;
            pthread_create(&threads[i], NULL, mapper, NULL);
        } else {
            thread_ids[i] = i - mapper_threads;
            pthread_create(&threads[i], NULL, reducer, &thread_ids[i]);
        }
    }

    /* Wait for all threads to finish. */
    for (int i = 0; i < mapper_threads + reducer_threads; i++) {
        pthread_join(threads[i], NULL);
    }

    pthread_mutex_destroy(&mutex_mapper_output);
    pthread_mutex_destroy(&mutex_reducer_output);
    pthread_mutex_destroy(&mutex_mapper_parse_file);
    pthread_barrier_destroy(&barrier);
    pthread_barrier_destroy(&finish_mapper_barrier);

    return 0;
}
