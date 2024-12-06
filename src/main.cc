#include <iostream>
#include <fstream>
#include <unordered_map>
#include <unordered_set>
#include <set>
#include <vector>
#include <atomic>
#include <algorithm>
#include "pthread_barrier_mac.h"

struct thread_args_t {
    /* Global buffers for mapper threads. */
    int mapper_index;
    pthread_mutex_t mutex_mapper_output;
    pthread_mutex_t mutex_mapper_parse_file;
    std::unordered_set<std::string> all_words;
    std::set<std::pair<std::string, int> > mapper_output;
    std::unordered_map<char, std::vector<std::string> > words_by_first_letter;
    int number_files;
    int mapper_threads;
    std::vector<std::string> files;

    /* Buffers used by both threads. */
    pthread_barrier_t finish_mapper_barrier;

    /* Global buffer for reducer threads. */
    int reducer_threads;
    std::unordered_map<std::string, std::vector<int> > reducer_output;
    pthread_mutex_t mutex_reducer_output;
    pthread_barrier_t barrier;
};

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

    struct thread_args_t *args = (struct thread_args_t*)arg;

    while (true) {
        pthread_mutex_lock(&args->mutex_mapper_parse_file);
        if (args->mapper_index == args->number_files) {
            pthread_mutex_unlock(&args->mutex_mapper_parse_file);
            break;
        } else {
            id_file = args->mapper_index;
            args->mapper_index++;
        }
        pthread_mutex_unlock(&args->mutex_mapper_parse_file);

        std::string word;
        std::unordered_set<std::string> words_in_file;
        std::set<std::pair<std::string, int> > mapper_output_curr_file;
        std::ifstream fin(args->files[id_file]);

        /* Check if file exists. */
        if (!fin.good()) {
            std::cerr << "File " << args->files[id_file] << " does not exist.\n";
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
        pthread_mutex_lock(&args->mutex_mapper_output);
        for (auto word : words_in_file) {
            if (args->all_words.find(word) == args->all_words.end()) {
                args->words_by_first_letter[word[0]].push_back(word);
            }
            args->all_words.insert(word);
        }
        for (auto word : mapper_output_curr_file) {
            args->mapper_output.insert(word);
        }
        pthread_mutex_unlock(&args->mutex_mapper_output);
    }
    
    pthread_barrier_wait(&args->finish_mapper_barrier);

    pthread_exit(NULL);
}

void* reducer(void *arg)
{
    std::pair<int, struct thread_args_t*> data = *(std::pair<int, struct thread_args_t*>*)arg;
    struct thread_args_t *args = data.second;

    pthread_barrier_wait(&args->finish_mapper_barrier);

    int id = data.first;
    int quantity = (args->mapper_output.size() + args->reducer_threads - 1) / args->reducer_threads;

    int start = id * quantity;
    int end = std::min((id + 1) * quantity, (int)args->mapper_output.size());

    auto itr = args->mapper_output.begin();
    std::advance(itr, start);

    pthread_mutex_lock(&args->mutex_reducer_output);
    for (; itr != args->mapper_output.end() && start < end; itr++, start++) {
        std::string word = itr->first;
        int file_index = itr->second;

        args->reducer_output[word].push_back(file_index);
    }
    pthread_mutex_unlock(&args->mutex_reducer_output);

    pthread_barrier_wait(&args->barrier);

    /* Can start to construct output files. */
    int letters_handle = (26 + args->reducer_threads - 1) / args->reducer_threads;
    int start_letter = id * letters_handle;
    int end_letter = std::min((id + 1) * letters_handle, 26);

    for (int i = start_letter; i < end_letter; i++) {
        char letter = 'a' + i;
        std::string file_name = std::string(1, letter) + ".txt";

        std::vector<std::pair<std::string, std::vector<int> > > output_in_file;
        for (auto word : args->words_by_first_letter[letter]) {
            std::vector<int> reducer_output_curr_word = args->reducer_output[word];
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

void read_input_file(std::string &file_input, int &number_files, std::vector<std::string> &files)
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
    int mapper_threads = std::atoi(argv[1]);
    int reducer_threads = std::atoi(argv[2]);
    std::string file_input = argv[3];
    int number_files;
    std::vector<std::string> files;

    /* Read the input file. */
    read_input_file(file_input, number_files, files);

    /* Initialize the thread arguments. */
    struct thread_args_t thread_args;

    thread_args.number_files = number_files;
    thread_args.mapper_threads = mapper_threads;
    thread_args.reducer_threads = reducer_threads;
    thread_args.files = files;

    thread_args.mapper_index = 0;

    pthread_mutex_init(&thread_args.mutex_mapper_output, NULL);
    pthread_mutex_init(&thread_args.mutex_reducer_output, NULL);
    pthread_mutex_init(&thread_args.mutex_mapper_parse_file, NULL);
    pthread_barrier_init(&thread_args.barrier, NULL, reducer_threads);
    pthread_barrier_init(&thread_args.finish_mapper_barrier, NULL, mapper_threads + reducer_threads);

    pthread_t threads[mapper_threads + reducer_threads];
    std::pair<int, struct thread_args_t*> thread_data[mapper_threads + reducer_threads];

    for (int i = 0; i < mapper_threads + reducer_threads; i++) {
        if (i < mapper_threads) {
            thread_data[i] = {i, &thread_args};
            pthread_create(&threads[i], NULL, mapper, &thread_args);
        } else {
            thread_data[i] = {i - mapper_threads, &thread_args};
            pthread_create(&threads[i], NULL, reducer, &(thread_data[i]));
        }
    }

    /* Wait for all threads to finish. */
    for (int i = 0; i < mapper_threads + reducer_threads; i++) {
        pthread_join(threads[i], NULL);
    }

    pthread_mutex_destroy(&thread_args.mutex_mapper_output);
    pthread_mutex_destroy(&thread_args.mutex_reducer_output);
    pthread_mutex_destroy(&thread_args.mutex_mapper_parse_file);
    pthread_barrier_destroy(&thread_args.barrier);
    pthread_barrier_destroy(&thread_args.finish_mapper_barrier);

    return 0;
}
