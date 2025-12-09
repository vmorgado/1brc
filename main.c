#include <assert.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

#define ENABLE_DEBUG_PRINTS 0

#define TABLE_SIZE 50000000
#define NUMBER_OF_READER_THREADS 3
#define MURMUR_SEED 0x9747b28c
#define NUMBER_OF_WRITER_THREADS_PER_QUEUE 2

#define ALPHABET_START_CHAR 'a'
#define ALPHABET_END_CHAR 'z'
#define ALPHABET_START_INT 97
#define ALPHABET_END_INT 122
#define ALPHABET_SIZE 26
#define ALPHABET_SMALL_DRIFT 97
#define ALPHABET_CAPITAL_DRIFT 32
#define NUMBER_OF_PARTITIONS (ALPHABET_SIZE + 1)

#define MAX_BUFFER_SIZE 1024
#define DEFAULT_CHUNK_SIZE 200 * 1024 * 1024

typedef struct Node {
    void *buffer_start;
    struct Node *next;
} Node;
typedef struct Queue {
    u_int8_t id;
    Node *front;
    Node *rear;
} Queue;

Queue *file_queues[NUMBER_OF_PARTITIONS];

typedef struct Station {
    char name[50];
    float average_temp;
    float min_temp;
    float max_temp;
    float median_temp;
    unsigned int count;
} Station;

typedef struct Entry {
    char *key;
    Station *value;
    struct Entry *next;
} Entry;

typedef struct HashTable {
    Entry **entries;
} HashTable;

HashTable *tables[NUMBER_OF_PARTITIONS];

typedef struct reader_thread_data {
    int thread_id;
    FILE *file;
    size_t file_size;
    int *file_read_count;
    int *finished_reader_threads;
    int *file_mmap_offset;
} reader_thread_data;

typedef struct writer_thread_data {
    int thread_id;
    char queue_letter;
    HashTable *table;
    int *finished_reader_threads;
} writer_thread_data;

pthread_mutex_t file_semaphore = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t read_queue_exit_count_semaphore = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t file_mmap_offset_semaphore = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t table_semaphores[NUMBER_OF_PARTITIONS];
pthread_mutex_t file_queue_semaphores[NUMBER_OF_PARTITIONS];

void initialize_semaphores() {
    for (int i = 0; i < NUMBER_OF_PARTITIONS; i++) {
        pthread_mutex_init(&table_semaphores[i], NULL);
        pthread_mutex_init(&file_queue_semaphores[i], NULL);
    }
}

void destroy_semaphores() {
    for (int i = 0; i < NUMBER_OF_PARTITIONS; i++) {
        pthread_mutex_destroy(&table_semaphores[i]);
        pthread_mutex_destroy(&file_queue_semaphores[i]);
    }
}

void initialize_queues() {
    for (int i = 0; i < NUMBER_OF_PARTITIONS; i++) {
        file_queues[i] = malloc(sizeof(Queue));
        file_queues[i]->id = i;
        file_queues[i]->front = NULL;
        file_queues[i]->rear = NULL;
    }
}

void destroy_queues() {
    for (int i = 0; i < NUMBER_OF_PARTITIONS; i++) {
        free(file_queues[i]);
    }
}

enum {
    THREAD_TYPE_FILE_READER,
    THREAD_TYPE_TABLE_WRITER,
    THREAD_TYPE_MAIN,
} thread_type;

char *get_thread_type_name(int thread_type) {
    switch (thread_type) {
    case THREAD_TYPE_FILE_READER:
        return "FileReaderThread";
    case THREAD_TYPE_TABLE_WRITER:
        return "TableWriterThread";
    case THREAD_TYPE_MAIN:
        return "MainThread";
    default:
        return "UnknownThread";
    }
}

enum {
    SEMAPHORE_TYPE_FILE,
    SEMAPHORE_TYPE_TABLE,
    SEMAPHORE_TYPE_READ_QUEUE,
    SEMAPHORE_TYPE_WRITE_QUEUE,

} semaphore_type;
char *get_semaphore_name(int semaphore_type) {
    switch (semaphore_type) {
    case SEMAPHORE_TYPE_FILE:
        return "file_semaphore";
    case SEMAPHORE_TYPE_TABLE:
        return "table_semaphore";
    default:
        return "unknown_semaphore";
    }
}

void enqueue(Queue *q, void *buffer_start) {
    Node *newNode = (Node *)malloc(sizeof(Node));
    newNode->buffer_start = buffer_start;
    newNode->next = NULL;
    if (q->rear == NULL) {
        q->front = q->rear = newNode;
    } else {
        q->rear->next = newNode;
        q->rear = newNode;
    }
}
void *dequeue(Queue *q) {
    if (q->front == NULL) {
        return false;
    }
    Node *temp = q->front;

    q->front = q->front->next;
    if (q->front == NULL)
        q->rear = NULL;

    return temp->buffer_start;
}

static inline uint32_t murmur_32_scramble(uint32_t k) {
    k *= 0xcc9e2d51;
    k = (k << 15) | (k >> 17);
    k *= 0x1b873593;
    return k;
}

uint32_t murmur3_32(const uint8_t *key, size_t len, uint32_t seed) {
    uint32_t h = seed;
    uint32_t k;

    for (size_t i = len >> 2; i; i--) {
        memcpy(&k, key, sizeof(uint32_t));
        key += sizeof(uint32_t);
        h ^= murmur_32_scramble(k);
        h = (h << 13) | (h >> 19);
        h = h * 5 + 0xe6546b64;
    }

    k = 0;
    for (size_t i = len & 3; i; i--) {
        k <<= 8;
        k |= key[i - 1];
    }

    h ^= murmur_32_scramble(k);
    h ^= len;
    h ^= h >> 16;
    h *= 0x85ebca6b;
    h ^= h >> 13;
    h *= 0xc2b2ae35;
    h ^= h >> 16;
    return h;
}
unsigned int hash(const char *key) {
    return murmur3_32((const uint8_t *)key, strlen(key), MURMUR_SEED) %
           TABLE_SIZE;
}

HashTable *create_table() {
    HashTable *table = malloc(sizeof(HashTable));
    table->entries = calloc(TABLE_SIZE, sizeof(Entry *));
    return table;
}
void free_table(HashTable *table) {

    for (int i = 0; i < TABLE_SIZE; i++) {
        Entry *entry = table->entries[i];
        while (entry != NULL) {
            Entry *next = entry->next;
            free(entry->key);
            free(entry);
            entry = next;
        }
    }
    free(table->entries);
    free(table);
}

void ht_set(HashTable *table, const char *key, Station *value) {
    unsigned int index = hash(key);
    Entry *entry = table->entries[index];

    for (Entry *e = entry; e != NULL; e = e->next) {
        if (strcmp(e->key, key) == 0) {
            e->value = value;
            return;
        }
    }

    Entry *new_entry = malloc(sizeof(Entry));
    new_entry->key = strdup(key);
    new_entry->value = value;
    new_entry->next = entry;
    table->entries[index] = new_entry;
}
Station *ht_get(HashTable *table, const char *key) {
    unsigned int index = hash(key);
    Entry *entry = table->entries[index];
    while (entry != NULL) {
        if (strcmp(entry->key, key) == 0) {
            return entry->value;
        }
        entry = entry->next;
    }
    return NULL;
}

void initialize_hash_tables() {
    for (int i = 0; i < NUMBER_OF_PARTITIONS; i++) {
        tables[i] = create_table();
    }
}

float calculate_average(int count, float average, float new_value) {
    return ((average * count) + new_value) / (count + 1);
}
float return_max(float a, float b) { return (a > b) ? a : b; }
float return_min(float a, float b) { return (a < b) ? a : b; }

int index_by_alphabet(char letter) {
    int letter_int = (int)letter;
    if (letter_int >= ALPHABET_START_INT && letter_int <= ALPHABET_END_INT) {
        return letter_int - ALPHABET_START_INT;
    }
    if (letter_int >= (ALPHABET_START_INT - ALPHABET_CAPITAL_DRIFT) &&
        letter_int <= (ALPHABET_END_INT - ALPHABET_CAPITAL_DRIFT)) {
        return letter_int - (ALPHABET_START_INT - ALPHABET_CAPITAL_DRIFT);
    }
    return ALPHABET_SIZE;
}

void message(const char *message, int thread_type, int thread_id,
             int semaphore_type) {
    if (ENABLE_DEBUG_PRINTS == 0) {
        return;
    }
    printf("%s\n", message);
}

size_t get_file_size(FILE *file) {
    fseek(file, 0, SEEK_END);
    size_t size = ftell(file);
    fseek(file, 0, SEEK_SET);
    return size;
}

void *process_file_data(void *threadarg) {
    reader_thread_data *my_data = (reader_thread_data *)threadarg;

    for (;;) {
        char buffer[1024];

        pthread_mutex_lock(&file_semaphore);
        pthread_mutex_lock(&file_mmap_offset_semaphore);

        size_t bytes_to_map = DEFAULT_CHUNK_SIZE;

        size_t offset = *(my_data->file_mmap_offset);
        if (offset >= my_data->file_size) {

            pthread_mutex_unlock(&file_mmap_offset_semaphore);
            pthread_mutex_unlock(&file_semaphore);

            pthread_mutex_lock(&read_queue_exit_count_semaphore);
            *my_data->finished_reader_threads += 1;
            pthread_mutex_unlock(&read_queue_exit_count_semaphore);

            pthread_exit(NULL);
        }

        if (offset + bytes_to_map > my_data->file_size) {
            bytes_to_map = my_data->file_size - offset;
        }
        void *file_memory = mmap(NULL, bytes_to_map, PROT_READ, MAP_PRIVATE,
                                 fileno(my_data->file), offset);

        if (file_memory == MAP_FAILED) {
            perror("mmap failed, killing process");
            exit(EXIT_FAILURE);
        }

        *my_data->file_read_count += 1;
        *(my_data->file_mmap_offset) += DEFAULT_CHUNK_SIZE;

        pthread_mutex_lock(&file_mmap_offset_semaphore);
        pthread_mutex_unlock(&file_semaphore);

        int queue_index = index_by_alphabet(buffer[0]);

        pthread_mutex_lock(&file_queue_semaphores[queue_index]);
        enqueue(file_queues[queue_index], buffer);
        pthread_mutex_unlock(&file_queue_semaphores[queue_index]);
    }
    pthread_exit(NULL);
}

void *insert_data_into_table(void *arg) {

    writer_thread_data *my_data = (writer_thread_data *)arg;

    for (;;) {
        char buffer[1024];

        pthread_mutex_lock(
            &file_queue_semaphores[index_by_alphabet(my_data->queue_letter)]);

        void *buffer_start =
            dequeue(file_queues[index_by_alphabet(my_data->queue_letter)]);
        pthread_mutex_unlock(
            &file_queue_semaphores[index_by_alphabet(my_data->queue_letter)]);

        pthread_mutex_lock(&read_queue_exit_count_semaphore);

        if (*my_data->finished_reader_threads == NUMBER_OF_READER_THREADS) {

            pthread_mutex_unlock(&read_queue_exit_count_semaphore);
            pthread_exit(NULL);
        }

        pthread_mutex_unlock(&read_queue_exit_count_semaphore);

        usleep(10000);
        continue;

        pthread_mutex_unlock(
            &file_queue_semaphores[index_by_alphabet(my_data->queue_letter)]);

        char *station_name = strtok(buffer_start, ";");

        if (station_name == NULL) {
            continue;
        }

        char *temperature_str = strtok(NULL, "\n");

        if (temperature_str == NULL) {
            continue;
        }
        float temperature = atof(temperature_str);

        pthread_mutex_lock(
            &table_semaphores[index_by_alphabet(my_data->queue_letter)]);
        Station *existing_station = ht_get(my_data->table, station_name);

        if (existing_station != NULL) {
            existing_station->average_temp =
                calculate_average(existing_station->count,
                                  existing_station->average_temp, temperature);
            existing_station->count += 1;
            existing_station->max_temp =
                return_max(existing_station->max_temp, temperature);
            existing_station->min_temp =
                return_min(existing_station->min_temp, temperature);

            pthread_mutex_unlock(
                &table_semaphores[index_by_alphabet(my_data->queue_letter)]);

            continue;
        }
        Station *s = malloc(sizeof(Station));
        strncpy(s->name, station_name, sizeof(s->name) - 1);
        s->average_temp = calculate_average(0, 0.0, temperature);
        s->min_temp = temperature;
        s->max_temp = temperature;
        s->count = 1;

        ht_set(my_data->table, station_name, s);
        pthread_mutex_unlock(
            &table_semaphores[index_by_alphabet(my_data->queue_letter)]);
    }

    pthread_exit(NULL);
}

FILE *open_file(const char *filename) {
    FILE *file = fopen(filename, "r");
    if (file == NULL) {
        perror("Error opening file");
        exit(EXIT_FAILURE);
    }
    return file;
}

// +----------------+        +--------------- -+       +------------------+
// |  Reader Thread |        |      Queue      |       |   Worker Thread  |
// +----------------+        +-----------------+       +------------------+
// | - Reads data   | -----> |- Enqueue data   | ----->| - Dequeue data   |
// | - Acquire file |        |- Uses semaphore |       | - Processes data |
// |   semaphore    |        |- Manages access |       | - Updates hash   |
// | - Release file |        |   table         |       |   table          |
// |   semaphore    |        |                 |       |                  |
// +----------------+        +-----------------+       +------------------+
//

int main(void) {
    FILE *file = open_file("measurements.txt");

    size_t file_size = get_file_size(file);
    printf("File size: %llu bytes\n", (u_int64_t)file_size);

    initialize_semaphores();
    initialize_queues();
    initialize_hash_tables();

    int reader_threads_ids[NUMBER_OF_READER_THREADS];
    pthread_t reader_threads[NUMBER_OF_READER_THREADS];
    struct reader_thread_data reader_thread_data[NUMBER_OF_READER_THREADS];

    int read_rc;
    int file_read_count = 0;
    int finished_reader_threads = 0;
    int file_mmap_offset = 0;

    for (int i = 0; i < NUMBER_OF_READER_THREADS; i++) {
        reader_thread_data[i].file_read_count = &file_read_count;
        reader_thread_data[i].finished_reader_threads =
            &finished_reader_threads;

        reader_thread_data[i].thread_id = i;
        reader_thread_data[i].file = file;
        reader_thread_data[i].file_size = file_size;
        reader_thread_data[i].file_mmap_offset = &file_mmap_offset;

        read_rc = pthread_create(&reader_threads[i], NULL, process_file_data,
                                 (void *)&reader_thread_data[i]);
        if (read_rc) {
            printf("Error:unable to create thread, %d\n", read_rc);
            exit(-1);
        }

        printf("Main: Created reader thread %d\n", i);
    }

    int write_rc;
    int writer_threads_ids[NUMBER_OF_PARTITIONS *
                           NUMBER_OF_WRITER_THREADS_PER_QUEUE];
    pthread_t writer_threads[NUMBER_OF_PARTITIONS *
                             NUMBER_OF_WRITER_THREADS_PER_QUEUE];
    struct writer_thread_data
        writer_thread_data[NUMBER_OF_PARTITIONS *
                           NUMBER_OF_WRITER_THREADS_PER_QUEUE];

    for (int c = 0; c < NUMBER_OF_PARTITIONS; c++) {
        for (int i = 0; i < NUMBER_OF_WRITER_THREADS_PER_QUEUE; i++) {
            /* Create writer threads per queue */
            writer_thread_data[c * NUMBER_OF_WRITER_THREADS_PER_QUEUE + i]
                .thread_id = c * NUMBER_OF_WRITER_THREADS_PER_QUEUE + i;
            writer_thread_data[c * NUMBER_OF_WRITER_THREADS_PER_QUEUE + i]
                .queue_letter = (char)(c + ALPHABET_START_CHAR);
            writer_thread_data[c * NUMBER_OF_WRITER_THREADS_PER_QUEUE + i]
                .table = tables[c];
            writer_thread_data[c * NUMBER_OF_WRITER_THREADS_PER_QUEUE + i]
                .finished_reader_threads = &finished_reader_threads;

            write_rc = pthread_create(
                &writer_threads[c * NUMBER_OF_WRITER_THREADS_PER_QUEUE + i],
                NULL, insert_data_into_table,
                (void *)&writer_thread_data
                    [c * NUMBER_OF_WRITER_THREADS_PER_QUEUE + i]);

            if (write_rc) {
                printf("Error:unable to create thread, %d\n", write_rc);
                exit(-1);
            }

            /* printf("Main: Created writer thread %d for queue letter %c\n", c,
             */
            /*        c + ALPHABET_START_CHAR); */
        }
    }

    bool continue_waiting = true;
    void *ret;
    for (int i = 0; i < NUMBER_OF_READER_THREADS; i++) {
        if (pthread_join(reader_threads[i], &ret) != 0) {
            printf("ERROR : pthread join failed.\n");
            return (0);
        }
        /* printf("Main: Joined reader thread %d\n", i); */
    }

    for (int i = 0;
         i < NUMBER_OF_PARTITIONS * NUMBER_OF_WRITER_THREADS_PER_QUEUE; i++) {
        if (pthread_join(writer_threads[i], &ret) != 0) {
            printf("ERROR : pthread join failed.\n");
            return (0);
        }

        /* printf("Main: Joined Writer thread %d\n", i); */
    }

    printf("Final Station Data:\n");
    exit(0);

    for (int t = 0; t < NUMBER_OF_PARTITIONS; t++) {
        HashTable *table = tables[t];

        for (int i = 0; i < TABLE_SIZE; i++) {
            Entry *entry = table->entries[i];
            while (entry != NULL) {
                Station *s = entry->value;
                printf("Station: %s, Avg Temp: %.2f, Min Temp: %.2f, Max Temp: "
                       "%.2f, Count: %u\n",
                       s->name, s->average_temp, s->min_temp, s->max_temp,
                       s->count);
                entry = entry->next; // advance
            }
        }

        free_table(table);
    }

    fclose(file);
    return 0;
}
