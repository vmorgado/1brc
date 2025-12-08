#include <assert.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define TABLE_SIZE 50000000
#define NUMBER_OF_THREADS 16

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

typedef struct thread_data {
    int thread_id;
    HashTable *table;
    FILE *file;
    int *file_read_count;
} thread_data;

pthread_mutex_t file_semaphore = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t table_semaphore = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t print_semaphore = PTHREAD_MUTEX_INITIALIZER;

enum {
    SEMAPHORE_TYPE_FILE,
    SEMAPHORE_TYPE_TABLE,
    SEMAPHORE_TYPE_PRINT

} semaphore_type;

char *get_semaphore_name(int semaphore_type) {
    switch (semaphore_type) {
    case SEMAPHORE_TYPE_FILE:
        return "file_semaphore";
    case SEMAPHORE_TYPE_TABLE:
        return "table_semaphore";
    case SEMAPHORE_TYPE_PRINT:
        return "print_semaphore";
    default:
        return "unknown_semaphore";
    }
}

unsigned int hash(const char *key) {
    unsigned int hash = 5381;
    int c;
    while ((c = *key++)) {
        hash = ((hash << 5) + hash) + c; // hash * 33 + c
    }
    return hash % TABLE_SIZE;
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

float calculate_average(int count, float average, float new_value) {
    return ((average * count) + new_value) / (count + 1);
}

float return_max(float a, float b) { return (a > b) ? a : b; }

float return_min(float a, float b) { return (a < b) ? a : b; }

void *process_data(void *threadarg) {
    thread_data *my_data = (thread_data *)threadarg;

    for (;;) {
        char buffer[1024];

        pthread_mutex_lock(&file_semaphore);
        printf("[Thread %d] Acquired file semaphore.\n", my_data->thread_id);
        char *res = fgets(buffer, sizeof(buffer), my_data->file);
        if (res == NULL) {
            pthread_mutex_unlock(&file_semaphore);
            pthread_exit(NULL);
        }

        *my_data->file_read_count += 1;

        printf("[Thread %d] Read file times: %d.\n", my_data->thread_id,
               *my_data->file_read_count);

        printf("[Thread %d] Releasimg file semaphore.\n", my_data->thread_id);
        pthread_mutex_unlock(&file_semaphore);

        char *station_name = strtok(buffer, ";");
        if (station_name == NULL) {
            printf("[Thread %d] Malformed line, skipping.\n",
                   my_data->thread_id);
            continue;
        }

        char *temperature_str = strtok(NULL, "\n");

        if (temperature_str == NULL) {
            printf("[Thread %d] Malformed line, skipping.\n",
                   my_data->thread_id);
            continue;
        }
        float temperature = atof(temperature_str);

        pthread_mutex_lock(&table_semaphore);
        printf("[Thread %d] Acquired table semaphore.\n", my_data->thread_id);

        Station *existing_station = ht_get(my_data->table, station_name);

        if (existing_station != NULL) {

            printf("[Thread %d] Found station: %s.\n", my_data->thread_id,
                   existing_station->name);

            existing_station->average_temp =
                calculate_average(existing_station->count,
                                  existing_station->average_temp, temperature);
            existing_station->count += 1;
            existing_station->max_temp =
                return_max(existing_station->max_temp, temperature);
            existing_station->min_temp =
                return_min(existing_station->min_temp, temperature);

        } else {

            printf("[Thread %d] Didn't Found station: %s. Creating new.\n",
                   my_data->thread_id, station_name);

            Station *s = malloc(sizeof(Station));
            strncpy(s->name, station_name, sizeof(s->name) - 1);
            s->average_temp = calculate_average(0, 0.0, temperature);
            s->min_temp = temperature;
            s->max_temp = temperature;
            s->count = 1;

            ht_set(my_data->table, station_name, s);
        }

        printf("[Thread %d] Releasing table semaphore.\n", my_data->thread_id);
        pthread_mutex_unlock(&table_semaphore);
    }

    printf("[Thread %d] Exiting through the end\n", my_data->thread_id);

    pthread_exit(NULL);
}

int main(void) {
    FILE *file = fopen("measurements.txt", "r");
    if (file == NULL) {
        perror("Error opening file");
        return 1;
    }

    HashTable *table = create_table();
    int ids[NUMBER_OF_THREADS];

    pthread_t threads[NUMBER_OF_THREADS];

    struct thread_data td[NUMBER_OF_THREADS];
    int rc, i;
    int file_read_count = 0;

    for (i = 0; i < NUMBER_OF_THREADS; i++) {
        td[i].file_read_count = &file_read_count;
        td[i].thread_id = i;
        td[i].table = table;
        td[i].file = file;
        rc = pthread_create(&threads[i], NULL, process_data, (void *)&td[i]);
        if (rc) {
            printf("Error:unable to create thread, %d\n", rc);
            exit(-1);
        }
    }

    bool continue_waiting = true;
    void *ret;
    for (i = 0; i < NUMBER_OF_THREADS; i++) {
        if (pthread_join(threads[i], &ret) != 0) {
            printf("ERROR : pthread join failed.\n");
            return (0);
        }

        printf("Main: Completed join with thread %d\n", i);
    }

    printf("Final Station Data:\n");

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
    fclose(file);
    return 0;
}
