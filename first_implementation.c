#include <assert.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define TABLE_SIZE 50000000

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

unsigned int hash(const char *key) {
    unsigned int hash = 5381;
    int c;
    while ((c = *key++)) {
        hash = ((hash << 5) + hash) + c;
    }
    return hash % TABLE_SIZE;
}

HashTable *create_table() {
    HashTable *table = malloc(sizeof(HashTable));
    table->entries = malloc(sizeof(Entry *) * TABLE_SIZE);
    for (int i = 0; i < TABLE_SIZE; i++) {
        table->entries[i] = NULL;
    }
    return table;
}

void free_table(HashTable *table) {
    for (int i = 0; i < TABLE_SIZE; i++) {
        Entry *entry = table->entries[i];
        while (entry != NULL) {
            Entry *temp = entry;
            entry = entry->next;
            free(temp->key);
            free(temp);
        }
    }
    free(table->entries);
    free(table);
}

void ht_set(HashTable *table, const char *key, Station *value) {
    unsigned int index = hash(key);
    Entry *entry = table->entries[index];
    while (entry != NULL) {
        if (strcmp(entry->key, key) == 0) {
            entry->value = value;
            return;
        }
        entry = entry->next;
    }

    entry = malloc(sizeof(Entry));
    entry->key = strdup(key);
    entry->value = value;
    entry->next = table->entries[index];
    table->entries[index] = entry;
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
    return 0;
}

float calculate_average(int count, float average, float new_value) {
    return ((average * count) + new_value) / (count + 1);
}

float return_max(float a, float b) { return (a > b) ? a : b; }

float return_min(float a, float b) { return (a < b) ? a : b; }

void *process_data(void *threadarg) {

    printf("Thread processing data...\n");

    pthread_exit(NULL);
}

typedef struct thread_data {
    int thread_id;
    HashTable *table;
} thread_data;

int main(void) {
    FILE *file = fopen("measurements.txt", "r");
    if (file == NULL) {
        perror("Error opening file");
        return 1;
    }

    HashTable *table = create_table();

    char buffer[1024]; // Using a larger buffer

    // Read and print each line
    while (fgets(buffer, sizeof(buffer), file) != NULL) {
        char *station_name = strtok(buffer, ";");
        assert(station_name != NULL);

        char *temperature_str = strtok(NULL, "\n");
        assert(temperature_str != NULL);
        float temperature = atof(temperature_str);

        int station_hash = hash(station_name);

        Station *existing_station = ht_get(table, station_name);

        if (existing_station != NULL) {
            existing_station->average_temp =
                calculate_average(existing_station->count,
                                  existing_station->average_temp, temperature);
            existing_station->count += 1;
            existing_station->max_temp =
                return_max(existing_station->max_temp, temperature);
            existing_station->min_temp =
                return_min(existing_station->min_temp, temperature);
        } else {
            Station *s = malloc(sizeof(Station));
            strncpy(s->name, station_name, sizeof(s->name) - 1);
            s->average_temp = calculate_average(0, 0.0, temperature);
            s->min_temp = temperature;
            s->max_temp = temperature;
            s->count = 1;

            ht_set(table, station_name, s);
        }
    }

    exit(0);

    for (int i = 0; i < TABLE_SIZE; i++) {
        Entry *entry = table->entries[i];
        while (entry != NULL) {
            Station *s = entry->value;
            printf("Station: %s, Avg Temp: %.2f, Min Temp: %.2f, Max Temp: "
                   "%.2f, Count: %u\n",
                   s->name, s->average_temp, s->min_temp, s->max_temp,
                   s->count);
            entry = entry->next;
        }
    }

    free_table(table);
    fclose(file);
    return 0;
}
