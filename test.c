#include <stdio.h>
#include <stdlib.h>

#define MAX_LINE_LENGTH 1024 // maximum length of a single line

void read_lines_in_chunks(const char *filename, int chunk_size) {
    FILE *fp = fopen(filename, "r");
    if (fp == NULL) {
        perror("Error opening file");
        return;
    }

    char **lines = malloc(chunk_size * sizeof(char *));
    if (lines == NULL) {
        perror("Memory allocation failed");
        fclose(fp);
        return;
    }

    for (int i = 0; i < chunk_size; i++) {
        lines[i] = malloc(MAX_LINE_LENGTH);
        if (lines[i] == NULL) {
            perror("Memory allocation failed");
            fclose(fp);
            return;
        }
    }

    int count = 0;
    while (fgets(lines[count], MAX_LINE_LENGTH, fp)) {
        count++;
        if (count == chunk_size) {
            // Process the chunk
            printf("Processing %d lines:\n", count);
            for (int i = 0; i < count; i++) {
                printf("%s", lines[i]);
            }
            printf("\n--- End of chunk ---\n\n");
            count = 0; // reset for next chunk
        }
    }

    // Handle leftover lines
    if (count > 0) {
        printf("Processing %d leftover lines:\n", count);
        for (int i = 0; i < count; i++) {
            printf("%s", lines[i]);
        }
        printf("\n--- End of final chunk ---\n\n");
    }

    // Cleanup
    for (int i = 0; i < chunk_size; i++) {
        free(lines[i]);
    }
    free(lines);
    fclose(fp);
}

int main() {
    const char *filename = "measurements.txt";
    int chunk_size = 50000; // read 5 lines at a time
    read_lines_in_chunks(filename, chunk_size);
    return 0;
}
