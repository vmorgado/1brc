#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>

#define MAX_LINE_LENGTH
#define DEFAULT_CHUNK_SIZE 200 * 1024 * 1024

size_t get_file_size(FILE *file) {
    fseek(file, 0, SEEK_END);
    size_t size = ftell(file);
    fseek(file, 0, SEEK_SET);
    return size;
}
int main() {

    FILE *file = fopen("measurements.txt", "r");
    if (file == NULL) {
        perror("Error opening file");
        return EXIT_FAILURE;
    }

    size_t file_size = get_file_size(file);

    printf("File size: %llu bytes\n", (u_int64_t)file_size);

    for (size_t offset = 0; offset < file_size; offset += DEFAULT_CHUNK_SIZE) {
        size_t bytes_to_map = DEFAULT_CHUNK_SIZE;

        if (offset + bytes_to_map > file_size) {
            bytes_to_map = file_size - offset;
        }

        void *file_memory = mmap(NULL, bytes_to_map, PROT_READ, MAP_PRIVATE,
                                 fileno(file), offset);

        if (file_memory == MAP_FAILED) {
            perror("mmap failed");
            fclose(file);
            return EXIT_FAILURE;
        }

        char remainer_buffer[1000];
        int remainer_index = 0;

        for (int scan_char = bytes_to_map - 1; scan_char >= 0; scan_char--) {

            if (((char *)file_memory)[scan_char] == '\n') {
                remainer_buffer[remainer_index] = '\0';
                break;
            }

            remainer_buffer[remainer_index] = ((char *)file_memory)[scan_char];
            remainer_index++;
        }

        char *send_buffer =
            malloc(sizeof(char) * (bytes_to_map - remainer_index + 1));

        memcpy(send_buffer, file_memory, bytes_to_map - remainer_index);

        memcpy(remainer_buffer, &file_memory[bytes_to_map - remainer_index],
               remainer_index);

        size_t print_size = (bytes_to_map < 100) ? bytes_to_map : 100;
        fwrite(file_memory, 1, print_size, stdout);
        printf("\n--- End of chunk ---\n");
        printf("%s", (char *)remainer_buffer);

        munmap(file_memory, bytes_to_map);
    }
}
