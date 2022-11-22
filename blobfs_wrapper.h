/* Author: liyankun
 * Email : liyankun01@58.com,lioveni99@gmail.com
 */
#include "spdk/blobfs.h"

#ifndef BLOBFS_WRAPPER_H
#define BLOBFS_WRAPPER_H

#define EBLOBFS  -128
#define ENOFS    -129
#define ENULLPTR -129
#define EMEM     -130
#define EARGS    -131

typedef struct _blobfs_file {
        struct spdk_file *s_file;
} blobfs_file;

typedef struct _blobfs_file_stat {
        struct spdk_file_stat *s_stat;
        uint64_t s_size;
} blobfs_file_stat;

int mount_blobfs(char* spdk_conf, char *spdk_dev_name, uint64_t cache_size_in_mb);

void unmount_blobfs(void);

int blobfs_create_file(char *name);

int blobfs_open_file(char *name, uint32_t flags, blobfs_file **file);

int blobfs_file_close(blobfs_file *file);

int blobfs_delete_file(char *name);

int blobfs_rename_file(char *old_name, char *new_name);

int blobfs_file_write(blobfs_file *file, void *payload, uint64_t offset, uint64_t length);

int64_t blobfs_file_read(blobfs_file *file, void *payload, uint64_t offset, uint64_t length);

int blobfs_file_sync(blobfs_file *file);

int blobfs_file_stat_f(char *name, blobfs_file_stat *stat);

int allocate_blobfs_file_stat(blobfs_file_stat **stat);

void free_blobfs_file_stat(blobfs_file_stat *stat);

int blobfs_file_truncate(blobfs_file *file, uint64_t length);

const char * blobfs_file_get_name(blobfs_file *file);

uint64_t blobfs_file_get_length(blobfs_file *file);

int blobfs_file_get_id(blobfs_file *file, void *id, size_t size);

#endif
