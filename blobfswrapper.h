
typedef struct _blobfs_file {
        struct spdk_file *s_file;
} blobfs_file;

typedef struct _blobfs_file_stat {
        struct spdk_file_stat *s_stat;
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
