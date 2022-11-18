/* Author: liyankun
 * Email : liyankun01@58.com,lioveni99@gmail.com
 */

#include <stdio.h>
#include <blobfs_wrapper.h>

int main(int argc, char **argv) {
        printf("start blobfs wrapper!");
	if (argc < 4) {
		fprintf(stderr, "usage: %s <conffile> <bdev name> <cache_size_in_mb>\n", argv[0]);
		exit(1);
	}

        uint64_t cache_size = (uint64_t) atoi(argv[3]);
        int rc;
        rc = mount_blobfs(argv[1], argv[2], cache_size);
        if (rc != 0) {
                fprintf(stderr, "mount_blobfs conf(%s) bdev name(%s) cache size(%ld)\n", argv[1], argv[2], cache_size);
                return rc;
        }
        fprintf(stdout, "blobfs mount successfully!!\n");

        char *filename = "blobfs-test";
        blobfs_file_stat *file_stat;
        allocate_blobfs_file_stat(&file_stat);
        fprintf(stdout, "blobfs stat file %s\n", filename);
        rc = blobfs_file_stat_f(filename, file_stat);
        if (rc != 0) {
                fprintf(stdout, "blobfs: file %s is not exist,  to create file %s\n", filename, filename);
                rc = blobfs_create_file(filename);
                if (rc != 0) {
                        fprintf(stderr, "ERR: blobfs create file %s\n", filename);
                        goto exit;
                }

                fprintf(stdout, "blobfs: to stat new create file %s\n", filename);
                rc = blobfs_file_stat_f(filename, file_stat);
                if (rc != 0) {
                  fprintf(stderr, "ERR: blobfs stat file %s\n", filename);
                  goto exit;
                }
        }

        blobfs_file *wfile = NULL;

        fprintf(stdout, "blobfs: to open file %s for write\n", filename);
        rc = blobfs_open_file(filename, SPDK_BLOBFS_OPEN_CREATE, &wfile);
        if (rc != 0) {
                fprintf(stderr, "ERR: blobfs open file %s rc %d\n", filename, rc);
                goto exit;
        }

        fprintf(stdout, "blobfs: to write data to file %s\n", filename);
        rc = blobfs_file_write(wfile, "hello world", file_stat->s_size, 12);
        if (rc != 0) {
                fprintf(stderr, "ERR: blobfs write file %s\n", filename);
                goto close_wfile;
        }
        file_stat->s_size += 12;

        fprintf(stdout, "blobfs: to sync file %s\n", filename);
        rc = blobfs_file_sync(wfile);
        if (rc != 0) {
                fprintf(stderr, "ERR: blobfs sync file %s\n", filename);
                goto close_wfile;
        }

        fprintf(stdout, "blobfs: to sync file %s again\n", filename);
        rc = blobfs_file_sync(wfile);
        if (rc != 0) {
                fprintf(stderr, "ERR: blobfs sync file %s\n", filename);
                goto close_wfile;
        }

        blobfs_file *rfile = NULL;

        fprintf(stdout, "blobfs: to open file %s for read\n", filename);
        rc = blobfs_open_file(filename, SPDK_BLOBFS_OPEN_CREATE, &rfile);
        if (rc != 0) {
                fprintf(stderr, "ERR: blobfs open file %s rc %d\n", filename, rc);
                goto close_wfile;
        }

        void *data = malloc(file_stat->s_size);
        int64_t end_off;
        end_off = blobfs_file_read(rfile, data, 0, file_stat->s_size);
        if (end_off < 0) {
                fprintf(stderr, "ERR: blobfs read file %s rc %d\n", filename, rc);
                goto close_file;
        }
        fprintf(stdout, "blobfs read data[ %s ] from %s\n", ((char *)data), filename);

close_file:
        fprintf(stdout, "blobfs: to close file %s for read\n", filename);
        rc = blobfs_file_close(rfile);
        if (rc != 0) {
                fprintf(stderr, "ERR: blobfs close read file %s\n", filename);
                goto exit;
        }
        rfile = NULL;
close_wfile:
        fprintf(stdout, "blobfs: to close file %s for write\n", filename);
        rc = blobfs_file_close(wfile);
        if (rc != 0) {
                fprintf(stderr, "ERR: blobfs close write file %s\n", filename);
                goto exit;
        }
        wfile = NULL;
        fprintf(stdout, "blobfs: to close file %s for write again\n", filename);
        rc = blobfs_file_close(wfile);
        if (rc != 0) {
                fprintf(stderr, "ERR: blobfs close write file %s\n", filename);
                goto exit;
        }
        //do something for file
        //
        //
        rc = blobfs_delete_file(filename);
        if (rc != 0) {
                fprintf(stderr, "ERR: blobfs delete file %s\n", filename);
                goto exit;
        }
exit:
        free_blobfs_file_stat(file_stat);
        file_stat = NULL;
        unmount_blobfs();
        fprintf(stdout, "blobfs exit!!\n");

        return 0;
}
