/* Author: liyankun
 * Email : liyankun01@58.com,lioveni99@gmail.com
 */

#include <stdio.h>
#include <blobfs_wrapper.h>
#include <sys/timeb.h>

#define GB 1024*1024*1024L

int main(int argc, char **argv) {
	struct timeb startTime , endTime;
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

	ftime(&startTime);
	if (strcmp(argv[4], "create") == 0) {
		char *name = (char *) malloc(125);
		char *prefix =  "testfilename-";
		int idx = 0;
		for (idx = 0; idx < 10000; idx++) {
			sprintf(name, "%s%d", prefix, idx);
			rc = blobfs_create_file(name);
			if (rc != 0) {
				fprintf(stderr, "ERR: blobfs bentch create file %s\n", name);
				goto createout;
			}
		}
createout:
	        ftime(&endTime);
		free(name);
	        fprintf(stdout, "create 10000 file escaped: %d ms\n", (endTime.time-startTime.time)*1000 + (endTime.millitm - startTime.millitm));
                unmount_blobfs();
		exit(0);
	}

	ftime(&startTime);
	if (strcmp(argv[4], "del") == 0) {
                char *name = (char *) malloc(125);
                char *prefix =  "testfilename-";
                int idx = 0;
                for (idx = 0; idx < 10000; idx++) {
                        sprintf(name, "%s%d", prefix, idx);
			rc = blobfs_delete_file(name);
                        if (rc != 0) {
                                fprintf(stderr, "ERR: blobfs bentch create file %s\n", name);
				goto delout;
                        }
                }
delout:
                ftime(&endTime);
		free(name);
                fprintf(stdout, "del 10000 file escaped: %d ms\n", (endTime.time-startTime.time)*1000 + (endTime.millitm - startTime.millitm));
                unmount_blobfs();
                exit(0);
        }

	static const int size = 1024*4;

	ftime(&startTime);
	if (strcmp(argv[4], "write") == 0) {
                blobfs_file *wfile = NULL;
		char *filename = "write_data";
		char data[size];
                rc = blobfs_open_file(filename, SPDK_BLOBFS_OPEN_CREATE, &wfile);
                if (rc != 0) {
			fprintf(stderr, "ERR: blobfs open file %s\n", filename);
			goto writeout;
		}

		int i;
		for (i = 0; i < size; i++) {
			data[i] = '1';
		}

		long long int total;
		for (total = 0; total < 10 * GB;) {
                        rc = blobfs_file_write(wfile, data, total, size);
                        if (rc != 0) {
                                fprintf(stderr, "ERR: blobfs write file %s\n", filename);
				goto writeout;
                        }
			total += size;
                        rc = blobfs_file_sync(wfile);
                        if (rc != 0) {
                                fprintf(stderr, "ERR: blobfs sync file %s\n", filename);
				goto writeout;
			}
		}
                rc = blobfs_file_close(wfile);
                if (rc != 0) {
                        fprintf(stderr, "ERR: blobfs close read file %s\n", filename);
			goto writeout;
                }
writeout:
                ftime(&endTime);
                fprintf(stdout, "write 10GB file escaped: %d ms\n", (endTime.time-startTime.time)*1000 + (endTime.millitm - startTime.millitm));
                unmount_blobfs();
                exit(0);
	}

	if (strcmp(argv[4], "read") == 0) } {
                blobfs_file *wfile = NULL;
		char *filename = "write_data";
		char data[size];
                rc = blobfs_open_file(filename, SPDK_BLOBFS_OPEN_CREATE, &wfile);
                if (rc != 0) {
			fprintf(stderr, "ERR: blobfs open file %s\n", filename);
			goto readout;
		}

	}

	if (strcmp(argv[4], "clear") == 0) {
        rc = blobfs_delete_file(argv[5]);
                if (rc != 0) {
                        fprintf(stderr, "ERR: blobfs delete file %s\n", argv[5]);
			exit(-1);
                }
                unmount_blobfs();
                exit(0);
	}


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
        fprintf(stdout, "blobfs: read data[ %s ] from %s\n", ((char *)data), filename);

        fprintf(stdout, "blobfs: call get file %s id\n", filename) ;
        void *id = malloc(8);
        size_t id_length = blobfs_file_get_id(rfile, id, 8);
        fprintf(stdout, "blobfs: get file %s id is %s\n", filename, (char *)id);
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
	ftime(&endTime);
	fprintf(stdout, "%d ms\n", (endTime.time-startTime.time)*1000 + (endTime.millitm - startTime.millitm));
        int times = 9;
        while (times) {
                fprintf(stdout, "blobfs: to list all files \n");
                blobfs_file_name *all_files = NULL;
                rc = blobfs_list_all_files(&all_files);
                if (rc != 0) {
                        fprintf(stderr, "ERR: blobfs list all file \n");
                        goto clean;
                }
                blobfs_file_name_ptr it = all_files;
                fprintf(stdout, "blobfs: files: ");
                while(it != NULL) {
                        fprintf(stdout, "%s ", it->name);
                        it = it->next;
                }
                free_blobfs_file_name(all_files);
                fprintf(stdout, "\n");
                times--;
        }
        //do something for file
        //
        //
clean:
        rc = blobfs_delete_file(filename);
        if (rc != 0) {
                fprintf(stderr, "ERR: blobfs delete file %s\n", filename);
                goto exit;
        }
exit:
        free_blobfs_file_stat(file_stat);
        file_stat = NULL;
	ftime(&endTime);
	fprintf(stdout, "%d ms\n", (endTime.time-startTime.time)*1000 + (endTime.millitm - startTime.millitm));
        unmount_blobfs();
        fprintf(stdout, "blobfs: exit!!\n");

        return 0;
}
