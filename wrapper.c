/* Author: liyankun
 * Email : liyankun01@58.com,lioveni99@gmail.com
 */

#include <stdio.h>
#include <blobfs_wrapper.h>
#include <sys/timeb.h>
#include <sys/time.h>

#define US 1L
#define MS (1000*US)
#define S (1000*MS)

#define KB 1024L
#define MB (1024*KB)
#define GB (1024*MB)

int main(int argc, char **argv) {
        int64_t bench_size = BENCH_SIZE; //unit GB
	struct timeval startTime , endTime;
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

	gettimeofday(&startTime, NULL);
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
	        gettimeofday(&endTime, NULL);
		free(name);
                uint64_t escaped = (endTime.tv_sec - startTime.tv_sec) * S + (endTime.tv_usec - startTime.tv_usec);
	        fprintf(stdout, "create 10000 file escaped: %d ms\n", escaped/MS);
                unmount_blobfs();
		exit(0);
	}

	gettimeofday(&startTime, NULL);
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
                gettimeofday(&endTime, NULL);
		free(name);
                uint64_t escaped = (endTime.tv_sec - startTime.tv_sec) * S + (endTime.tv_usec - startTime.tv_usec);
                fprintf(stdout, "del 10000 file escaped: %d ms\n", escaped/MS);
                unmount_blobfs();
                exit(0);
        }

	static const int size = 1024*4;

	if (strcmp(argv[4], "write") == 0) {
                blobfs_file *wfile = NULL;
		char *filename = "write_data";
                rc = blobfs_open_file(filename, SPDK_BLOBFS_OPEN_CREATE, &wfile);
                if (rc != 0) {
			fprintf(stderr, "ERR: blobfs open file %s\n", filename);
			goto writeout;
		}

                uint64_t chunk_size = (uint64_t) atol(argv[5]) * KB;
                char *data = (char *) malloc(chunk_size);
		int i;
		for (i = 0; i < chunk_size; i++) {
			data[i] = '1';
		}

	        gettimeofday(&startTime, NULL);
                uint64_t times = 0;
		long long int total;
		for (total = 0; total < bench_size * GB;) {
                        rc = blobfs_file_write(wfile, data, total, chunk_size);
                        if (rc != 0) {
                                fprintf(stderr, "ERR: blobfs write file %s\n", filename);
				goto writeout;
                        }
                        times++;
			total += chunk_size;
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
                gettimeofday(&endTime, NULL);
                float escaped = (endTime.tv_sec - startTime.tv_sec) * S + (endTime.tv_usec - startTime.tv_usec);
                fprintf(stdout, "Write: %d GB file\nChunk size: %ld \nEscaped time: %f ms\nWrite times: %ld\nIOPS: %f\nlatency: %f us\nOutput: %f MB\n",
                    bench_size, chunk_size, escaped/MS, times, times/(escaped/S), escaped/times, bench_size*GB/(escaped/S)/MB);
                //fprintf(stdout, "GB: %d MB: %d KB: %d US: %d MS: %d S: %d", GB, MB, KB, US, MS, S);
                unmount_blobfs();
                exit(0);
	}

	gettimeofday(&startTime, NULL);
	if (strcmp(argv[4], "read") == 0) {
                blobfs_file *rfile = NULL;
		char *filename = "write_data";
                uint64_t chunk_size = (uint64_t) atol(argv[5]) * KB;
                char *buf = (char *) malloc(chunk_size);
                rc = blobfs_open_file(filename, SPDK_BLOBFS_OPEN_CREATE, &rfile);
                if (rc != 0) {
			fprintf(stderr, "ERR: blobfs open file %s\n", filename);
			goto readout;
		}

                uint64_t times = 0;
                int64_t filesize = blobfs_file_get_length(rfile);
                int64_t offset = 0;
                uint64_t read_len = 0;
                do {
                       read_len = blobfs_file_read(rfile, buf, offset, 4*1024);
                       times++;
                       //fprintf(stdout, "read size: %d read offset: %ld filesize: %ld\n", read_len, offset, filesize);
                       offset += read_len;
                } while (filesize != offset);
                rc = blobfs_file_close(rfile);
                if (rc != 0) {
                        fprintf(stderr, "ERR: blobfs close read file %s\n", filename);
			goto readout;
                }
readout:

                gettimeofday(&endTime, NULL);
                float escaped = (endTime.tv_sec - startTime.tv_sec) * S + (endTime.tv_usec - startTime.tv_usec);
                fprintf(stdout, "Read: %d GB file\nChunk size: %ld \nEscaped time: %f ms\nRead times: %ld\nIOPS: %f\nlatency: %f us\nInput: %f MB\n",
                    bench_size, chunk_size, escaped/MS, times, times/(escaped/S), escaped/times, bench_size*GB/(escaped/S)/MB);
                //uint64_t escaped = (endTime.tv_sec - startTime.tv_sec) * S + (endTime.tv_usec - startTime.tv_usec);
                //fprintf(stdout, "read  %d GB chunk size: %d file escaped: %d ms\n", bench_size, size, escaped/MS );
                unmount_blobfs();
                exit(0);
	}

	if (strcmp(argv[4], "clear") == 0) {
                blobfs_file_name *all_files = NULL;
                rc = blobfs_list_all_files(&all_files);
                if (rc != 0) {
                        fprintf(stderr, "ERR: blobfs list all file \n");
                        goto clean;
                }
                blobfs_file_name_ptr it = all_files;
                fprintf(stdout, "blobfs: files: ");
                while(it != NULL) {
                        fprintf(stdout, "to clear file %s ", it->name);
                        rc = blobfs_delete_file(it->name);
                        if (rc != 0) {
                                fprintf(stderr, "ERR: blobfs delete file %s\n", argv[5]);
			        exit(-1);
                        }
                        it = it->next;
                }
                free_blobfs_file_name(all_files);
                fprintf(stdout, "\n");
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
	gettimeofday(&endTime, NULL);
        uint64_t escaped = (endTime.tv_sec - startTime.tv_sec) * S + (endTime.tv_usec - startTime.tv_usec);
	fprintf(stdout, "%d ms\n", escaped/MS);
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
	gettimeofday(&endTime, NULL);
        escaped = (endTime.tv_sec - startTime.tv_sec) * S + (endTime.tv_usec - startTime.tv_usec);
	fprintf(stdout, "%d ms\n", escaped/MS);
        unmount_blobfs();
        fprintf(stdout, "blobfs: exit!!\n");

        return 0;
}
