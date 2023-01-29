/* Author: liyankun
 * Email : liyankun01@58.com,lioveni99@gmail.com
 */

#define _GNU_SOURCE
#include <stdio.h>
#include <blobfs_wrapper.h>
#include <sys/timeb.h>
#include <sys/time.h>
//#include <pthread.h>
//#include <unistd.h>
//#include <sched.h>

#define US 1L
#define MS (1000*US)
#define S (1000*MS)

#define KB 1024L
#define MB (1024*KB)
#define GB (1024*MB)

struct ctxval {
  pthread_t pid;
  char     *mode;
  char     *filename;
  int      chunksize; //unit KB
  uint64_t benchsize; //unit GB
  uint64_t times;
  int      ret;
  int      core;
  void (*exit)(void);
};

enum OP {
  WRITE=1, READ, CLEAR
};

void *benchwrite(void *val) {
       // cpu_set_t mask;
       // CPU_ZERO(&mask);
       // CPU_SET(1, &mask); //指定该线程使用的CPU
       // if (pthread_setaffinity_np(pthread_self(), sizeof(mask), &mask) < 0) {
       //         perror("pthread_setaffinity_np");
       //         goto exit;
       // }

        struct ctxval *ctx = (struct ctxval *) val;
        blobfs_file *wfile = NULL;
	char *filename = ctx->filename;
        //fprintf(stdout, "filename: %s\n", filename);
        int rc = blobfs_open_file(filename, SPDK_BLOBFS_OPEN_CREATE, &wfile);
        if (rc != 0) {
		fprintf(stderr, "ERR: blobfs open file %s, code: %d\n", filename, rc);
                ctx->ret = -1;
                goto exit;
	}

        uint64_t bench_size = ctx->benchsize;
        uint64_t chunk_size = ctx->chunksize;
        char *data = (char *) malloc(chunk_size);
	int i;
	for (i = 0; i < chunk_size; i++) {
		data[i] = '1';
	}

        uint64_t times = 0;
	long long int total;
	for (total = 0; total < bench_size;) {
                rc = blobfs_file_write(wfile, data, total, chunk_size);
                if (rc != 0) {
                        fprintf(stderr, "ERR: blobfs write file %s, code: %d\n", filename, rc);
                        ctx->ret = -1;
                        goto exit;
                }
                times++;
		total += chunk_size;
                rc = blobfs_file_sync(wfile);
                if (rc != 0) {
                        fprintf(stderr, "ERR: blobfs sync file %s\n", filename);
                        ctx->ret = -1;
                        goto exit;
	        }
                if (ctx->core == 1) {
                 // fprintf(stdout, "writed size: %f\n", 0.1*total/bench_size *100);
                }
                //usleep(2500);
	}
        rc = blobfs_file_close(wfile);
        if (rc != 0) {
                fprintf(stderr, "ERR: blobfs close read file %s\n", filename);
                ctx->ret = -1;
                goto exit;
        }
        ctx->times = times;
exit:
        free((void*)data);
        if (ctx->exit != NULL) {
          ctx->exit();
        }
        pthread_exit(NULL);
        return NULL;
}

void *benchread(void *val) {
       // fprintf(stdout, "Start benchread thread\n");
        struct ctxval *ctx = (struct ctxval *) val;
	char *filename = ctx->filename;
        blobfs_file *rfile = NULL;
        uint64_t chunk_size = ctx->chunksize;
        char *buf = (char *) malloc(chunk_size);
        int rc = blobfs_open_file(filename, SPDK_BLOBFS_OPEN_CREATE, &rfile);
        if (rc != 0) {
		fprintf(stderr, "ERR: blobfs open file %s\n", filename);
		goto readout;
	}

        uint64_t times = 0;
        int64_t filesize = blobfs_file_get_length(rfile);
        int64_t offset = 0;
        uint64_t read_len = 0;
        struct timeval startTime, interval; // 20s
        gettimeofday(&startTime, NULL);
        int bench_interval = 0;
        if (strcmp(ctx->mode, "seqread") == 0) {
                do {
                       read_len = blobfs_file_read(rfile, buf, offset, chunk_size);
                       times++;
        //               fprintf(stdout, "read size: %d read offset: %ld filesize: %ld\n", read_len, offset, filesize);
                       offset += read_len;
                } while (filesize != offset);
        } else if (strcmp(ctx->mode, "randread") == 0) {
                do {
                       offset = rand()%(BENCH_SIZE*GB);
        //               fprintf(stdout, "offset: %d", offset);
                       read_len = blobfs_file_read(rfile, buf, offset, chunk_size);
                       times++;
                       //fprintf(stdout, "read size: %d read offset: %ld filesize: %ld\n", read_len, offset, filesize);
                       if (bench_interval > 10000) {
                               gettimeofday(&interval, NULL);
                               if (interval.tv_sec - startTime.tv_sec > 20) {
                                 break;
                               }
                               bench_interval = 0;
                       }
                       bench_interval++;
                } while (1);
        } else {
                fprintf(stdout, "Unknow read mode: %s\n", ctx->mode);
                goto readout;
        }
        rc = blobfs_file_close(rfile);
        if (rc != 0) {
                fprintf(stderr, "ERR: blobfs close read file %s\n", filename);
		goto readout;
        }
        ctx->times = times;
readout:
        free((void*)buf);
        if (ctx->exit != NULL) {
          ctx->exit();
        }
        pthread_exit(NULL);
}
void benchctrl(char * mode, uint64_t chunksize, int threads) {
      //threads += 10+10+10;
      struct timeval startTime, endTime;
      uint64_t bench_size = BENCH_SIZE;
      struct ctxval *ctxs = (struct ctxval *)malloc(sizeof(struct ctxval) * threads);
      int idx = 0;
      char *prefix =  "testfilename-";
      for (idx = 0; idx < threads; idx++) {
	      ctxs[idx].filename = (char *) malloc(125);
              sprintf(ctxs[idx].filename, "%s%d", prefix, idx);
              ctxs[idx].chunksize = chunksize * KB;
              ctxs[idx].benchsize = bench_size * GB;
              //ctxs[idx].exit = NULL; //work_thread_exit;
              ctxs[idx].exit = work_thread_exit;
              ctxs[idx].mode = mode;
              ctxs[idx].core = 1 + idx;
      }
      int rc = 0;
      void* (*work)(void*val);
      if (strcmp(mode, "seqwrite") == 0) {
        work = benchwrite;
      } else if (strcmp(mode, "seqread")) {
        work = benchread;
      } else if (strcmp(mode, "randread")) {
        work = benchread;
      } else {
        fprintf(stdout, "Unknow op %s\n", mode);
        return;
      }
      gettimeofday(&startTime, NULL);
      for (idx = 0; idx < threads; idx++) {
              rc = pthread_create(&ctxs[idx].pid, NULL, work, (void *)&ctxs[idx]);
              if (rc != 0) {
                       fprintf(stdout, "Start thread idx %d fail\n", rc);
                       goto writeout;
              } else {
     //                  fprintf(stdout, "Start thread idx %d successfully, pid: %ld\n",idx, ctxs[idx].pid);
              }
      }

      for (idx = 0; idx < threads; idx++) {
              fprintf(stdout, "To join thread: %ld\n", ctxs[idx].pid);
              pthread_join(ctxs[idx].pid, NULL);
             // fprintf(stdout, "Thread idx %d retcode: %d", idx, *retthread);
      }
      gettimeofday(&endTime, NULL);
      float total_escaped = (endTime.tv_sec - startTime.tv_sec) * S + (endTime.tv_usec - startTime.tv_usec);

      uint64_t total_bench_size = 0;
      uint64_t total_times = 0;
      for (idx = 0; idx < threads; idx++) {
              if (ctxs[idx].ret < 0) {
                      goto writeout;
              }
              fprintf(stdout, "idx: %d bench size: %ld times: %d\n", idx, ctxs[idx].benchsize, ctxs[idx].times);
              total_bench_size += ctxs[idx].benchsize;
              total_times += ctxs[idx].times;
              free((void*)ctxs[idx].filename);
      }
      fprintf(stdout, "Mode: %s\nData size: %ld GB file\nThreads: %d\nChunk size: %ld KB\nEscaped time: %f ms\nWrite times: %ld\nIOPS: %f\nlatency: %f us\nOutput: %f MB\n",
      mode, total_bench_size/GB, threads, chunksize, total_escaped/MS, total_times, total_times/(total_escaped/S), total_escaped/total_times,
      total_bench_size/(total_escaped/S)/MB);
writeout:
      unmount_blobfs();
      exit(0);
}


int main(int argc, char **argv) {
        int64_t bench_size = BENCH_SIZE; //unit GB
	struct timeval startTime , endTime;
        printf("start blobfs wrapper!");
	if (argc < 4) {
		fprintf(stderr, "usage: %s <conffile> <bdev name> <cache_size_in_mb>\n", argv[0]);
		exit(1);
	}

        fprintf(stdout, "conf: %s bdev %s cache: %s mode: %s threads: %s chunk size: %s\n",
            argv[1], argv[2], argv[3], argv[4], argv[5], argv[6]);
        uint64_t cache_size = (uint64_t) atoi(argv[3]);
        int rc;
        rc = mount_blobfs(argv[1], argv[2], cache_size);
        if (rc != 0) {
                fprintf(stderr, "mount_blobfs conf(%s) bdev name(%s) cache size(%ld)\n", argv[1], argv[2], cache_size);
                return rc;
        }
        fprintf(stdout, "blobfs mount successfully!!\n");

	gettimeofday(&startTime, NULL);
        char *mode = argv[4];
        if (strcmp(mode, "clear") != 0 && strcmp(mode, "list") != 0) {
                int threads = atoi(argv[5]);;
                int chunksize = atoi(argv[6]);;
                benchctrl(mode, chunksize, threads);
                exit(0);
        }

	static const int size = 1024*4;

	if (strcmp(argv[4], "list") == 0) {
               blobfs_file_stat *file_stat;
               allocate_blobfs_file_stat(&file_stat);
                blobfs_file_name *all_files = NULL;
                rc = blobfs_list_all_files(&all_files);
                if (rc != 0) {
                        fprintf(stderr, "ERR: blobfs list all file \n");
                        goto listexit;
                }
                blobfs_file_name_ptr it = all_files;
                fprintf(stdout, "blobfs: files:\n");
                while(it != NULL) {
                        blobfs_file_stat_f(it->name, file_stat);
                        fprintf(stdout, "file: %s  size: %ld \n", it->name, file_stat->s_size);
                        it = it->next;
                }
listexit:
                free_blobfs_file_name(all_files);
                fprintf(stdout, "\n");
                unmount_blobfs();
                exit(0);
	}

	if (strcmp(argv[4], "clear") == 0) {
                blobfs_file_name *all_files = NULL;
                rc = blobfs_list_all_files(&all_files);
                if (rc != 0) {
                        fprintf(stderr, "ERR: blobfs list all file. rc: %d\n", rc);
                        exit(-1);
                }
                blobfs_file_name_ptr it = all_files;
                fprintf(stdout, "blobfs: files:\n");
                int id = 0;
              //  for (;id<57;id++) {
	      //          char *name = (char *) malloc(125);
	      //          char *prefix =  "testfilename-";
              //          sprintf(name, "%s%d", prefix, id);
              //          fprintf(stdout, "To clear file %s\n", name);
              //          blobfs_delete_file(name);
              //  }
                while(it != NULL) {
                        fprintf(stdout, "To clear file %s\n", it->name);
                        rc = blobfs_delete_file(it->name);
                        if (rc != 0) {
                                fprintf(stderr, "ERR: blobfs delete file %d\n", rc);
			        exit(-1);
                        }
                        it = it->next;
                }
                free_blobfs_file_name(all_files);
                fprintf(stdout, "\n");
                unmount_blobfs();
                exit(0);
	}

exit:
        unmount_blobfs();
        fprintf(stdout, "blobfs: exit!!\n");

        return 0;
}
