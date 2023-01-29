/*   SPDX-License-Identifier: BSD-3-Clause
 *   Copyright (C) 2017 Intel Corporation.
 *   All rights reserved.
 */

#include <set>
#include <iostream>
#include <stdexcept>
#include <memory>
#include <thread>

extern "C" {
#include "spdk/env.h"
#include "spdk/event.h"
#include "spdk/blob.h"
#include "spdk/blobfs.h"
#include "spdk/blob_bdev.h"
#include "spdk/log.h"
#include "spdk/thread.h"
#include "spdk/bdev.h"
}
uint32_t chunk_size = 4*1024;
uint64_t bench_size = uint64_t(1024)*1024*BENCH_SIZE*1024;

struct spdk_filesystem *g_fs = NULL;
struct spdk_bs_dev *g_bs_dev;
uint32_t g_lcore = 0;
std::string g_bdev_name;
volatile bool g_spdk_ready = false;
volatile bool g_spdk_start_failure = false;

void SpdkInitializeThread(void);

class SpdkThreadCtx
{
public:
	struct spdk_fs_thread_ctx *channel;

	SpdkThreadCtx(void) : channel(NULL)
	{
		SpdkInitializeThread();
	}

	~SpdkThreadCtx(void)
	{
		if (channel) {
			spdk_fs_free_thread_ctx(channel);
			channel = NULL;
		}
	}

private:
	SpdkThreadCtx(const SpdkThreadCtx &);
	SpdkThreadCtx &operator=(const SpdkThreadCtx &);
};

thread_local SpdkThreadCtx g_sync_args;

static void
set_channel()
{
	if (g_fs != NULL && g_sync_args.channel == NULL) {
		g_sync_args.channel = spdk_fs_alloc_thread_ctx(g_fs);
	}
}

static void
__call_fn(void *arg1, void *arg2)
{
	fs_request_fn fn;

	fn = (fs_request_fn)arg1;
	fn(arg2);
}

static void
__send_request(fs_request_fn fn, void *arg)
{
	struct spdk_event *event;

	event = spdk_event_allocate(g_lcore, __call_fn, (void *)fn, arg);
	spdk_event_call(event);
}

class SpdkWritableFile
{
	struct spdk_file *mFile;
	uint64_t mSize;

public:
	SpdkWritableFile(struct spdk_file *file) : mFile(file), mSize(0) {}
	~SpdkWritableFile()
	{
		if (mFile != NULL) {
			Close();
		}
	}

	virtual int Close()
	{
		set_channel();
		spdk_file_close(mFile, g_sync_args.channel);
		mFile = NULL;
		return 0;
	}
	virtual int Append(const char *data);
	virtual uint64_t GetFileSize()
	{
		return mSize;
	}
	virtual int Allocate(uint64_t offset, uint64_t len)
	{
		int rc;

		set_channel();
		rc = spdk_file_truncate(mFile, g_sync_args.channel, offset + len);
		if (!rc) {
			return 0;
		} else {
			return -1;
		}
	}

	virtual int Sync()
	{
		int rc;

		set_channel();
		rc = spdk_file_sync(mFile, g_sync_args.channel);
		if (!rc) {
			return 0;
		} else {
                        return -1;
		}
	}

};

int
SpdkWritableFile::Append(const char *data)
{
	int64_t rc;

	set_channel();
        int size = chunk_size;
	rc = spdk_file_write(mFile, g_sync_args.channel, (void *)data, mSize, size);
	if (rc >= 0) {
		mSize += size;
		return 0;
	} else {
                fprintf(stdout, "append file fail, rc: %d\n", rc);
		return -1;
	}
}

class SpdkAppStartException : public std::runtime_error
{
public:
	SpdkAppStartException(std::string mess): std::runtime_error(mess) {}
};

class SpdkEnv
{
private:
	pthread_t mSpdkTid;
	std::string mConfig;
	std::string mBdev;

public:
	SpdkEnv(const std::string &conf,
		const std::string &bdev, uint64_t cache_size_in_mb);

	virtual ~SpdkEnv();

	virtual int NewWritableFile(const std::string &fname,
				       std::unique_ptr<SpdkWritableFile> *result)
	{
	       struct spdk_file *file;
	       int rc;

	       set_channel();
	       rc = spdk_fs_open_file(g_fs, g_sync_args.channel, fname.c_str(),
	       		       SPDK_BLOBFS_OPEN_CREATE, &file);
	       if (rc == 0) {
	       	result->reset(new SpdkWritableFile(file));
	       	return 0;
	       } else {
	       	return -1;
	       }
	}

	virtual int FileExists(const std::string &fname)
	{
		struct spdk_file_stat stat;
		int rc;

		set_channel();
		rc = spdk_fs_file_stat(g_fs, g_sync_args.channel, fname.c_str(), &stat);
		if (rc == 0) {
			return 0;
		}
                return -1;
	}

	virtual int GetFileSize(const std::string &fname, uint64_t *size)
	{
		struct spdk_file_stat stat;
		int rc;

		set_channel();
		rc = spdk_fs_file_stat(g_fs, g_sync_args.channel, fname.c_str(), &stat);
		if (rc == -ENOENT) {
                        return rc;
		}
		*size = stat.size;
		return 0;
	}

	virtual int DeleteFile(const std::string &fname)
	{
		int rc;

		set_channel();
		rc = spdk_fs_delete_file(g_fs, g_sync_args.channel, fname.c_str());
		if (rc == -ENOENT) {
                      return -1;
		}
		return 0;
	}
};

/* The thread local constructor doesn't work for the main thread, since
 * the filesystem hasn't been loaded yet.  So we break out this
 * SpdkInitializeThread function, so that the main thread can explicitly
 * call it after the filesystem has been loaded.
 */
void SpdkInitializeThread(void)
{
	if (g_fs != NULL) {
		if (g_sync_args.channel) {
			spdk_fs_free_thread_ctx(g_sync_args.channel);
		}
		g_sync_args.channel = spdk_fs_alloc_thread_ctx(g_fs);
	}
}

static void
fs_load_cb(__attribute__((unused)) void *ctx,
	   struct spdk_filesystem *fs, int fserrno)
{
	if (fserrno == 0) {
		g_fs = fs;
	}
	g_spdk_ready = true;
}

static void
base_bdev_event_cb(enum spdk_bdev_event_type type, __attribute__((unused)) struct spdk_bdev *bdev,
		   __attribute__((unused)) void *event_ctx)
{
	printf("Unsupported bdev event: type %d\n", type);
}

static void
rocksdb_run(__attribute__((unused)) void *arg1)
{
	int rc;

	rc = spdk_bdev_create_bs_dev_ext(g_bdev_name.c_str(), base_bdev_event_cb, NULL,
					 &g_bs_dev);
	if (rc != 0) {
		printf("Could not create blob bdev\n");
		spdk_app_stop(0);
		exit(1);
	}

	g_lcore = spdk_env_get_first_core();

	printf("using bdev %s\n", g_bdev_name.c_str());
	spdk_fs_load(g_bs_dev, __send_request, fs_load_cb, NULL);
}

static void
fs_unload_cb(__attribute__((unused)) void *ctx,
	     __attribute__((unused)) int fserrno)
{
	assert(fserrno == 0);

	spdk_app_stop(0);
}

static void
rocksdb_shutdown(void)
{
	if (g_fs != NULL) {
		spdk_fs_unload(g_fs, fs_unload_cb, NULL);
	} else {
		fs_unload_cb(NULL, 0);
	}
}

static void *
initialize_spdk(void *arg)
{
	struct spdk_app_opts *opts = (struct spdk_app_opts *)arg;
	int rc;

	rc = spdk_app_start(opts, rocksdb_run, NULL);
	/*
	 * TODO:  Revisit for case of internal failure of
	 * spdk_app_start(), itself.  At this time, it's known
	 * the only application's use of spdk_app_stop() passes
	 * a zero; i.e. no fail (non-zero) cases so here we
	 * assume there was an internal failure and flag it
	 * so we can throw an exception.
	 */
	if (rc) {
		g_spdk_start_failure = true;
	} else {
		spdk_app_fini();
		delete opts;
	}
	pthread_exit(NULL);

}

SpdkEnv::SpdkEnv(const std::string &conf,
		 const std::string &bdev, uint64_t cache_size_in_mb)
	: mConfig(conf), mBdev(bdev)
{
	struct spdk_app_opts *opts = new struct spdk_app_opts;

	spdk_app_opts_init(opts, sizeof(*opts));
	opts->name = "rocksdb";
	opts->json_config_file = mConfig.c_str();
	opts->shutdown_cb = rocksdb_shutdown;
	opts->tpoint_group_mask = "0x80";

	spdk_fs_set_cache_size(cache_size_in_mb);
	g_bdev_name = mBdev;

	pthread_create(&mSpdkTid, NULL, &initialize_spdk, opts);
	while (!g_spdk_ready && !g_spdk_start_failure)
		;
	if (g_spdk_start_failure) {
		delete opts;
		throw SpdkAppStartException("spdk_app_start() unable to start rocksdb_run()");
	}

	SpdkInitializeThread();
}

SpdkEnv::~SpdkEnv()
{
	/* This is a workaround for rocksdb test, we close the files if the rocksdb not
	 * do the work before the test quit.
	 */
	if (g_fs != NULL) {
		spdk_fs_iter iter;
		struct spdk_file *file;

		if (!g_sync_args.channel) {
			SpdkInitializeThread();
		}

		iter = spdk_fs_iter_first(g_fs);
		while (iter != NULL) {
			file = spdk_fs_iter_get_file(iter);
			spdk_file_close(file, g_sync_args.channel);
			iter = spdk_fs_iter_next(iter);
		}
	}

	spdk_app_start_shutdown();
	pthread_join(mSpdkTid, NULL);
}

SpdkEnv *NewSpdkEnv(const std::string &conf,
		const std::string &bdev, uint64_t cache_size_in_mb)
{
	try {
		SpdkEnv *spdk_env = new SpdkEnv(conf, bdev, cache_size_in_mb);
		if (g_fs != NULL) {
			return spdk_env;
		} else {
			delete spdk_env;
			return NULL;
		}
	} catch (SpdkAppStartException &e) {
		SPDK_ERRLOG("NewSpdkEnv: exception caught: %s", e.what());
		return NULL;
	} catch (...) {
		SPDK_ERRLOG("NewSpdkEnv: default exception caught");
		return NULL;
	}
}

void
benchwrite(SpdkEnv * env, int i)
{
    char buf[128];
    uint64_t times = 0;
    char *data = (char *) malloc(chunk_size);
    int idx;
    sprintf(buf, "envtestfile-%d", i);
    std::string filename = std::string(buf);
    std::unique_ptr<SpdkWritableFile> writefile;
    int rc = env->NewWritableFile(filename, &writefile);
    if (rc != 0) {
          delete env;
          fprintf(stderr, "New writable file fail");
          exit(-1);
    }

    rc = writefile->Allocate(0, bench_size);
    if (rc != 0) {
          delete env;
          fprintf(stderr, "allocate offset: %ld size: %ld\n", 0, bench_size);
          goto out_exit;
          exit(-1);
    }


   fprintf(stdout, "becnh size: %ld, chunk size: %ld\n", bench_size, chunk_size);

    for (idx = 0; idx < chunk_size; idx++) {
    	data[i] = '1';
    }

    do {
        rc = writefile->Append(data);
        if (rc != 0) {
            fprintf(stdout, "append data fail, rc: %d\n", rc);
            goto out_exit;
        }
        writefile->Sync();
        //usleep(1000);
        times++;
    } while(writefile->GetFileSize() < bench_size);
    fprintf(stdout, "file %d write size; %ld, times: %ld\n", i, writefile->GetFileSize(), times);

out_exit:
    writefile.reset();
}

int
main(int argc, char *argv[])
{
  SpdkInitializeThread();
    std::string conf = std::string(argv[1]);
    std::string bdev = std::string(argv[2]);
    uint64_t cache_size_mb = atol(argv[3]);
    const int thread_num = atoi(argv[4]);

    fprintf(stdout, "conf: %s bdev: %s cache size: %ld mb\n", conf.c_str(), bdev.c_str(), cache_size_mb);
    SpdkEnv *env = NewSpdkEnv(conf, bdev, cache_size_mb);

    std::thread *threads[thread_num];
    for (int i = 0; i < thread_num; i++) {
            threads[i] = new std::thread(benchwrite, env, i);
    }

    for (int i = 0; i < thread_num; i++) {
            threads[i]->join();
    }

    delete env;
    return 0;
}
