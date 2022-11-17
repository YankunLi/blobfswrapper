#include <stdio.h>
#include <malloc.h>

#include "spdk/env.h"
#include "spdk/event.h"
#include "spdk/blob.h"
#include "spdk/blobfs.h"
#include "spdk/blob_bdev.h"
#include "spdk/log.h"
#include "spdk/thread.h"
#include "spdk/bdev.h"

#include "./blobfswrapper.h"

struct spdk_filesystem *g_fs = NULL;
struct spdk_bs_dev *g_bs_dev;
thread_local struct spdk_fs_thread_ctx *g_sync_channel;
//struct spdk_fs_thread_ctx *g_sync_channel;
uint32_t g_lcore = 0;
char* g_bdev_name;
pthread_t spdktid;
volatile bool g_spdk_ready = false;
volatile bool g_spdk_start_failure = false;

static void
fs_unload_cb(__attribute__((unused)) void *ctx,
	     __attribute__((unused)) int fserrno)
{
	assert(fserrno == 0);

	spdk_app_stop(0);
}

static void
blobfs_shutdown(void)
{
	if (g_fs != NULL) {
		spdk_fs_unload(g_fs, fs_unload_cb, NULL);
	} else {
		fs_unload_cb(NULL, 0);
	}
}

static void
base_bdev_event_cb(enum spdk_bdev_event_type type, __attribute__((unused)) struct spdk_bdev *bdev,
		   __attribute__((unused)) void *event_ctx)
{
	printf("Unsupported bdev event: type %d\n", type);
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
blobfs_run(__attribute__((unused)) void *arg1)
{
	int rc;

	rc = spdk_bdev_create_bs_dev_ext(g_bdev_name, base_bdev_event_cb, NULL,
					 &g_bs_dev);
	if (rc != 0) {
		printf("Could not create blob bdev\n");
		spdk_app_stop(0);
		exit(1);
	}

	g_lcore = spdk_env_get_first_core();

	printf("using bdev %s\n", g_bdev_name);
	spdk_fs_load(g_bs_dev, __send_request, fs_load_cb, NULL);
}

static void *
initialize_spdk(void *arg)
{
	struct spdk_app_opts *opts = (struct spdk_app_opts *)arg;
	int rc;

	rc = spdk_app_start(opts, blobfs_run, NULL);
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
                free((void *)opts);
	}
	pthread_exit(NULL);
}

/* The thread local constructor doesn't work for the main thread, since
 * the filesystem hasn't been loaded yet.  So we break out this
 * spdk_initialize_thread_ctx function, so that the main thread can explicitly
 * call it after the filesystem has been loaded.
 */
static void
spdk_initialize_thread_ctx(void)
{
	struct spdk_thread *thread;

	if (g_fs != NULL) {
		if (g_sync_channel) {
			spdk_fs_free_thread_ctx(g_sync_channel);
		}
		thread = spdk_thread_create("spdk_blobfs", NULL);
		spdk_set_thread(thread);
		g_sync_channel = spdk_fs_alloc_thread_ctx(g_fs);
	}
}

int
mount_blobfs(char* spdk_conf, char *spdk_dev_name, uint64_t cache_size_in_mb)
{
        struct spdk_app_opts *opts = (struct spdk_app_opts *) malloc(sizeof(struct spdk_app_opts));
        if (opts == NULL) {
                return -1;
        }

        spdk_app_opts_init(opts, sizeof(*opts));
        opts->name = "blobfs";
        opts->json_config_file = spdk_conf;
        opts->shutdown_cb = blobfs_shutdown;
        opts->tpoint_group_mask = "0x80";

        spdk_fs_set_cache_size(cache_size_in_mb);
        g_bdev_name = spdk_dev_name;

        pthread_create(&spdktid, NULL, &initialize_spdk, opts);
        while (!g_spdk_ready && !g_spdk_start_failure)
          ;
        if (g_spdk_start_failure) {
          free((void *)opts);
          return -1;
        }

        spdk_initialize_thread_ctx();

        return 0;
}


__attribute__((unused)) static void
set_channel(void)
{
	struct spdk_thread *thread;

	if (g_fs != NULL && g_sync_channel == NULL) {
		thread = spdk_thread_create("spdk_blobfs", NULL);
		spdk_set_thread(thread);
		g_sync_channel = spdk_fs_alloc_thread_ctx(g_fs);
	}
}

static void
free_spdk_thread_ctx(void)
{
          if (g_sync_channel != NULL) {
                  spdk_fs_free_thread_ctx(g_sync_channel);
                  g_sync_channel = NULL;
          }
}


void
unmount_blobfs(void)
{
	/* This is a workaround for blobfs test, we close the files if the blobfs not
	 * do the work before the test quit.
	 */
	if (g_fs != NULL) {
		spdk_fs_iter iter;
		struct spdk_file *file;

		if (!g_sync_channel) {
                        spdk_initialize_thread_ctx();
		}

		iter = spdk_fs_iter_first(g_fs);
		while (iter != NULL) {
			file = spdk_fs_iter_get_file(iter);
			spdk_file_close(file, g_sync_channel);
			iter = spdk_fs_iter_next(iter);
		}
	}

	spdk_app_start_shutdown();
	pthread_join(spdktid, NULL);

        free_spdk_thread_ctx();
}

#define EBLOBFS -128
#define ENOFS -129
#define ENULLPTR -129
#define EMEM -130

typedef struct _blobfs_file {
        struct spdk_file *s_file;
} blobfs_file;

static bool
check_fs_and_channel()
{
  if (g_fs == NULL || g_sync_channel == NULL) {
          return false;
  }

  return true;
}

//int spdk_fs_create_file(struct spdk_filesystem *fs, struct spdk_fs_thread_ctx *ctx,
//			const char *name);
int
blobfs_create_file(char *name)
{
        if (!check_fs_and_channel())
                return ENOFS;
        if (name == NULL) {
                return ENULLPTR;
        }
        int rc;

        rc = spdk_fs_create_file(g_fs, g_sync_channel, name);
        if (rc != 0)
                return EBLOBFS;

        return 0;
}

static int
allocate_blobfs_file(blobfs_file ** file)
{
        *file = (blobfs_file *) malloc(sizeof(blobfs_file));
        if (*file == NULL) {
                return ENULLPTR;
        }

        return 0;
}

static void
free_blobfs_file(blobfs_file *file)
{
        if (file != NULL) {
                if (file->s_file != NULL) {
                      free(file->s_file);
                      file->s_file = NULL;
                }
                free(file);
                file = NULL;
        }
}

//int spdk_fs_open_file(struct spdk_filesystem *fs, struct spdk_fs_thread_ctx *ctx,
//		      const char *name, uint32_t flags, struct spdk_file **file);
int
blobfs_open_file(char *name, uint32_t flags, blobfs_file ** file) {
        if (!check_fs_and_channel()) {
                return ENOFS;
        }
        if (name == NULL) {
                return ENULLPTR;
        }
        if (allocate_blobfs_file(file) != 0) {
          return EMEM;
        }
        int rc;
        rc = spdk_fs_open_file(g_fs, g_sync_channel, name, flags, *file->s_file);
        if (rc != 0)
          return EBLOBFS;

        return 0;
}



int main(int argc, char **argv) {
  printf("hello blobfs wrapper!");

  return 0;
}
