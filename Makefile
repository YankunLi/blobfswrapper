#  SPDX-License-Identifier: BSD-3-Clause
#  Copyright (c) Intel Corporation.
#  All rights reserved.
#
#
#spdk_wks=/root/workspace/spdk
spdk_wks=/root/liyankun/

wrapper: lib
	cc -g -o wrapper -DBENCH_SIZE=3 -Wl,-z,relro,-z,now -Wl,-z,noexecstack -fuse-ld=bfd wrapper.c -I ./ -L ./ -lblobfs_wrapper -L/root/liyankun/spdk/build/lib -Wl,--whole-archive -Wl,--no-as-needed -lspdk_bdev_malloc -lspdk_bdev_null -lspdk_bdev_nvme -lspdk_bdev_passthru -lspdk_bdev_lvol -lspdk_bdev_raid -lspdk_bdev_error -lspdk_bdev_gpt -lspdk_bdev_split -lspdk_bdev_delay -lspdk_bdev_zone_block -lspdk_blobfs_bdev -lspdk_blobfs -lspdk_blob_bdev -lspdk_lvol -lspdk_blob -lspdk_nvme -lspdk_bdev_aio -lspdk_bdev_ftl -lspdk_ftl -lspdk_bdev_virtio -lspdk_virtio -lspdk_vfio_user -lspdk_accel_ioat -lspdk_ioat -lspdk_scheduler_dynamic -lspdk_env_dpdk -lspdk_scheduler_dpdk_governor -lspdk_scheduler_gscheduler -lspdk_event -lspdk_sock_posix -lspdk_event_bdev -lspdk_bdev -lspdk_notify -lspdk_dma -lspdk_event_accel -lspdk_accel -lspdk_event_vmd -lspdk_vmd -lspdk_event_sock -lspdk_sock -lspdk_event_iobuf -lspdk_init -lspdk_thread -lspdk_trace -lspdk_rpc -lspdk_jsonrpc -lspdk_json -lspdk_util -lspdk_log -Wl,--no-whole-archive /root/liyankun/spdk/build/lib/libspdk_env_dpdk.a -Wl,--whole-archive /root/liyankun/spdk/dpdk/build/lib/librte_bus_pci.a /root/liyankun/spdk/dpdk/build/lib/librte_cryptodev.a /root/liyankun/spdk/dpdk/build/lib/librte_dmadev.a /root/liyankun/spdk/dpdk/build/lib/librte_eal.a /root/liyankun/spdk/dpdk/build/lib/librte_ethdev.a /root/liyankun/spdk/dpdk/build/lib/librte_hash.a /root/liyankun/spdk/dpdk/build/lib/librte_kvargs.a /root/liyankun/spdk/dpdk/build/lib/librte_mbuf.a /root/liyankun/spdk/dpdk/build/lib/librte_mempool.a /root/liyankun/spdk/dpdk/build/lib/librte_mempool_ring.a /root/liyankun/spdk/dpdk/build/lib/librte_net.a /root/liyankun/spdk/dpdk/build/lib/librte_pci.a /root/liyankun/spdk/dpdk/build/lib/librte_power.a /root/liyankun/spdk/dpdk/build/lib/librte_rcu.a /root/liyankun/spdk/dpdk/build/lib/librte_ring.a /root/liyankun/spdk/dpdk/build/lib/librte_telemetry.a /root/liyankun/spdk/dpdk/build/lib/librte_vhost.a -Wl,--no-whole-archive  -ldl  -pthread -lrt -luuid -lssl -lcrypto -lm -lfuse3 -laio

# cc -o wrapper -Wl,-z,relro,-z,now -Wl,-z,noexecstack -fuse-ld=bfd wrapper.c  -I ./  -L ./ -lblobfs_wrapper -L$(spdk_wks)/spdk/build/lib -Wl,--whole-archive -Wl,--no-as-needed -lspdk_bdev_malloc -lspdk_bdev_null -lspdk_bdev_nvme -lspdk_bdev_passthru -lspdk_bdev_lvol -lspdk_bdev_raid -lspdk_bdev_error -lspdk_bdev_gpt -lspdk_bdev_split -lspdk_bdev_delay -lspdk_bdev_zone_block -lspdk_blobfs_bdev -lspdk_blobfs -lspdk_blob_bdev -lspdk_lvol -lspdk_blob -lspdk_nvme -lspdk_bdev_aio -lspdk_bdev_ftl -lspdk_ftl -lspdk_bdev_virtio -lspdk_virtio -lspdk_vfio_user -lspdk_accel_ioat -lspdk_ioat -lspdk_scheduler_dynamic -lspdk_env_dpdk -lspdk_scheduler_dpdk_governor -lspdk_scheduler_gscheduler -lspdk_event -lspdk_sock_posix -lspdk_event_bdev -lspdk_bdev -lspdk_notify -lspdk_dma -lspdk_event_accel -lspdk_accel -lspdk_event_vmd -lspdk_vmd -lspdk_event_sock -lspdk_init -lspdk_thread -lspdk_trace -lspdk_sock -lspdk_rpc -lspdk_jsonrpc -lspdk_json -lspdk_util -lspdk_log -Wl,--no-whole-archive $(spdk_wks)/spdk/build/lib/libspdk_env_dpdk.a -Wl,--whole-archive $(spdk_wks)/spdk/dpdk/build/lib/librte_bus_pci.a $(spdk_wks)/spdk/dpdk/build/lib/librte_cryptodev.a $(spdk_wks)/spdk/dpdk/build/lib/librte_dmadev.a $(spdk_wks)/spdk/dpdk/build/lib/librte_eal.a $(spdk_wks)/spdk/dpdk/build/lib/librte_ethdev.a $(spdk_wks)/spdk/dpdk/build/lib/librte_hash.a $(spdk_wks)/spdk/dpdk/build/lib/librte_kvargs.a $(spdk_wks)/spdk/dpdk/build/lib/librte_mbuf.a $(spdk_wks)/spdk/dpdk/build/lib/librte_mempool.a $(spdk_wks)/spdk/dpdk/build/lib/librte_mempool_ring.a $(spdk_wks)/spdk/dpdk/build/lib/librte_net.a $(spdk_wks)/spdk/dpdk/build/lib/librte_pci.a $(spdk_wks)/spdk/dpdk/build/lib/librte_power.a $(spdk_wks)/spdk/dpdk/build/lib/librte_rcu.a $(spdk_wks)/spdk/dpdk/build/lib/librte_ring.a $(spdk_wks)/spdk/dpdk/build/lib/librte_telemetry.a $(spdk_wks)/spdk/dpdk/build/lib/librte_vhost.a -Wl,--no-whole-archive  -lnuma -ldl  -L$(spdk_wks)/spdk/isa-l/.libs -lisal -pthread -lrt -luuid -lssl -lcrypto -lm -lfuse3 -laio
#	cc -o wrapper -Wl,-z,relro,-z,now -Wl,-z,noexecstack -fuse-ld=bfd wrapper.c  -I ./  -L ./ -lblobfs_wrapper -L $(spdk_wks)/spdk/build/lib -Wl,--whole-archive -Wl,--no-as-needed -lspdk_bdev_malloc -lspdk_bdev_null -lspdk_bdev_nvme -lspdk_bdev_passthru -lspdk_bdev_lvol -lspdk_bdev_raid -lspdk_bdev_error -lspdk_bdev_gpt -lspdk_bdev_split -lspdk_bdev_delay -lspdk_bdev_zone_block -lspdk_blobfs_bdev -lspdk_blobfs -lspdk_blob_bdev -lspdk_lvol -lspdk_blob -lspdk_nvme -lspdk_bdev_aio -lspdk_bdev_ftl -lspdk_ftl -lspdk_bdev_virtio -lspdk_virtio -lspdk_vfio_user -lspdk_accel_ioat -lspdk_ioat -lspdk_scheduler_dynamic -lspdk_env_dpdk -lspdk_scheduler_dpdk_governor -lspdk_scheduler_gscheduler -lspdk_event -lspdk_sock_posix -lspdk_event_bdev -lspdk_bdev -lspdk_notify -lspdk_dma -lspdk_event_accel -lspdk_accel -lspdk_event_vmd -lspdk_vmd -lspdk_event_sock -lspdk_init -lspdk_thread -lspdk_trace -lspdk_sock -lspdk_rpc -lspdk_jsonrpc -lspdk_json -lspdk_util -lspdk_log -Wl,--no-whole-archive $(spdk_wks)/spdk/build/lib/libspdk_env_dpdk.a -Wl,--whole-archive $(spdk_wks)/spdk/dpdk/build/lib/librte_bus_pci.a $(spdk_wks)/spdk/dpdk/build/lib/librte_cryptodev.a $(spdk_wks)/spdk/dpdk/build/lib/librte_dmadev.a $(spdk_wks)/spdk/dpdk/build/lib/librte_eal.a $(spdk_wks)/spdk/dpdk/build/lib/librte_ethdev.a $(spdk_wks)/spdk/dpdk/build/lib/librte_hash.a $(spdk_wks)/spdk/dpdk/build/lib/librte_kvargs.a $(spdk_wks)/spdk/dpdk/build/lib/librte_mbuf.a $(spdk_wks)/spdk/dpdk/build/lib/librte_mempool.a $(spdk_wks)/spdk/dpdk/build/lib/librte_mempool_ring.a $(spdk_wks)/spdk/dpdk/build/lib/librte_net.a $(spdk_wks)/spdk/dpdk/build/lib/librte_pci.a $(spdk_wks)/spdk/dpdk/build/lib/librte_power.a $(spdk_wks)/spdk/dpdk/build/lib/librte_rcu.a $(spdk_wks)/spdk/dpdk/build/lib/librte_ring.a $(spdk_wks)/spdk/dpdk/build/lib/librte_telemetry.a $(spdk_wks)/spdk/dpdk/build/lib/librte_vhost.a -Wl,--no-whole-archive -lnuma -ldl -pthread -lrt -luuid -lssl -lcrypto -lm -lfuse4 -laio

lib:
	cc -Wl,-z,relro,-z,now -Wl,-z,noexecstack -fuse-ld=bfd -c blobfs_wrapper.c -I ./ 
	ar -crv libblobfs_wrapper.a blobfs_wrapper.o

clear:
	./wrapper ./test.json Nvme0n1 4096 clear

write:
	./wrapper ./test.json Nvme0n1 4096 1 seq 8 4

read:
	./wrapper ./test.json Nvme0n1 4096 read 512 seq

randread:
	./wrapper ./test.json Nvme0n1 4096 read 4 rand

clean:
	rm -rf wrapper blobfs_wrapper.o libblobfs_wrapper.a
.PHONY: wrapper
