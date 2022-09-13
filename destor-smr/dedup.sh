#!/bin/bash

# This script will test miss.llf's latest idea.

run_time=$(date +%Y%m%d-%H%M%S)




./rebuild

    echo === backup file at smr:  ===
   ./destor /mnt/e/linux/linux-3.0.001  >> ${run_time}.smr.backup_stdout.log
    ./destor /mnt/e/linux/linux-3.0.002 >> ${run_time}.smr.backup_stdout.log
    ./destor /mnt/e/linux/linux-3.0.003 >> ${run_time}.smr.backup_stdout.log

    echo == use lru to restore  ==
   ./destor -r0 /mnt/e/linux-data-config/linux-30/file/restore/  >> ${run_time}.smr.restore_lru_stdout.log
    ./destor -r1 /mnt/e/linux-data-config/linux-30/file/restore/  >> ${run_time}.smr.restore_lru_stdout.log
    ./destor -r2 /mnt/e/linux-data-config/linux-30/file/restore/  >> ${run_time}.smr.restore_lru_stdout.log
    cat ./restore.log >> ${run_time}.smr.restore_lru.states
    rm ./restore.log

./destor -s >> ${run_time}.smr.backup_sys.log
mv ./backup.log ${run_time}.smr.backup.states
