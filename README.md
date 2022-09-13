# InDe

We propose an inline deduplication approach for storage systems, called InDe, which uses a greedy algorithm to detect valid container utilization and dynamically adjusts the number of old container references in each segment.

# The InDe paper
Lifang Lin,Yuhui Deng, Yi Zhou, Yifeng Zhu. InDe: An Inline Data Deduplication Approach via Adaptive Detection of Valid Container Utilization. ACM Transactions on Storage. ACM Press. (Accepted)


# Implementation

We use the interfaces in [Destor](https://github.com/fomy/destor) and make some revisions to implement InDe. 


# Environment

Linux 64bit.

# Dependencies

1. libssl-dev is required to calculate sha-1 digest;
2. GLib 2.32 or later version

# Installation

If all dependencies are installed, compiling destor is straightforward:

> ./configure

> make

> make install

# Running

The `rebuild` script should be run before destor to prepare working directory and clear data.

The default working path is in `destor.config`. You can modify it together with the rebuild script. 

You can do the following steps to backup and restore the backup stream.

1. start a backup task,

   > destor /backup_stream_directory

   Example: `destor /home/data/`

2. start a restore task,

   > destor -r$backup_job_id$ /restore_direstory

   Example: `destor -r0 /home/restore/ `

# Configuration

A sample configuration is shown in `destor.config`.

# Contact

Email: tyhdeng@email.jnu.edu.cn

If you have any questions, please feel free to contact me.
