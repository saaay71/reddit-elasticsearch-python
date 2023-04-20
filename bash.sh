#!/bin/bash
#zstd -cd ../RC_2023-01.zst | split -b 1G - RC_2023_01_ > /dev/null 2>&1 &
zstd -cd ../RC_2023-01.zst | split -l 770000 -d - RC_2023-01_ > /dev/null 2>&1 &
