#!/bin/bash
CMD="../cli/cli -e $1"
echo $CMD
bash -c "$CMD"
