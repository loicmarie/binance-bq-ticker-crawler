#!/bin/bash
until python crawler.py; do
    echo "'crawler.py' crashed with exit code $?. Restarting..." >&2
    sleep 1
done
