#!/bin/bash

if [ ! -d venv ]; then
  python3 -m virtualenv venv
  source venv/bin/activate
  pip install -r requirements.txt
else
  source venv/bin/activate
fi

while read pub; do
  echo $pub && \
  python3 publisher_data.py $pub;
done < un_publishers.txt
