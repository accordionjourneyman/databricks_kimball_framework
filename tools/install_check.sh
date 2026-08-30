#!/bin/bash
set -e
pip install -q "pyspark[pipelines]==4.2.0"
python tools/check_scd2_support.py