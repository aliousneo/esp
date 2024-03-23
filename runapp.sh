#!/bin/bash

uvicorn app:app --proxy-headers --host 0.0.0.0 --port 6969 --reload