#!/bin/bash
REMOTE_URL=https://github.com/lucianosrp/talks.git

# Clone the repository with sparse and filtering out blob
git clone --filter=blob:none --sparse $REMOTE_URL

cd talks
git sparse-checkout add "$1"
