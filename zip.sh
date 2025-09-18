#!/bin/bash

OUT="exacopilot.zip"
IN="assets helpers rag rag_read_only simulations client.py config.ini dcli.py exacopilot.sh README.md requirements.txt server.py server_read_only.py workarounds.py zip.sh"
zip -r $OUT $IN