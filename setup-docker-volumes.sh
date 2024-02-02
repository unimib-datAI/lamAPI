#!/bin/bash

# Define all directories to be created
dirs=(
  "./api"
  "./mongo-data"
  "./certs"
  "./esdata01"
  "./esdata02"
  "./esdata03"
  "./kibanadata"
)

# Loop through the directories and create them if they don't exist
for dir in "${dirs[@]}"; do
  if [ ! -d "$dir" ]; then
    echo "Creating directory: $dir"
    mkdir -p "$dir"
  else
    echo "Directory already exists: $dir"
  fi
done

echo "Setting permissions to 777"
for dir in "${dirs[@]}"; do
  # Set permissions to 777 for directories and files within
  chmod -R 777 "$dir"
done

echo "Setup complete."
