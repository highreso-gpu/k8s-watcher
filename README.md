# k8s-watcher

This is a Kubernetes watcher project to monitor pod events and notify a custom API (e.g., `clusterapi`).

## Features
- Monitor Kubernetes pod events (ADDED, MODIFIED, DELETED)
- Trigger an API call on pod changes
- Works in local, staging, and production environments
- Dockerized for easy deployment

## Usage
1. Build the Docker image:
   ```bash
   docker build -t k8s-watcher .