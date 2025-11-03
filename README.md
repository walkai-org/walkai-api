# walk:ai-api

FastAPI service powering the walk:ai backend APIs.

## Getting Started

1. Start a local DynamoDB instance (required for state storage):

    ```bash
    docker run -d --name dynamodb \
      -p 6379:8000 \
      amazon/dynamodb-local:latest \
      -jar DynamoDBLocal.jar -sharedDb -inMemory
    ```

    - Point `DYNAMODB_ENDPOINT` to `http://localhost:6379` (or the Docker host IP if needed).
2. Run a [dev cluster](#create-a-dev-cluster)
3. Sync dependencies with [uv](https://github.com/astral-sh/uv): `uv sync`
4. Launch the development server: `uv run uvicorn app.main:app --reload`

## Environment Variables

Copy `.env.example` to `.env` and provide the values required by your deployment.
- `DATABASE_URL` can point to PostgreSQL in shared environments or a local SQLite file such as `sqlite:///./data/dev.db` for lightweight development.

## Create a dev cluster

```bash
minikube start --driver=docker --disk-size 10g -p walkai-dev
```

Get the value for CLUSTER_URL
```bash
CLUSTER_NAME="walkai-dev"
kubectl config view -o jsonpath="{.clusters[?(@.name==\"$CLUSTER_NAME\")].cluster.server}"
```
Create a token
```bash
kubectl apply -f secret.yml
```
Get the value for CLUSTER_TOKEN
```bash
kubectl get secret api-client-permanent-token -n walkai   -o jsonpath='{.data.token}' | base64 -d; echo
```

### Set up fake-mig
```bash
kubectl label node walkai-dev run.ai/simulated-gpu-node-pool=default --overwrite
kubectl annotate node walkai-dev kwok.x-k8s.io/node=fake --overwrite
helm upgrade -i gpu-operator oci://ghcr.io/run-ai/fake-gpu-operator/fake-gpu-operator --namespace gpu-operator --create-namespace -f mig-values.yml
kubectl -n gpu-operator rollout status deploy/status-updater 
kubectl -n gpu-operator rollout status deploy/kwok-gpu-device-plugin
```
