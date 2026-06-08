# Worker — Nœud d'exécution des tâches

Le **worker** est un nœud d'exécution. Il s'enregistre auprès du scheduler du
nœud, envoie des battements de cœur (*heartbeats*), récupère les tâches qui
correspondent à ses ressources (CPU/mémoire/GPU et tags), les exécute dans des
conteneurs Docker, puis téléverse les artefacts produits vers le stockage S3
(MinIO) du nœud.

> Prérequis fonctionnel : le **nœud** doit être démarré et joignable sur le
> tailnet. Voir `../node/README.md`. On peut lancer autant de workers que voulu
> (sur des machines différentes), chacun rejoignant le tailnet.

## Rôle dans la plateforme

```
   Worker
     │  s'enregistre + heartbeat ─► Scheduler du nœud (node:50051, gRPC)
     │  écoute "jobs.available"  ◄─ NATS du nœud (node:4222)
     │  exécute le job ──────────► conteneur Docker (via /var/run/docker.sock)
     └─ téléverse l'artefact ────► MinIO/S3 du nœud (node:9000)
```

Le worker tourne dans le *network namespace* du sidecar `tailscale`
(`network_mode: service:tailscale`) : il joint le nœud par ses noms tailnet
(`node:50051`, `node:4222`, `node:9000`). **Aucun port n'est exposé** sur l'hôte.

## Prérequis

- **Docker** et **Docker Compose v2** — le worker pilote le démon Docker de
  l'hôte (montage de `/var/run/docker.sock`) pour lancer les conteneurs de tâches.
- Une **clé d'auth Tailscale** (`tskey-auth-…`), sur **la même tailnet** que le
  nœud.
- Le **GID du groupe `docker`** de l'hôte (pour que le worker puisse accéder au
  socket Docker) :

  ```bash
  getent group docker | cut -d: -f3      # ex. 988
  ```

- **Pour un worker GPU uniquement** : le **NVIDIA Container Toolkit** installé et
  configuré sur l'hôte (le worker demande des GPU aux conteneurs de tâches via
  `DeviceRequest(capabilities=[["gpu"]])`). Vérifiez avec :

  ```bash
  docker run --rm --gpus all nvidia/cuda:12.4.0-base-ubuntu22.04 nvidia-smi
  ```

- *(Optionnel)* **uv** + **Python 3.14** seulement si vous régénérez les stubs
  gRPC (`make proto`) ou exécutez le worker hors conteneur.

## Initialisation — pas à pas

1. **Créer le fichier d'environnement** :

   ```bash
   cd worker
   cp .env.example .env
   ```

2. **Renseigner `.env`** (voir [Variables d'environnement](#variables-denvironnement)).
   Exemple d'un worker CPU :

   ```dotenv
   TS_AUTHKEY=tskey-auth-xxxxx
   TS_HOSTNAME=worker-01

   SCHEDULER_GRPC_URL=node:50051
   NATS_URL=nats://node:4222

   WORKER_ID=worker-01
   WORKER_TAGS=["cpu","docker"]
   WORKER_CPU=4
   WORKER_MEM_MB=8192
   WORKER_GPU=0
   WORKER_MAX_SLOTS=2

   HEARTBEAT_INTERVAL_S=5
   PULL_INTERVAL_S=3
   ```

   Puis, **sur la ligne de commande au lancement**, fournissez les éléments qui
   ne sont pas dans `.env.example` mais nécessaires :

   ```dotenv
   DOCKER_GID=988                 # GID du groupe docker de l'hôte (cf. prérequis)
   MINIO_ROOT_USER=minioadmin     # doit correspondre aux identifiants MinIO du nœud
   MINIO_ROOT_PASSWORD=minioadmin
   ```

   > Les valeurs S3 par défaut (`S3_ENDPOINT_URL=http://node:9000`,
   > `S3_BUCKET=job-results`, identifiants `minioadmin`) correspondent au nœud
   > tel qu'il est livré. Ne les changez que si vous avez personnalisé MinIO.

3. **Démarrer le worker** :

   ```bash
   docker compose up -d --build
   ```

4. **Vérifier l'enregistrement** :

   ```bash
   docker compose logs -f worker
   ```

   Vous devez voir une ligne « Worker … starting » puis l'enregistrement auprès
   du scheduler. Côté nœud, le worker apparaît alors dans le registre et reçoit
   des tâches compatibles.

### Configurer un worker GPU

```dotenv
WORKER_TAGS=["gpu","cuda","docker"]
WORKER_GPU=1
```

Le NVIDIA Container Toolkit doit être présent sur l'hôte (voir prérequis). Le
scheduler n'attribuera des jobs `min_gpu>=1` qu'aux workers déclarant des GPU
libres.

## Variables d'environnement

| Variable               | Défaut                  | Description |
|------------------------|-------------------------|-------------|
| `TS_AUTHKEY`           | —                       | **Obligatoire.** Clé d'auth Tailscale. |
| `TS_HOSTNAME`          | `worker`                | Nom d'hôte du worker sur le tailnet. |
| `DOCKER_GID`           | `981`                   | **À adapter.** GID du groupe `docker` de l'hôte (accès au socket). |
| `SCHEDULER_GRPC_URL`   | `node:50051`            | Scheduler gRPC du nœud. |
| `NATS_URL`             | `nats://node:4222`      | NATS du nœud. |
| `WORKER_ID`            | *(hostname + uuid)*     | Identifiant unique du worker. |
| `WORKER_TAGS`          | `["cpu","docker"]`      | Tags annoncés (JSON ou liste séparée par virgules). |
| `WORKER_CPU`           | `4`                     | Cœurs CPU déclarés. |
| `WORKER_MEM_MB`        | `8192`                  | Mémoire déclarée (Mo). |
| `WORKER_GPU`           | `0`                     | Nombre de GPU déclarés. |
| `WORKER_MAX_SLOTS`     | `2`                     | Jobs concurrents maximum. |
| `HEARTBEAT_INTERVAL_S` | `5`                     | Période des heartbeats (s). |
| `PULL_INTERVAL_S`      | `3`                     | Période de récupération des jobs (s). |
| `S3_ENDPOINT_URL`      | `http://node:9000`      | Endpoint S3/MinIO interne (téléversement). |
| `S3_EXTERNAL_ENDPOINT_URL` | `http://node:9000`  | Endpoint S3 pour les URLs présignées (téléchargement). |
| `MINIO_ROOT_USER`      | `minioadmin`            | Clé d'accès S3 (doit matcher le nœud). |
| `MINIO_ROOT_PASSWORD`  | `minioadmin`            | Clé secrète S3 (doit matcher le nœud). |
| `S3_BUCKET`            | `job-results`           | Bucket des artefacts. |
| `UID` / `GID`          | `1000`                  | UID/GID du process dans le conteneur. |

Configuration via **Pydantic Settings**, délimiteur `__` (les variables
`WORKER_*` ci-dessus sont mappées vers `WORKER__*`, etc., dans `compose.yaml`).

## Commandes utiles

```bash
docker compose up -d --build      # (re)construire et démarrer
docker compose logs -f worker     # suivre les logs (enregistrement, jobs)
docker compose down               # arrêter le worker

make proto                        # régénérer les stubs gRPC worker (nécessite uv)
```

## Dépannage

- **`permission denied` sur `/var/run/docker.sock`** : `DOCKER_GID` ne correspond
  pas au groupe `docker` de l'hôte. Corrigez-le (`getent group docker | cut -d: -f3`)
  et relancez.
- **Le worker ne s'enregistre pas** : nœud injoignable. Vérifiez le tailnet
  (`docker compose exec tailscale tailscale status`) et `SCHEDULER_GRPC_URL`.
- **Jobs GPU jamais attribués** : assurez-vous que `WORKER_GPU>=1`, que les tags
  incluent `gpu`, et que `docker run --gpus all …` fonctionne sur l'hôte.
- **Échec de téléversement d'artefact** : identifiants S3 (`MINIO_ROOT_USER`/
  `MINIO_ROOT_PASSWORD`) différents de ceux du nœud, ou bucket `job-results`
  absent (vérifiez `mc-init` côté nœud).
