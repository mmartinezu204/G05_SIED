# PRUEBAAA

# ddia — AIS vessel data → Databricks Unity Catalog

Pipeline de infraestructura + carga de datos para bajar datos públicos de tráfico
marítimo (AIS, NOAA) e ingerirlos en un volumen de Unity Catalog en Databricks,
como base para materializar una tabla Delta con `spark.read`.

## Prerrequisitos

- Docker + VS Code con la extensión Dev Containers.
- Un workspace de Databricks con Unity Catalog habilitado y un PAT (Personal
  Access Token).

## Setup

1. Copiar `.env.example` a `.env` y completar `DATABRICKS_HOST` / `DATABRICKS_TOKEN`
   (`DATABRICKS_HTTP_PATH` no lo usa este pipeline, es para conectarse a un SQL
   Warehouse aparte).
2. `Cmd/Ctrl+Shift+P` → **Dev Containers: Rebuild and Reopen in Container**.
   El `dockerfile` instala ahí adentro `terraform`, el `databricks` CLI, `uv`,
   `jq` y `unzip` — el script asume que corrés siempre dentro del container,
   no en el host.
3. `./setup.sh`

## Qué hace `setup.sh`

1. **Carga `.env`** con `set -a; source .env; set +a` y exporta
   `TF_VAR_databricks_host` / `TF_VAR_databricks_token` para que Terraform
   los recoja automáticamente. El `databricks` CLI no necesita el prefijo
   `TF_VAR_`: lee `DATABRICKS_HOST`/`DATABRICKS_TOKEN` directo del entorno.
2. **Descarga los 7 días de AIS de junio 2023** (`datos/raw/`) con reintentos
   (`--retry 5 --retry-all-errors`) y resume de descargas cortadas (`-C -`),
   forzando HTTP/1.1 porque el servidor de NOAA no negocia bien HTTP/2 con
   curl. Si un archivo ya existe, se salta la descarga — corridas repetidas
   no vuelven a bajar ~6.3GB de nuevo.
3. **Chunkea y comprime cada CSV en paralelo** (`datos/chunked/`): parte cada
   archivo (~900MB) en pedazos de ~100MB con `split -C` (corta por líneas
   completas, nunca a mitad de una fila), le vuelve a pegar el header a cada
   pedazo, y lo comprime con `gzip`. Los 7 días se procesan en paralelo
   (`&` + `wait` por PID) en vez de uno por uno.
4. **Aplica Terraform** (`infra/`): crea catálogo/schemas/volumen en Unity
   Catalog y expone el path del volumen como output.
5. **Sube todo a Databricks** con un solo `databricks fs cp -r datos/chunked
   dbfs:<volumen> --concurrency 12`.

## Por qué está partido en chunks en vez de subir los CSV enteros

El primer intento subía los 7 CSV completos vía Terraform
(`databricks_file` resource, ver el bloque comentado en `infra/main.tf`).
Eso crasheaba el provider (`plugin6.(*GRPCProvider).PlanResourceChange`)
porque intenta leer/hashear archivos de ~1GB enteros en memoria.

Sacar la subida de Terraform y hacerla con el CLI tampoco alcanzaba: medimos
~450ms de latencia (TTFB) contra el workspace de Databricks, y `databricks fs
cp` sube secuencial — con un solo archivo grande por llamada no hay nada que
paralelizar. El cuello de botella no era ancho de banda (con Cloudflare el
container sostenía ~350Mbps sin problema) sino la cantidad de round-trips.

El flag `--concurrency` del CLI (default 8) paraleliza **operaciones de
copia distintas**, no chunks internos de un mismo archivo. Por eso: en vez
de subir 7 archivos grandes, subimos ~63 archivos chicos (`.csv.gz`, ~100MB
cada uno) en un único `fs cp -r`, y ahí sí `--concurrency` tiene qué repartir
entre varias conexiones. Resultado medido: de un estimado de ~10-15min
secuencial a **~2min** reales.

## Terraform (`infra/`)

- `variables.tf` — `catalog_name`, `schemas`, `volumes` con defaults; `files`
  es la plantilla `{ volume, path }` con un placeholder literal `$()` en el
  path.
- `locals.tf` — `file_names` arma un **mapa** (no lista — `for_each` no
  acepta listas/tuplas) de 7 entradas, una por día, reemplazando `$()` en
  `var.files.path` por el número de día con cero-relleno vía `replace()` +
  `format("%02d", ...)`.
- `main.tf` — instancia el módulo `catalog` (schemas + volumen managed). El
  `resource "databricks_file"` que subía datos vía Terraform quedó
  deshabilitado (ver antipatrón arriba); `local.file_names` queda sin uso
  mientras tanto.
- `outputs.tf` — `volume_path` reexporta `module.catalog.volume_paths`
  (el mapa `{volumen => path}` del módulo) para que `setup.sh` pueda hacer
  `terraform output -json volume_path | jq -r '.landing'`.
- `modules/catalog/` — crea los `databricks_schema` y `databricks_volume`
  (el `databricks_catalog` está comentado: con cuenta free no se puede
  setear el Default Storage vía Terraform, así que el catálogo se crea
  manualmente en la UI).

## Devcontainer

`workspaceFolder` en `.devcontainer/devcontainer.json` está hardcodeado a
`/workspace/ddia` (monta `${localWorkspaceFolder}/../` como `/workspace`).
Si esta carpeta vive junto a otro proyecto hermano y ese valor no coincide
con el nombre real de la carpeta, VS Code te termina abriendo el proyecto
vecino en vez de este — pasó al copiar el devcontainer desde otro repo
(`modelamiento`) sin actualizar ese path. Cualquier cambio al `dockerfile`
o al `devcontainer.json` requiere **Rebuild Container**, no alcanza con
reabrir.

## Antipatrón para la clase

El punto central para justificar esto en sistemas distribuidos: la
"lentitud" no era falta de cómputo distribuido, era latencia de red en
operaciones secuenciales de una sola conexión — la distinción clásica entre
*bandwidth-bound* y *latency-bound*. Hacer esta orquestación IO-bound (bajar
archivos, subirlos) desde un notebook corriendo en el driver de un cluster
Databricks sería el antipatrón real: no aprovecha el cluster para nada, solo
paga cómputo caro para esperar sockets. Por eso vive en `setup.sh`, fuera
del notebook, y la paralelización que sí importa (chunking en CPU, subida
por conexiones concurrentes) se resolvió a nivel de shell/CLI antes de que
los datos lleguen a Spark.
