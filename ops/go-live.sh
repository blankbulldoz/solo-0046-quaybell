#!/usr/bin/env bash
# Quaybell 滚动上线命令（带防误连护栏）。
#
# 必填环境变量：
#   PORT       本进程监听端口（如 18091）
#   DB         本进程要打开的 SQLite 路径
#   CLUSTER    共同库集群标识（init-cluster 时设定）
#   FENCE      ops/qb_rollout.py fence 生成的栅栏 JSON
# 可选：
#   COMMON_DB  权威共同库路径（默认等于 DB；DB 是显式 --db 时建议都写）
#   RELEASE    发布代号，写入 X-Quaybell-Release 响应头
#
# 护栏不通过时 python 进程以 2/3/4/5/6 退出，绝不会在误连库上启动服务。
# 例：
#   PORT=18091 DB=/srv/qb/common.sqlite3 COMMON_DB=/srv/qb/common.sqlite3 \
#   CLUSTER=prod-a FENCE=/srv/qb/fence-18091.json RELEASE=candidate-r053 \
#     ops/go-live.sh
set -euo pipefail

: "${PORT:?need PORT}"
: "${DB:?need DB}"
: "${CLUSTER:?need CLUSTER}"
: "${FENCE:?need FENCE}"
COMMON_DB="${COMMON_DB:-$DB}"
RELEASE="${RELEASE:-unversioned}"

cd "$(dirname "$0")/.."
export QUAYBELL_RELEASE="$RELEASE"

exec python3 -m quaybell \
  --host 127.0.0.1 \
  --port "$PORT" \
  --db "$DB" \
  --common-db "$COMMON_DB" \
  --expect-cluster "$CLUSTER" \
  --require-fence "$FENCE" \
  "$@"
