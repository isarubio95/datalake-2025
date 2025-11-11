#!/usr/bin/env bash
set -euo pipefail

# ==== AJUSTA ESTAS 3 VARIABLES SI LO NECESITAS ====
BUCKET="bbtwins-test"
ROOT_PREFIX="Data"                # raíz donde cuelgan tus namespaces/ tablas
SPARK_CONT="${COMPOSE_PROJECT_NAME:-ngods}-spark-thrift"  # nombre real del contenedor spark-thrift

# Si quieres limitar a una rama concreta, por ejemplo portesa/uploads:
# ROOT_PREFIX="Data/portesa/uploads"

ROOT="s3://${BUCKET}/${ROOT_PREFIX}"

echo "Escaneando tablas Iceberg bajo ${ROOT} ..."
# Listado de nivel 1 (namespaces o primeras carpetas)
# Vamos a recorrer recursivamente todas las carpetas que contengan "metadata/"
all_table_roots=()

# Función recursiva: busca carpetas con /metadata/ dentro
find_tables() {
  local prefix="$1"            # s3://bucket/loquesea/
  # Lista subcarpetas directas
  while read -r line; do
    name=$(awk '{print $2}' <<<"$line")
    [[ -z "$name" ]] && continue
    sub="s3://${BUCKET}/${prefix#s3://${BUCKET}/}${name}"
    # ¿Tiene metadata/?
    if aws s3 ls "${sub}metadata/" --recursive --summarize >/dev/null 2>&1; then
      all_table_roots+=("${sub}")
    else
      # sigue bajando
      find_tables "${sub}"
    fi
  done < <(aws s3 ls "$prefix" | grep "PRE" || true)
}

find_tables "${ROOT}/"

echo "Detectadas ${#all_table_roots[@]} candidatas."
# Dry-run listado
for troot in "${all_table_roots[@]}"; do
  echo "  - ${troot}"
done

# Esperar a que el metastore esté accesible (simple)
echo "Comprobando Hive Metastore en thrift://metastore:9083 ..."
# pequeño retry
for i in {1..20}; do
  if docker exec "$SPARK_CONT" bash -lc "nc -z metastore 9083" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

# Registrar cada tabla:
# Regla: convertimos la ruta s3://bucket/Data/<ns...>/<table>/ en:
#   schema_name = '<ns...>' con puntos (ej: portesa.uploads)
#   table_name  = '<table>'
register_count=0
skip_count=0

for troot in "${all_table_roots[@]}"; do
  # Quita "s3://bucket/"
  rel="${troot#s3://${BUCKET}/}"           # p.ej. Data/portesa/uploads/gemelo_pedidos/
  # Quita el ROOT_PREFIX
  rel="${rel#${ROOT_PREFIX}/}"            # p.ej. portesa/uploads/gemelo_pedidos/
  # Elimina trailing slash
  rel="${rel%/}"

  IFS='/' read -r -a parts <<< "$rel"
  n=${#parts[@]}
  if (( n < 2 )); then
    echo "  [omitir] No puedo inferir schema/table de: ${troot}"
    continue
  fi

  table_name="${parts[$((n-1))]}"
  # namespace = todo menos el último, unidos por '.'
  namespace=$(IFS='.'; echo "${parts[*]:0:$((n-1))}")

  echo "Registrando: ${namespace}.${table_name}"
  # Creamos namespaces si no existen (Spark ignora si ya existen)
  docker exec -i "$SPARK_CONT" /opt/spark/bin/spark-sql -S <<SQL || { echo "  [error] ${namespace}.${table_name}"; continue; }
CREATE NAMESPACE IF NOT EXISTS ${parts[0]};
-- Si el namespace tiene varios niveles, asegúrate de crearlos; Spark/ Iceberg los crea implícitamente al registrar,
-- pero añadimos el más común de dos niveles:
$( (( n>2 )) && echo "CREATE NAMESPACE IF NOT EXISTS ${parts[0]}.${parts[1]};" )
CALL iceberg.system.register_table(
  schema_name   => '${namespace}',
  table_name    => '${table_name}',
  table_location=> 's3a://${BUCKET}/${ROOT_PREFIX}/${rel}'
);
SQL

  if [[ $? -eq 0 ]]; then
    echo "  [ok] ${namespace}.${table_name}"
    ((register_count++))
  else
    echo "  [skip] ${namespace}.${table_name}"
    ((skip_count++))
  fi
done

echo "Registro completado. OK=${register_count}  SKIP/ERR=${skip_count}"
