# Reacción a resultados — Alpha Vantage / Airflow

Pipeline del Proyecto Integrador de Ciencia de Datos (UTN FRM). Guarda respuestas
originales en bronze y produce eventos trimestrales, precios y calendario en plata.
Toda la lógica está en `dags/earnings_ingest.py`; no hay módulos propios auxiliares.
No calcula CAR ni ratios: corresponden a la futura capa oro.

## Puesta en marcha

El proyecto conserva Astro Runtime `3.3-7` del Dockerfile original. Requiere Docker
y Astro CLI para ejecutar Airflow. Para la CLI sin Airflow, usar Python 3.12+ y
`pip install -r requirements.txt` dentro de un entorno virtual.

1. Completar el archivo `.env` creado en la raíz con claves propias:

   ```dotenv
   ALPHAVANTAGE_API_KEYS=["primera_clave","segunda_clave","tercera_clave"]
   AV_MIN_REQUEST_INTERVAL_SECONDS=1.1
   AV_RATE_COOLDOWN_SECONDS=65
   AV_DAILY_COOLDOWN_SECONDS=86400
   ```

   El array vacío inicial evita utilizar credenciales inventadas. `.env`, los
   datos y los logs están excluidos de Git. `.env.example` sí se versiona.
   Astro carga `.env`; la CLI también lo carga sin reemplazar variables de entorno.

2. Desde esta carpeta ejecutar `astro dev start`. Abrir la dirección de Airflow
   que muestra Astro, habilitar `earnings_ingest` y disparar una prueba con:

   ```json
   {"mode":"subset","force":false,"solo_plata":false,"outputsize":"compact","incluir_consenso":false}
   ```

   `subset` usa únicamente NVIDIA (`NVDA`) y SPY como benchmark. Es el modo predeterminado. No se presenta como entrega
   académica: publica en `prueba_subset_<fecha>/` y exige al menos un evento.

3. Para cargar el universo completo usar `mode="full"`. Mantener `compact` con
   claves gratuitas; `outputsize="full"` requiere acceso premium. La prueba usa
   `compact` como valor predeterminado tanto en Airflow como en la CLI.
   Una respuesta de falta de permisos no se guarda ni se considera cuota agotada.

4. Habilitado en Airflow, corre diariamente (`@daily`, UTC, sin catchup). Revisar
   los logs y `include/output/silver/validacion.json`. No se publica una entrega
   hasta satisfacer todos los controles. Si todavía falta el bronze básico, la
   corrida queda sin entrega y marca refinamiento/validación/publicación como
   `skipped`; el motivo y los archivos faltantes quedan en
   `include/output/state/ingesta_subset.json` (o `ingesta_full.json`).

Cambiar `.env` requiere reiniciar los servicios si Astro ya estaba ejecutándose.
Este repositorio no incluye claves reales ni datos de mercado descargados.

## Ejecución directa y reprocesamiento

Todos estos comandos se ejecutan desde la raíz del proyecto:

```sh
# Planifica sin requests ni escritura de datos.
python dags/earnings_ingest.py --plan --mode full --outputsize compact

# Prueba de ingesta y refinamiento.
python dags/earnings_ingest.py --mode subset --outputsize compact

# Reprocesa exclusivamente archivos locales: no necesita claves ni red.
python dags/earnings_ingest.py --solo-plata --mode subset

# Validación de entrega completa: más de 1.000 eventos.
python dags/earnings_ingest.py --solo-plata --mode full

# Incorporar las cuatro columnas de consenso cuando haya capturas previas.
python dags/earnings_ingest.py --solo-plata --mode full --incluir-consenso
```

En Airflow usar `solo_plata=true` para el mismo reprocesamiento. No ejecutar la CLI
y Airflow simultáneamente sobre `include/output`: las tareas Airflow se protegen
con `max_active_runs=1`, la CLI con su propio lock, pero no comparten una
transacción de refinamiento. Los workers deben compartir el mismo volumen local;
los locks de archivo no sustituyen una coordinación distribuida sobre S3.

## Rotación de claves

- Se intenta la clave activa hasta recibir HTTP 429 o un mensaje de cuota en
  `Information`, `Note` o `Error Message`, incluso con HTTP 200.
- Se registra un enfriamiento y se reintenta **la misma descarga** con la próxima
  clave disponible del array. Cada clave se prueba como máximo una vez por llamada.
- `Retry-After` tiene prioridad cuando está presente. Sin él se usan 65 segundos
  para límites breves y 86.400 para límites diarios, ambos configurables. El
  enfriamiento diario es conservador desde la respuesta: no supone una hora de
  reinicio del proveedor. Se debe adaptar a la cuota contratada.
- La posición activa, el último uso y los enfriamientos se guardan en
  `include/output/state/api_keys.json` bajo lock; sobreviven a lotes, reintentos y
  reinicios. El estado guarda hashes de claves, no sus valores.
- Si todas están limitadas, se corta el lote sin espera infinita. Los archivos
  faltantes siguen pendientes. Se puede refinar el bronze acumulado; la
  validación decide si alcanza para publicar.
- Errores de conexión/servidor permiten reintentos de Airflow. Los archivos ya
  obtenidos no se descargan otra vez. Errores de permisos, símbolo o esquema no
  se almacenan; el log indica que la respuesta fue rechazada.
- Para no consumir toda una cuota pequeña refrescando las primeras empresas,
  las descargas sin snapshot tienen prioridad; luego se actualiza lo más antiguo.

No se asigna una cuota hipotética por anticipado: la rotación responde al límite
que devuelve la API. Usar sólo claves propias con acceso habilitado a los endpoints.

## Bronze y logs de lo obtenido

```text
include/
  universo/universo.csv
  output/
    bronze/EARNINGS/ticker=IBM/snapshot=2026-09-08.json
    bronze/TIME_SERIES_DAILY/ticker=IBM/snapshot=2026-09-08.json
    logs/bronze.jsonl
    state/api_keys.json
    silver/
      slv_eventos.csv
      slv_precios.csv
      slv_calendario.csv
      linaje.json
      validacion.json
    entrega_2026-09-08/
```

Cada descarga válida genera un mensaje `BRONZE_OBTENIDO` en el log de ejecución
y una línea JSON persistente en `logs/bronze.jsonl`: endpoint, ticker, fecha UTC de
obtención, snapshot, ruta, bytes, SHA-256, cantidad de registros y rango de fechas
(o campos recibidos para OVERVIEW). Ejemplo ilustrativo del formato:

```json
{"event":"bronze_obtenido","endpoint":"EARNINGS","ticker":"IBM","snapshot":"2026-09-08","records":100,"date_min":"2001-06-30","date_max":"2026-03-31","path":".../snapshot=2026-09-08.json"}
```

La cantidad corresponde al bloque que se usará en plata (`quarterlyEarnings`,
`quarterlyReports`, `estimates`, `data` o serie diaria). El JSON completo de la
fuente se conserva exactamente como llegó, incluidos bloques anuales no usados.
Los logs y XCom no contienen payloads completos ni claves. Un archivo existente
no se vuelve a registrar como una descarga nueva.

La escritura usa archivo temporal y reemplazo atómico bajo lock. Bronze es
inmutable: `force=true` ignora TTL, pero **no sobrescribe el snapshot del mismo
día**. Una segunda corrida el mismo día, con todos los ítems completos, no hace
requests, ni siquiera al sensor. Si se cambia de `compact` a `full` el mismo día,
la captura existente se conserva; usar la próxima fecha de captura.

TTL en días: EARNINGS y estados contables 80; SPLITS 90; OVERVIEW 30;
EARNINGS_ESTIMATES y TIME_SERIES_DAILY 1. La fecha del snapshot es la fecha real de
captura UTC. La fecha de entrega sale de `logical_date` o `run_after` para admitir
corridas manuales sin intervalo.

## Plata y decisiones del plan

- `slv_eventos.csv`: clave `(ticker, fiscal_quarter_end)`. Se usa el último snapshot
  histórico de cada endpoint y se deduplica antes de escribir; `reported_date`
  conserva su papel de dato corregible, nunca de clave.
- Se descartan EPS reportado/estimado faltantes, fechas inválidas y estimados de
  valor absoluto menor que 0,05. El horario queda `pre-market`, `post-market` o
  nulo. Se excluyen resultados anuales.
- La proporción de sorpresa cero se mide globalmente sobre eventos deduplicados.
  Si supera 3 %, se descartan esos eventos según el plan y se registra cuántos.
  Es una heurística del plan: no demuestra por sí sola que la fuente rellenó datos.
- Los tres estados contables se unen por cierre fiscal. Se guardan niveles y el
  signo original de capex. No se imputan dividendos nulos automáticamente: pueden
  representar ausencia de pago o falta de cobertura.
- OVERVIEW aporta la clasificación y acciones actuales; el sector de la semilla
  sólo sirve para estratificar, no rellena valores ausentes de la fuente.
- `snapshot_date` identifica la captura de EARNINGS. `linaje.json` documenta además
  el último snapshot usado de cada endpoint/ticker; los consensos se reconstruyen
  desde todas las capturas y la regla temporal.
- `slv_precios.csv`: clave `(ticker,date)`, OHLCV original y cierre ajustado
  exclusivamente por splits posteriores al día. Se acumulan todos los snapshots
  diarios; el más reciente resuelve fechas superpuestas. Se soportan splits
  inversos. SPLITS vacío significa factor 1; SPLITS ausente deja `close_adj` nulo
  y la validación lo rechaza, para evitar aparentar un ajuste inexistente.
- `slv_calendario.csv`: fechas únicas ordenadas exclusivamente de SPY y `t`
  consecutivo desde cero.

### Consenso: 23 columnas iniciales, 27 cuando exista cobertura

La API devuelve `estimates`, con horizontes `fiscal quarter` y `fiscal year`.
Se guardan todos los snapshots bronze, pero plata sólo usa los trimestrales.
Para cada evento se exige coincidencia de cierre fiscal y se elige la captura
más reciente **estrictamente anterior** al anuncio. Una captura del mismo día
no se usa porque no tenemos hora de captura histórica comparable con el anuncio.
Nunca se rellenan eventos históricos con consenso actual.

Se implementaron ambas alternativas de la sección 6 del plan:

- `incluir_consenso=false` (predeterminado): 23 columnas; se retiran únicamente
  `eps_estimate_high`, `eps_estimate_low`, `eps_estimate_analyst_count` y
  `revenue_estimate_average`. El consenso se sigue recolectando en bronze.
- `incluir_consenso=true`: esquema completo de 27 columnas. Falla si una de esas
  columnas sigue 100 % nula, igual que para cualquier otra columna.

El dataset de 23 columnas mantiene el EPS estimado original de EARNINGS
(`consensus_eps`) y la sorpresa reportada. Se puede activar el enriquecimiento
cuando haya anuncios posteriores a las capturas. Este cambio evita una entrega
inicial necesariamente vacía en esas cuatro columnas, sin inventar historia.

### Validación y publicación

La validación registra siempre el perfil de nulos y sus causas en el log y en
`validacion.json`. Controla esquema, claves de las tres tablas, campos numéricos,
fechas, EPS, columnas vacías, volumen, lag mediano de 10–90 días, anuncios previos
al cierre fiscal, OHLC, precios positivos, presencia de SPY, hasta 10 % de empresas
sin precios y correspondencia exacta del calendario con SPY.

Antes de refinar se comprueba que exista el bronze de SPY (precios y splits) y
al menos una empresa con resultados, estados contables, OVERVIEW, precios y splits.
Si falta esa base, se registra `INGESTA_PENDIENTE` y se omiten refinamiento,
validación y publicación. No se escriben CSV vacíos ni se pisan los CSV anteriores.
El informe `state/ingesta_<mode>.json` indica `estado: esperando_datos`, endpoints
faltantes y próxima disponibilidad local de las claves. El estado verde del DAG
con tareas `skipped` sólo indica una espera manejada; **no significa que se haya
validado o publicado un dataset**. La CLI sale con código 2 para esta condición.

Los archivos presentes pero inválidos siguen siendo un error. Una vez que existe
la base de bronze, se aplican todos los controles de calidad y volumen; no se
convierte una validación fallida en `skipped`. `validar` no reintenta los mismos
CSV: un error de calidad falla inmediatamente y conserva su diagnóstico. Los
reintentos de descarga ante problemas de transporte se mantienen.

`full` exige **al menos 1.001 eventos**; `subset` exige uno para verificar el flujo.
El número de eventos real depende de la cobertura, no está garantizado por la
semilla de 150 tickers. Se registra cuántos eventos quedan fuera del rango temporal
de precios del benchmark; no se presenta ese dato como cobertura suficiente de
CAR. La futura capa oro deberá exigir todas las ruedas de cada ventana.

`guardar` verifica los hashes de los tres CSV contra la validación antes de copiar
los CSV y los dos informes a una carpeta de entrega, publicada atómicamente.
Una validación fallida no crea entrega. Una entrega del mismo día con idénticos CSV
es idempotente; una distinta falla conservando la anterior.

El grafo tiene nueve tareas (el texto del plan dice ocho, pero enumera nueve):
sensor, branch, short-circuit, lotes, descarga mapeada, fallback, refinamiento,
validación y publicación. La descarga y el sensor son los únicos accesos de red.
`ALL_DONE` permite la rama de respaldo cuando el sensor termina skipped/failed;
`NONE_FAILED_MIN_ONE_SUCCESS` permite refinar cualquiera de las ramas válidas.
Sin trabajo, el short-circuit omite el resto y termina en verde.

## Universo y limitaciones

`include/universo/universo.csv` congela 150 símbolos seleccionados manualmente en
11 sectores, con empresas de distinta escala. El modo `full` usa toda la semilla;
el modo `subset` selecciona solamente `NVDA`, independientemente del orden del CSV. No pretende representar un índice histórico ni contener tamaños
verificados en la fecha de cada anuncio. Los símbolos pueden cambiar; respuestas
no válidas quedan pendientes y deben revisarse en los logs. No hay scraping.

Para oro: desplazar fundamentales a t-1 y comprobar disponibilidad real; avanzar
`post-market` a la siguiente rueda; calcular CAR [-1,+1] frente a SPY; construir
ratios e interacciones con sorpresa. OVERVIEW actual (incluidas acciones) no es
información histórica point-in-time y no debe usarse como tamaño previo a anuncios
antiguos sin una fuente temporal adecuada. SPY es un ETF proxy; `close_adj` no
incluye dividendos. Esas limitaciones se mantienen explícitas.

## Pruebas

```sh
# Astro: usa el Airflow del contenedor e incluye pruebas del grafo.
astro dev pytest

# Entorno local: pruebas offline de ingesta, parsers y flujo; el grafo se
# prueba también cuando Airflow 3 está instalado (en caso contrario se omite).
pip install -r requirements-dev.txt
python -m pytest tests/test_earnings_ingest.py tests/dags/test_earnings_dag.py -q
```

Las pruebas usan datos sintéticos identificados como tales y directorios
temporales: no consumen cuota ni crean un bronze de prueba en el output real.
Cubren rotación por cuota HTTP 200/429, concurrencia, cooldown persistente,
reintentos, secretos, TTL, logs, deduplicación, consenso temporal, splits normales
e inversos, validación y entrega completa con 1.050 eventos sintéticos.

El DAG de ejemplo de astronautas original se conserva como referencia pero se
excluye con `dags/.airflowignore`.

## Referencias de contrato

Esquemas comprobados con las respuestas públicas demo de IBM el 2026-09-08:
`EARNINGS_ESTIMATES.estimates[].date/horizon/eps_estimate_high/...`,
`SPLITS.data[].effective_date/split_factor` y
`EARNINGS.quarterlyEarnings[].reportedDate/estimatedEPS/reportTime`.

- [Documentación oficial Alpha Vantage](https://www.alphavantage.co/documentation/)
- [Ejemplo de consenso IBM](https://www.alphavantage.co/query?function=EARNINGS_ESTIMATES&symbol=IBM&apikey=demo)
- [Ejemplo de splits IBM](https://www.alphavantage.co/query?function=SPLITS&symbol=IBM&apikey=demo)
- [Ejemplo de resultados IBM](https://www.alphavantage.co/query?function=EARNINGS&symbol=IBM&apikey=demo)
