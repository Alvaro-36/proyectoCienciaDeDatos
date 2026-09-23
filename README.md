# Reacción a resultados — Yahoo Finance + SEC EDGAR / Airflow

Pipeline del Proyecto Integrador de Ciencia de Datos (UTN FRM). Guarda respuestas
originales en bronze y produce eventos trimestrales, precios y calendario en plata.
Toda la lógica está en `dags/earnings_ingest.py`; no hay módulos propios auxiliares.
No calcula CAR ni ratios: corresponden a la futura capa oro.

## Puesta en marcha

El proyecto conserva Astro Runtime `3.3-7` del Dockerfile original. Requiere Docker
y Astro CLI para ejecutar Airflow. Para la CLI sin Airflow, usar Python 3.12+ y
`pip install -r requirements.txt` dentro de un entorno virtual.

1. Completar el archivo `.env` creado en la raíz (guía en `.env.example`). No
   hace falta clave ni API key; sólo definir `SEC_USER_AGENT` con un contacto
   real para cumplir la política de SEC EDGAR:

   ```dotenv
   SEC_USER_AGENT="UTN-FRM-CienciaDatos/1.0 (contacto: estudiante@example.com)"
   ```

   El bloque `ALPHAVANTAGE_API_KEYS` de `.env.example` es legado de la versión
   original del pipeline y el código actual no lo lee. `.env`, los datos y los
   logs están excluidos de Git. `.env.example` sí se versiona. Astro carga
   `.env`; la CLI también lo carga sin reemplazar variables de entorno.

2. Desde esta carpeta ejecutar `astro dev start`. Abrir la dirección de Airflow
   que muestra Astro, habilitar `earnings_ingest` y disparar una prueba con:

   ```json
   {"mode":"subset","force":false,"solo_plata":false}
   ```

   `subset` usa únicamente NVIDIA (`NVDA`) y SPY como benchmark. Es el modo
   predeterminado. No se presenta como entrega académica: publica en
   `prueba_subset_<fecha>/` y exige al menos un evento.

3. Para cargar el universo completo usar `mode="fullset"` (los 906 símbolos de la
   semilla). No hay claves ni `outputsize`: las fuentes son gratuitas
   (Yahoo Finance + SEC EDGAR). Respuestas no válidas no se guardan y quedan
   pendientes para revisión en los logs.

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
python dags/earnings_ingest.py --plan --mode fullset

# Prueba de ingesta y refinamiento.
python dags/earnings_ingest.py --mode subset

# Reprocesa exclusivamente archivos locales: no necesita red.
python dags/earnings_ingest.py --solo-plata --mode subset

# Validación de entrega completa: más de 1.000 eventos.
python dags/earnings_ingest.py --solo-plata --mode fullset
```

En Airflow usar `solo_plata=true` para el mismo reprocesamiento. No ejecutar la CLI
y Airflow simultáneamente sobre `include/output`: las tareas Airflow se protegen
con `max_active_runs=1`, la CLI con su propio lock, pero no comparten una
transacción de refinamiento. Los workers deben compartir el mismo volumen local;
los locks de archivo no sustituyen una coordinación distribuida sobre S3.

## Límites de las fuentes

El pipeline actual no usa claves: Yahoo Finance limita por IP y SEC EDGAR por
User-Agent (10 req/s, sin autenticación). Pautas implementadas:

- Cada descarga respeta un ritmo global y los errores de conexión/servidor
  permiten reintentos de Airflow. Los archivos ya obtenidos no se descargan otra
  vez.
- Respuestas no válidas (símbolo desconocido, esquema, sin CIK en `cik.json`) no
  se almacenan y quedan pendientes; el log indica el rechazo (BRONCE_RECHAZADO,
  EDGAR_SIN_CIK).
- Para no refrescar siempre las primeras empresas, las descargas sin snapshot
  tienen prioridad; luego se actualiza lo más antiguo.
- SEC EDGAR es un servicio público: usar un `SEC_USER_AGENT` identificable con
  contacto real y no exceder ~10 req/s agregados.

La sección "Rotación de claves" de versiones anteriores (Alpha Vantage) quedó
obsoleta: el código actual no rota claves ni escribe `state/api_keys.json`.

## Bronze y logs de lo obtenido

```text
include/
  universo/universo.csv
  output/
    bronze/EARNINGS/ticker=IBM/snapshot=2026-09-08.json
    bronze/FUNDAMENTALES/ticker=IBM/snapshot=2026-09-08.json
    bronze/EDGAR/ticker=IBM/snapshot=2026-09-08.json
    bronze/PERFIL/ticker=IBM/snapshot=2026-09-08.json
    bronze/PRECIOS/ticker=IBM/snapshot=2026-09-08.json
    logs/bronze.jsonl
    state/ingesta_full.json
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
(o campos recibidos para PERFIL y EDGAR). Ejemplo ilustrativo del formato:

```json
{"event":"bronze_obtenido","endpoint":"EARNINGS","ticker":"IBM","snapshot":"2026-09-08","records":100,"date_min":"2001-06-30","date_max":"2026-03-31","path":".../snapshot=2026-09-08.json"}
```

La cantidad corresponde a lo que se usará en plata: filas de resultados
(EARNINGS), periodos contables (FUNDAMENTALES y EDGAR) o ruedas de precios
(PRECIOS). El JSON completo de Yahoo se conserva exactamente como llegó,
incluidos bloques anuales no usados. Para EDGAR, bronze guarda los hechos
10-Q/10-K utilizados (cada valor con tag, form y fecha de filing en `fuentes`),
no el companyfacts completo de la SEC: auditables y livianos. Los logs y XCom no
contienen payloads completos ni claves. Un archivo existente no se vuelve a
registrar como una descarga nueva.

La escritura usa archivo temporal y reemplazo atómico bajo lock. Bronze es
inmutable: `force=true` ignora TTL, pero **no sobrescribe el snapshot del mismo
día**. Una segunda corrida el mismo día, con todos los ítems completos, no hace
requests, ni siquiera al sensor. Si se cambia de `compact` a `full` el mismo día,
la captura existente se conserva; usar la próxima fecha de captura.

TTL en días: EARNINGS 7; PERFIL 30; FUNDAMENTALES 30; EDGAR 30; PRECIOS 1. EDGAR
se refresca mensualmente: los hechos de un trimestre quedan cerrados salvo
restatements (10-K/A, 10-Q/A) que conviene recoger. La fecha del snapshot es la
fecha real de captura UTC. La fecha de entrega sale de `logical_date` o
`run_after` para admitir corridas manuales sin intervalo.

Las descargas se reparten en lotes de hasta 100 ítems (`MAX_ITEMS_POR_LOTE`):
cada tarea mappeada de `bajar_bronce` termina en minutos y, si un límite de
tiempo del entorno la corta, el bronze ya guardado persiste y la corrida
siguiente continúa sin repetir nada. El ritmo entre descargas es por fuente
(`ritmo_segundos`): SEC EDGAR respira más rápido (hasta 10 req/s) que Yahoo.

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
- Los fundamentales de Yahoo ganan donde existen (últimos 5-7 trimestres) y
  SEC EDGAR completa los trimestres históricos que Yahoo no publica.
- PERFIL aporta la clasificación y las acciones actuales; EDGAR puede aportar
  las acciones del trimestre cuando la empresa las declara en sus 10-Q. El
  sector de la semilla sólo sirve para estratificar, no rellena valores ausentes.
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

Antes de refinar se comprueba que exista el bronze de SPY (precios) y al menos
una empresa con resultados, estados contables, EDGAR, PERFIL y precios.
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

`fullset` exige **al menos 1.001 eventos**; `subset` exige uno para verificar el flujo.
El número de eventos real depende de la cobertura, no está garantizado por la
semilla de 906 tickers. Se registra cuántos eventos quedan fuera del rango temporal
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

`include/universo/universo.csv` congela 906 símbolos: el S&P 500 de la semilla
más los constituyentes del S&P MidCap 400 (los ya presentes no se duplican).
Los sectores son GICS (11) salvo tres filas históricas marcadas como
`Technology`. El modo `full` usa toda la semilla; el modo `subset` selecciona
solamente `NVDA`, independientemente del orden del CSV. No pretende representar
un índice histórico ni contener tamaños verificados en la fecha de cada anuncio.
Los símbolos pueden cambiar; respuestas no válidas quedan pendientes y deben
revisarse en los logs. No hay scraping.

### Cobertura temporal de fundamentales (SEC EDGAR)

Yahoo sólo publica 5-7 trimestres de estados contables por empresa; SEC EDGAR
cubre los 10-Q/10-K desde la era XBRL (~2009) y alimenta los mismos 9 campos.
`include/universo/cik.json` mapea ticker -> CIK (snapshot del índice SEC, que
puede regenerarse desde `company_tickers.json`). Limitaciones a tener presentes:

- Los 10-Q declaran los items de resultado acumulados dentro del año fiscal
  (YTD): EDGAR los desacumula por trimestre y no fabrica un trimestre cuando
  queda un hueco mayor a 120 días en la cadena de periodos.
- Los cierres fiscales de 52/53 semanas (ej. `2026-06-27`) se normalizan a fin
  de mes para coincidir con `fiscal_quarter_end`.
- Ante restatements o migración de tags (ASC 606) manda el filing más reciente
  del periodo; cada valor del bronze guarda tag, form y fecha de filing.
- Emisores privados extranjeros pueden tener menos 10-Q o filar sólo por año;
  sus trimestres históricos pueden quedar incompletos.
- Las financieras siguen sin desglose corriente/no corriente: no completan los 9
  campos y sus eventos se descartan, igual que hoy con Yahoo.
- `shares_outstanding` histórico no siempre se declara en los 10-Q: cuando falta,
  el evento usa las acciones actuales de PERFIL.
- `EA` no figura en el snapshot del índice SEC (su CIK se agregó a mano en
  `cik.json`); si una empresa falta del índice, su descarga EDGAR queda pendiente
  y debe revisarse en los logs.

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
python -m pytest tests/test_earnings_ingest.py tests/test_edgar_ingest.py tests/dags/test_earnings_dag.py -q
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
