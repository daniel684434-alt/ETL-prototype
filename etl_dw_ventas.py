

import os
import sqlite3
import pandas as pd

# ---------------------------------------------------------------------------
# CONFIGURACIÓN GLOBAL
# ---------------------------------------------------------------------------
# Ajusta esta variable según el nombre real de tu archivo fuente.
ARCHIVO_FUENTE_XLSX = "Online Retail.xlsx"
ARCHIVO_FUENTE_CSV  = "online_retail.csv"
BASE_DATOS          = "dw_ventas.db"

# Valor sustituto para Customer ID nulos
CLIENTE_DESCONOCIDO = 0

# ---------------------------------------------------------------------------
# UTILIDADES
# ---------------------------------------------------------------------------

def separador(titulo: str) -> None:
    """Imprime un separador visual para identificar cada etapa en consola."""
    print(f"\n{'='*70}")
    print(f"  {titulo}")
    print(f"{'='*70}")


# =============================================================================
# ETAPA 1 — EXTRACCIÓN
# =============================================================================
# Se lee el archivo fuente tal como viene, sin ninguna modificación.
# El DataFrame resultante se llama df_raw y representa la "zona de aterrizaje"
# (landing zone) antes de cualquier validación o limpieza.
# =============================================================================

def extraer_datos() -> pd.DataFrame:
    separador("ETAPA 1 · EXTRACCIÓN")

    if os.path.exists(ARCHIVO_FUENTE_XLSX):
        print(f"[INFO] Leyendo archivo Excel: {ARCHIVO_FUENTE_XLSX}")
        df_raw = pd.read_excel(ARCHIVO_FUENTE_XLSX, dtype=str)
    elif os.path.exists(ARCHIVO_FUENTE_CSV):
        print(f"[INFO] Leyendo archivo CSV: {ARCHIVO_FUENTE_CSV}")
        df_raw = pd.read_csv(ARCHIVO_FUENTE_CSV, dtype=str, encoding="latin-1")
    else:
        raise FileNotFoundError(
            "No se encontró el archivo fuente. "
            f"Coloca '{ARCHIVO_FUENTE_XLSX}' o '{ARCHIVO_FUENTE_CSV}' "
            "en la misma carpeta que este script."
        )

    print(f"[OK]   Filas extraídas : {len(df_raw):,}")
    print(f"[OK]   Columnas        : {list(df_raw.columns)}")
    return df_raw


# =============================================================================
# ETAPA 2 — STAGING
# =============================================================================
# El staging area replica los datos crudos con el mínimo de tipado necesario
# para operar sobre ellos. Aquí NO se aplica lógica de negocio; solo se
# realizan conversiones de tipo y una limpieza básica de estructura.
# Equivale a una tabla temporal en un proceso ETL real.
# =============================================================================

def staging(df_raw: pd.DataFrame) -> pd.DataFrame:
    separador("ETAPA 2 · STAGING")

    df = df_raw.copy()

    # --- Renombrado a nombres operativos (snake_case) -----------------------
    df.columns = [c.strip() for c in df.columns]   # elimina espacios
    df.rename(columns={
        "InvoiceNo"   : "invoice_no",
        "StockCode"   : "stock_code",
        "Description" : "descripcion",
        "Quantity"    : "cantidad",
        "InvoiceDate" : "invoice_date",
        "UnitPrice"   : "precio_unitario",
        "CustomerID"  : "customer_id",
        "Country"     : "country",
    }, inplace=True)

    # --- Conversión de tipos ------------------------------------------------
    df["invoice_date"]    = pd.to_datetime(df["invoice_date"],
                                       errors="coerce")
    df["cantidad"]        = pd.to_numeric(df["cantidad"],       errors="coerce")
    df["precio_unitario"] = pd.to_numeric(df["precio_unitario"], errors="coerce")
    df["customer_id"]     = pd.to_numeric(df["customer_id"],    errors="coerce")

    # --- Eliminar filas sin fecha (no se puede ubicar en el modelo) ----------
    nulos_fecha = df["invoice_date"].isna().sum()
    df.dropna(subset=["invoice_date"], inplace=True)
    print(f"[INFO] Filas eliminadas por fecha nula : {nulos_fecha:,}")
    print(f"[OK]   Filas en staging                : {len(df):,}")
    print(f"[OK]   Rango de fechas                 : "
          f"{df['invoice_date'].min()} → {df['invoice_date'].max()}")
    return df


# =============================================================================
# ETAPA 3 — TRANSFORMACIÓN
# =============================================================================
# Se aplica la lógica de negocio para derivar los atributos del modelo
# dimensional: separar fecha/hora, calcular métricas, codificar indicadores
# y enriquecer dimensiones con atributos derivados.
# =============================================================================

def transformar(df_stg: pd.DataFrame) -> pd.DataFrame:
    separador("ETAPA 3 · TRANSFORMACIÓN")

    df = df_stg.copy()

    # ---- 3a. Descomponer InvoiceDate en columnas de fecha y hora -----------
    df["fecha"]   = df["invoice_date"].dt.date          # solo la parte de fecha
    df["hora"]    = df["invoice_date"].dt.hour
    df["minuto"]  = df["invoice_date"].dt.minute

    # ---- 3b. Indicador de cancelación --------------------------------------
    # Las facturas canceladas en Online Retail comienzan con la letra "C".
    df["indicador_cancelacion"] = df["invoice_no"].str.startswith("C").astype(int)
    canceladas = df["indicador_cancelacion"].sum()
    print(f"[INFO] Transacciones canceladas (C*)  : {canceladas:,}")

    # ---- 3c. Valor bruto ---------------------------------------------------
    df["valor_bruto"] = df["cantidad"] * df["precio_unitario"]

    # ---- 3d. Manejo de Customer ID nulos -----------------------------------
    nulos_cliente = df["customer_id"].isna().sum()
    df["customer_id"] = df["customer_id"].fillna(0)
    df["customer_id"] = df["customer_id"].astype("Int64")
    print(f"[INFO] Customer ID nulos sustituidos  : {nulos_cliente:,} → {CLIENTE_DESCONOCIDO}")

    # ---- 3e. Tipo de cliente (regla de negocio simplificada) ---------------
    # Clientes con ID = 0 se marcan como "Desconocido";
    # los demás como "Registrado". En un DW real podría integrarse un CRM.
    df["tipo_cliente"] = df["customer_id"].apply(
        lambda x: "Desconocido" if x == CLIENTE_DESCONOCIDO else "Registrado"
    )

    # ---- 3f. Región geográfica (mapping básico) ----------------------------
    # Se mapean los países a regiones continentales. Los países no presentes
    # en el diccionario quedan como "Otra Región".
    regiones = {
        # Europa Occidental
        "United Kingdom": "Europa Occidental",
        "France"        : "Europa Occidental",
        "Germany"       : "Europa Occidental",
        "Spain"         : "Europa Occidental",
        "Netherlands"   : "Europa Occidental",
        "Belgium"       : "Europa Occidental",
        "Portugal"      : "Europa Occidental",
        "Switzerland"   : "Europa Occidental",
        "Austria"       : "Europa Occidental",
        "Italy"         : "Europa Occidental",
        "Denmark"       : "Europa Occidental",
        "Sweden"        : "Europa Occidental",
        "Finland"       : "Europa Occidental",
        "Norway"        : "Europa Occidental",
        "Iceland"       : "Europa Occidental",
        "Malta"         : "Europa Occidental",
        "Cyprus"        : "Europa Occidental",
        # Europa del Este
        "Poland"        : "Europa del Este",
        "Czech Republic": "Europa del Este",
        "Lithuania"     : "Europa del Este",
        "EIRE"          : "Europa del Este",
        # América
        "USA"           : "América del Norte",
        "Canada"        : "América del Norte",
        "Brazil"        : "América del Sur",
        # Asia-Pacífico
        "Australia"     : "Asia-Pacífico",
        "Japan"         : "Asia-Pacífico",
        "Singapore"     : "Asia-Pacífico",
        "Hong Kong"     : "Asia-Pacífico",
        "United Arab Emirates": "Medio Oriente",
        "Israel"        : "Medio Oriente",
        "Lebanon"       : "Medio Oriente",
        "Bahrain"       : "Medio Oriente",
        "Saudi Arabia"  : "Medio Oriente",
    }
    df["region"] = df["country"].map(regiones).fillna("Otra Región")

    print(f"[OK]   Transformación completada. Filas resultantes: {len(df):,}")
    return df


# =============================================================================
# ETAPA 4 — CONSTRUCCIÓN DE DIMENSIONES
# =============================================================================
# Cada función genera una dimensión independiente con su clave sustituta
# (surrogate key). Las claves sustitutas son enteros secuenciales sin
# significado de negocio, lo que desacopla el DW de los sistemas fuente.
# =============================================================================

# ---- DIM_FECHA -------------------------------------------------------------

def construir_dim_fecha(df: pd.DataFrame) -> pd.DataFrame:
    """
    Genera la dimensión de fecha descomponiendo el campo 'fecha'.
    Atributos de jerarquía: año → trimestre → mes → semana → día.
    """
    fechas_unicas = pd.DataFrame({"fecha": df["fecha"].unique()})
    fechas_unicas["fecha"] = pd.to_datetime(fechas_unicas["fecha"])

    fechas_unicas["anio"]       = fechas_unicas["fecha"].dt.year
    fechas_unicas["trimestre"]  = fechas_unicas["fecha"].dt.quarter
    fechas_unicas["mes"]        = fechas_unicas["fecha"].dt.month
    fechas_unicas["nombre_mes"] = fechas_unicas["fecha"].dt.month_name()
    fechas_unicas["semana"]     = fechas_unicas["fecha"].dt.isocalendar().week.astype(int)
    fechas_unicas["dia"]        = fechas_unicas["fecha"].dt.day
    fechas_unicas["nombre_dia"] = fechas_unicas["fecha"].dt.day_name()

    fechas_unicas.sort_values("fecha", inplace=True)
    fechas_unicas.reset_index(drop=True, inplace=True)
    fechas_unicas.index += 1                          # clave sustituta desde 1
    fechas_unicas.index.name = "fecha_key"
    fechas_unicas["fecha"] = fechas_unicas["fecha"].dt.date.astype(str)

    print(f"[OK]   DIM_FECHA   → {len(fechas_unicas):,} filas")
    return fechas_unicas.reset_index()


# ---- DIM_HORA --------------------------------------------------------------

def construir_dim_hora(df: pd.DataFrame) -> pd.DataFrame:
    """
    Genera la dimensión de hora con granularidad hora:minuto.
    Agrega 'franja_horaria' como atributo descriptivo (Madrugada,
    Mañana, Tarde, Noche).
    """
    horas_unicas = df[["hora", "minuto"]].drop_duplicates().copy()

    def franja(h: int) -> str:
        if   0  <= h < 6:  return "Madrugada"
        elif 6  <= h < 12: return "Mañana"
        elif 12 <= h < 18: return "Tarde"
        else:              return "Noche"

    horas_unicas["franja_horaria"] = horas_unicas["hora"].apply(franja)
    horas_unicas.sort_values(["hora", "minuto"], inplace=True)
    horas_unicas.reset_index(drop=True, inplace=True)
    horas_unicas.index += 1
    horas_unicas.index.name = "hora_key"

    print(f"[OK]   DIM_HORA    → {len(horas_unicas):,} filas")
    return horas_unicas.reset_index()


# ---- DIM_PRODUCTO ----------------------------------------------------------

def construir_dim_producto(df: pd.DataFrame) -> pd.DataFrame:
    """
    Genera la dimensión de producto. La 'descripcion' se toma del
    último registro por stock_code (para manejar variaciones de texto
    en el dataset). En producción se usaría un catálogo maestro.
    """
    productos = (
        df[["stock_code", "descripcion"]]
        .dropna(subset=["stock_code"])
        .drop_duplicates(subset=["stock_code"], keep="last")
        .copy()
    )
    productos["descripcion"].fillna("SIN DESCRIPCIÓN", inplace=True)
    productos.sort_values("stock_code", inplace=True)
    productos.reset_index(drop=True, inplace=True)
    productos.index += 1
    productos.index.name = "producto_key"

    print(f"[OK]   DIM_PRODUCTO → {len(productos):,} filas")
    return productos.reset_index()


# ---- DIM_CLIENTE -----------------------------------------------------------

def construir_dim_cliente(df: pd.DataFrame) -> pd.DataFrame:
    """
    Genera la dimensión de cliente. Se incluyen atributos derivados
    'tipo_cliente' (Registrado / Desconocido) y 'region' (geográfica).
    """
    clientes = (
        df[["customer_id", "tipo_cliente", "country", "region"]]
        .drop_duplicates(subset=["customer_id"], keep="last")
        .copy()
    )
    clientes.sort_values("customer_id", inplace=True)
    clientes.reset_index(drop=True, inplace=True)
    clientes.index += 1
    clientes.index.name = "cliente_key"

    print(f"[OK]   DIM_CLIENTE  → {len(clientes):,} filas")
    return clientes.reset_index()


# =============================================================================
# ETAPA 4 (cont.) — CONSTRUCCIÓN DE LA TABLA DE HECHOS
# =============================================================================
# Se realizan los JOIN (merge) entre el DataFrame transformado y cada
# dimensión para sustituir las claves naturales por claves sustitutas.
# Este paso materializa el "esquema estrella" en el modelo de datos.
# =============================================================================

def construir_fact_ventas(
    df          : pd.DataFrame,
    dim_fecha   : pd.DataFrame,
    dim_hora    : pd.DataFrame,
    dim_producto: pd.DataFrame,
    dim_cliente : pd.DataFrame,
) -> pd.DataFrame:
    separador("ETAPA 4 · TABLA DE HECHOS")

    fact = df.copy()

    # -- Unir clave de fecha -------------------------------------------------
    dim_fecha_lookup = dim_fecha[["fecha_key", "fecha"]].copy()
    dim_fecha_lookup["fecha"] = dim_fecha_lookup["fecha"].astype(str)
    fact["fecha"] = fact["fecha"].astype(str)
    fact = fact.merge(dim_fecha_lookup, on="fecha", how="left")

    # -- Unir clave de hora --------------------------------------------------
    fact = fact.merge(
        dim_hora[["hora_key", "hora", "minuto"]],
        on=["hora", "minuto"],
        how="left"
    )

    # -- Unir clave de producto ----------------------------------------------
    fact = fact.merge(
        dim_producto[["producto_key", "stock_code"]],
        on="stock_code",
        how="left"
    )

    # -- Unir clave de cliente -----------------------------------------------
    fact = fact.merge(
        dim_cliente[["cliente_key", "customer_id"]],
        on="customer_id",
        how="left"
    )

    # -- Seleccionar y renombrar columnas finales ----------------------------
    fact = fact[[
        "invoice_no",
        "fecha_key",
        "hora_key",
        "producto_key",
        "cliente_key",
        "cantidad",
        "precio_unitario",
        "valor_bruto",
        "indicador_cancelacion",
    ]].copy()

    # -- Clave sustituta de la tabla de hechos ------------------------------
    fact.reset_index(drop=True, inplace=True)
    fact.index += 1
    fact.index.name = "venta_key"
    fact = fact.reset_index()

    # -- Convertir claves a entero (pueden quedar como float tras el merge) --
    for col in ["fecha_key", "hora_key", "producto_key", "cliente_key"]:
        fact[col] = pd.to_numeric(fact[col], errors="coerce").astype("Int64")

    print(f"[OK]   FACT_VENTAS  → {len(fact):,} filas")
    nulos_fk = {
        col: int(fact[col].isna().sum())
        for col in ["fecha_key", "hora_key", "producto_key", "cliente_key"]
    }
    print(f"[INFO] Nulos en FK  : {nulos_fk}")
    return fact


# =============================================================================
# ETAPA 5 — CARGA (LOAD)
# =============================================================================
# Se persisten todas las tablas en SQLite. Se usa if_exists='replace' para
# permitir re-ejecuciones idempotentes del script durante el desarrollo.
# En un entorno productivo se usaría lógica de carga incremental (SCD o UPSERT).
# =============================================================================

def cargar_en_sqlite(
    dim_fecha   : pd.DataFrame,
    dim_hora    : pd.DataFrame,
    dim_producto: pd.DataFrame,
    dim_cliente : pd.DataFrame,
    fact_ventas : pd.DataFrame,
) -> None:
    separador("ETAPA 5 · CARGA EN SQLITE")

    conn = sqlite3.connect(BASE_DATOS)

    tablas = {
        "DIM_FECHA"   : dim_fecha,
        "DIM_HORA"    : dim_hora,
        "DIM_PRODUCTO": dim_producto,
        "DIM_CLIENTE" : dim_cliente,
        "FACT_VENTAS" : fact_ventas,
    }

    for nombre, df_tabla in tablas.items():
        df_tabla.to_sql(nombre, conn, if_exists="replace", index=False)
        print(f"[OK]   Tabla cargada → {nombre} ({len(df_tabla):,} filas)")

    conn.close()
    print(f"\n[✓]   Base de datos guardada en: {BASE_DATOS}")


# =============================================================================
# ETAPA 6 — VALIDACIÓN SQL
# =============================================================================
# Consultas de calidad de datos para verificar la integridad del modelo
# antes de habilitarlo para consumo analítico (BI / reportería).
# =============================================================================

CONSULTAS_VALIDACION = {

    "1. Conteo de registros por tabla": """
        SELECT 'DIM_FECHA'    AS tabla, COUNT(*) AS total FROM DIM_FECHA    UNION ALL
        SELECT 'DIM_HORA'     AS tabla, COUNT(*) AS total FROM DIM_HORA     UNION ALL
        SELECT 'DIM_PRODUCTO' AS tabla, COUNT(*) AS total FROM DIM_PRODUCTO UNION ALL
        SELECT 'DIM_CLIENTE'  AS tabla, COUNT(*) AS total FROM DIM_CLIENTE  UNION ALL
        SELECT 'FACT_VENTAS'  AS tabla, COUNT(*) AS total FROM FACT_VENTAS;
    """,

    "2. Nulos en claves foráneas (FACT_VENTAS)": """
        SELECT
            SUM(CASE WHEN fecha_key    IS NULL THEN 1 ELSE 0 END) AS nulos_fecha_key,
            SUM(CASE WHEN hora_key     IS NULL THEN 1 ELSE 0 END) AS nulos_hora_key,
            SUM(CASE WHEN producto_key IS NULL THEN 1 ELSE 0 END) AS nulos_producto_key,
            SUM(CASE WHEN cliente_key  IS NULL THEN 1 ELSE 0 END) AS nulos_cliente_key
        FROM FACT_VENTAS;
    """,

    "3. Consistencia de valor_bruto (muestra 10 filas)": """
        SELECT
            invoice_no,
            cantidad,
            precio_unitario,
            valor_bruto,
            ROUND(cantidad * precio_unitario, 2) AS valor_calculado,
            ROUND(valor_bruto - (cantidad * precio_unitario), 4) AS diferencia
        FROM FACT_VENTAS
        LIMIT 10;
    """,

    "4. Conteo y monto total de cancelaciones": """
        SELECT
            indicador_cancelacion,
            COUNT(*)                           AS transacciones,
            ROUND(SUM(valor_bruto), 2)         AS valor_total,
            ROUND(AVG(ABS(valor_bruto)), 2)    AS ticket_promedio
        FROM FACT_VENTAS
        GROUP BY indicador_cancelacion
        ORDER BY indicador_cancelacion;
    """,

    "5. Ventas por región (top 10)": """
        SELECT
            c.region,
            COUNT(*)                      AS transacciones,
            ROUND(SUM(f.valor_bruto), 2)  AS ingresos_brutos
        FROM FACT_VENTAS  f
        JOIN DIM_CLIENTE  c ON f.cliente_key = c.cliente_key
        GROUP BY c.region
        ORDER BY ingresos_brutos DESC
        LIMIT 10;
    """,

    "6. Distribución por franja horaria": """
        SELECT
            h.franja_horaria,
            COUNT(*)                     AS transacciones,
            ROUND(SUM(f.valor_bruto), 2) AS valor_total
        FROM FACT_VENTAS f
        JOIN DIM_HORA    h ON f.hora_key = h.hora_key
        GROUP BY h.franja_horaria
        ORDER BY transacciones DESC;
    """,

    "7. Top 10 productos por ingresos": """
        SELECT
            p.stock_code,
            p.descripcion,
            COUNT(*)                       AS veces_vendido,
            ROUND(SUM(f.valor_bruto), 2)   AS ingresos_brutos
        FROM FACT_VENTAS  f
        JOIN DIM_PRODUCTO p ON f.producto_key = p.producto_key
        WHERE f.indicador_cancelacion = 0
        GROUP BY p.stock_code, p.descripcion
        ORDER BY ingresos_brutos DESC
        LIMIT 10;
    """,
}


def ejecutar_validaciones() -> None:
    separador("ETAPA 6 · VALIDACIONES SQL")

    conn = sqlite3.connect(BASE_DATOS)

    for titulo, sql in CONSULTAS_VALIDACION.items():
        print(f"\n--- {titulo} ---")
        try:
            resultado = pd.read_sql_query(sql, conn)
            print(resultado.to_string(index=False))
        except Exception as e:
            print(f"[ERROR] {e}")

    conn.close()


# =============================================================================
# PUNTO DE ENTRADA PRINCIPAL
# =============================================================================

def main() -> None:
    print("\n" + "="*70)
    print("   ETL — DATA WAREHOUSE DE VENTAS   (Online Retail Dataset)")
    print("="*70)

    # ---- Extracción --------------------------------------------------------
    df_raw = extraer_datos()

    # ---- Staging -----------------------------------------------------------
    df_stg = staging(df_raw)

    # ---- Transformación ----------------------------------------------------
    df_trans = transformar(df_stg)

    # ---- Construcción de dimensiones ---------------------------------------
    separador("ETAPA 4 · CONSTRUCCIÓN DE DIMENSIONES")
    dim_fecha    = construir_dim_fecha(df_trans)
    dim_hora     = construir_dim_hora(df_trans)
    dim_producto = construir_dim_producto(df_trans)
    dim_cliente  = construir_dim_cliente(df_trans)

    # ---- Tabla de hechos ---------------------------------------------------
    fact_ventas  = construir_fact_ventas(
        df_trans, dim_fecha, dim_hora, dim_producto, dim_cliente
    )

    # ---- Carga en SQLite ---------------------------------------------------
    cargar_en_sqlite(dim_fecha, dim_hora, dim_producto, dim_cliente, fact_ventas)

    # ---- Validaciones ------------------------------------------------------
    ejecutar_validaciones()

    separador("PROCESO FINALIZADO")
    print(f"  Base de datos disponible en → {os.path.abspath(BASE_DATOS)}")
    print("  Conéctate con cualquier cliente SQLite para explorar el modelo.\n")


if __name__ == "__main__":
    main()
