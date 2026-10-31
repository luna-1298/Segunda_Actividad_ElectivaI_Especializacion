from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("03_Capa_Gold").getOrCreate()

# # Creación de los Insight de negocio
# ## Enriquecer tabla de vuelos

spark.sql("""
CREATE OR REPLACE TABLE aerolinea.gold.vuelos_enriquecidos
USING DELTA AS
SELECT v.vuelo_id, v.fecha,
       CAST(date_trunc('MONTH', v.fecha) AS DATE) AS mes,
       v.aeronave_id, a.modelo, a.fabricante, a.anio_fabricacion,
       YEAR(v.fecha) - a.anio_fabricacion AS edad_aeronave,
       v.origen_id, o.nombre AS origen_nombre, o.ciudad AS origen_ciudad, o.pais AS origen_pais,
       v.destino_id, d.nombre AS destino_nombre, d.ciudad AS destino_ciudad, d.pais AS destino_pais,
       v.estado, v.duracion_min, v.estado <> 'cancelado' AS operado,
       v.ruta_por_revisar
FROM aerolinea.silver.vuelos v
LEFT JOIN aerolinea.silver.aeronaves a ON v.aeronave_id = a.aeronave_id
LEFT JOIN aerolinea.silver.aeropuertos o ON v.origen_id = o.aeropuerto_id
LEFT JOIN aerolinea.silver.aeropuertos d ON v.destino_id = d.aeropuerto_id
""")

# Consulta de verificación (resultado completo)
display(spark.sql("""
SELECT * FROM aerolinea.gold.vuelos_enriquecidos ORDER BY vuelo_id
"""))

# ##Enriquecer la tabla de mantenimiento
spark.sql("""
CREATE OR REPLACE TABLE aerolinea.gold.mantenimientos_enriquecidos
USING DELTA AS
SELECT m.mantenimiento_id, m.fecha,
       CAST(date_trunc('MONTH', m.fecha) AS DATE) AS mes,
       m.aeronave_id, a.modelo, a.fabricante, a.anio_fabricacion,
       YEAR(m.fecha) - a.anio_fabricacion AS edad_aeronave,
       m.tipo, m.costo_usd, m.duracion_hr
FROM aerolinea.silver.mantenimientos m
LEFT JOIN aerolinea.silver.aeronaves a ON m.aeronave_id = a.aeronave_id
""")

# Consulta de verificación (resultado completo)
display(spark.sql("""
SELECT * FROM aerolinea.gold.mantenimientos_enriquecidos
"""))

# Comprobar el enriquecimiento
vuelos = spark.table("aerolinea.gold.vuelos_enriquecidos").count()
mantenimientos = spark.table("aerolinea.gold.mantenimientos_enriquecidos").count()
assert vuelos == spark.table("aerolinea.silver.vuelos").count(), "La unión multiplicó o perdió vuelos."
assert mantenimientos == spark.table("aerolinea.silver.mantenimientos").count(), "La unión multiplicó o perdió intervenciones."
print("Vuelos enriquecidos:", vuelos)
print("Mantenimientos enriquecidos:", mantenimientos)

# ## Puntualidad por grupo
spark.sql("""
CREATE OR REPLACE TABLE aerolinea.gold.kpi_puntualidad_vuelos
USING DELTA AS
SELECT modelo, fabricante, origen_pais, mes,
       COUNT(*) AS total_vuelos,
       SUM(CASE WHEN operado THEN 1 ELSE 0 END) AS vuelos_operados,
       SUM(CASE WHEN estado = 'a_tiempo' THEN 1 ELSE 0 END) AS vuelos_a_tiempo,
       SUM(CASE WHEN estado = 'retrasado' THEN 1 ELSE 0 END) AS vuelos_retrasados,
       SUM(CASE WHEN estado = 'cancelado' THEN 1 ELSE 0 END) AS vuelos_cancelados,
       ROUND(100.0 * SUM(CASE WHEN estado = 'a_tiempo' THEN 1 ELSE 0 END)
         / NULLIF(SUM(CASE WHEN operado THEN 1 ELSE 0 END), 0), 2) AS puntualidad_operados_pct,
       ROUND(100.0 * SUM(CASE WHEN estado = 'cancelado' THEN 1 ELSE 0 END)
         / COUNT(*), 2) AS cancelacion_pct,
       ROUND(AVG(CASE WHEN operado THEN duracion_min END), 2) AS duracion_promedio_operados_min,
       SUM(CASE WHEN ruta_por_revisar THEN 1 ELSE 0 END) AS rutas_por_revisar
FROM aerolinea.gold.vuelos_enriquecidos
GROUP BY modelo, fabricante, origen_pais, mes
""")

# Consulta de verificación (resultado completo)
display(spark.sql("""
SELECT * FROM aerolinea.gold.kpi_puntualidad_vuelos ORDER BY modelo, origen_pais
"""))

# ## Puntualidad global
spark.sql("""
SELECT SUM(total_vuelos) AS vuelos,
       SUM(vuelos_operados) AS operados,
       SUM(vuelos_a_tiempo) AS a_tiempo,
       SUM(vuelos_cancelados) AS cancelados,
       ROUND(100.0 * SUM(vuelos_a_tiempo) / NULLIF(SUM(vuelos_operados), 0), 2) AS puntualidad_pct,
       ROUND(100.0 * SUM(vuelos_cancelados) / SUM(total_vuelos), 2) AS cancelacion_pct
FROM aerolinea.gold.kpi_puntualidad_vuelos
""")

# ## Costos de mantenimiento
spark.sql("""
CREATE OR REPLACE TABLE aerolinea.gold.kpi_costo_mantenimiento
USING DELTA AS
SELECT aeronave_id, modelo, fabricante, tipo, mes,
       COUNT(*) AS total_mantenimientos,
       SUM(costo_usd) AS costo_total_usd,
       ROUND(AVG(costo_usd), 2) AS costo_promedio_usd,
       SUM(duracion_hr) AS duracion_total_hr,
       ROUND(AVG(duracion_hr), 2) AS duracion_promedio_hr
FROM aerolinea.gold.mantenimientos_enriquecidos
GROUP BY aeronave_id, modelo, fabricante, tipo, mes
""")

spark.sql("""
SELECT tipo,
       SUM(total_mantenimientos) AS intervenciones,
       SUM(costo_total_usd) AS costo_total_usd
FROM aerolinea.gold.kpi_costo_mantenimiento
GROUP BY tipo
ORDER BY costo_total_usd DESC
""")

# ## Insight de actividad y costo en el mismo nivel de detalle
spark.sql("""
CREATE OR REPLACE TABLE aerolinea.gold.resumen_aeronave_mes
USING DELTA AS
WITH actividad AS (
  SELECT aeronave_id, mes, COUNT(*) AS total_vuelos,
         SUM(CASE WHEN operado THEN 1 ELSE 0 END) AS vuelos_operados
  FROM aerolinea.gold.vuelos_enriquecidos GROUP BY aeronave_id, mes
), mantenimiento AS (
  SELECT aeronave_id, mes, COUNT(*) AS total_mantenimientos,
         SUM(costo_usd) AS costo_total_usd
  FROM aerolinea.gold.mantenimientos_enriquecidos GROUP BY aeronave_id, mes
)
SELECT COALESCE(v.aeronave_id, m.aeronave_id) AS aeronave_id,
       COALESCE(v.mes, m.mes) AS mes,
       COALESCE(v.total_vuelos, 0) AS total_vuelos,
       COALESCE(v.vuelos_operados, 0) AS vuelos_operados,
       COALESCE(m.total_mantenimientos, 0) AS total_mantenimientos,
       COALESCE(m.costo_total_usd, 0) AS costo_total_usd
FROM actividad v FULL OUTER JOIN mantenimiento m
  ON v.aeronave_id = m.aeronave_id AND v.mes = m.mes
""")

# Consulta de verificación (resultado completo)
display(spark.sql("""
SELECT * FROM aerolinea.gold.resumen_aeronave_mes ORDER BY aeronave_id, mes
"""))

# ## Construcción base para ML
spark.sql("""
CREATE OR REPLACE TABLE aerolinea.gold.base_ml_mantenimiento
USING DELTA AS
SELECT mantenimiento_id, fecha, aeronave_id,
       tipo, modelo, fabricante, edad_aeronave, costo_usd
FROM aerolinea.gold.mantenimientos_enriquecidos
""")

# Consulta de verificación (resultado completo)
display(spark.sql("""
SELECT * FROM aerolinea.gold.base_ml_mantenimiento ORDER BY mantenimiento_id
"""))
