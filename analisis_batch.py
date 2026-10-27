import os, sys
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession, functions as F

patron = sys.argv[1] if len(sys.argv) > 1 else "parte_*.csv"
BASE = os.path.expanduser("~/proyecto")
OUT = os.path.join(BASE, "resultados")
os.makedirs(OUT, exist_ok=True)

spark = SparkSession.builder.appName("AnalisisBatchBoyaca") \
    .config("spark.sql.shuffle.partitions", "8").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 1. CARGA
raw = spark.read.option("header", True).csv(f"file://{BASE}/{patron}")
n_raw = raw.count()
print(f"\n[1] Filas cargadas: {n_raw:,}")
raw.printSchema()

# 2. TIPOS
df = (raw
      .withColumn("valor", F.col("valorobservado").cast("double"))
      .withColumn("fecha", F.to_timestamp("fechaobservacion", "yyyy-MM-dd'T'HH:mm:ss.SSS"))
      .withColumn("latitud", F.col("latitud").cast("double"))
      .withColumn("longitud", F.col("longitud").cast("double"))
      .drop("valorobservado", "fechaobservacion"))

nulos = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]).collect()[0].asDict()
print("[2] Nulos por columna:", nulos)

# 3. LIMPIEZA
df1 = df.dropna(subset=["codigoestacion", "fecha", "valor"])
n_sin_nulos = df1.count()
df2 = df1.dropDuplicates(["codigoestacion", "fecha"])
n_sin_dup = df2.count()

LIM_MIN, LIM_MAX = -10.0, 40.0
atipicos = df2.filter((F.col("valor") < LIM_MIN) | (F.col("valor") > LIM_MAX))
n_atip = atipicos.count()

limpio = (df2.filter(F.col("valor").between(LIM_MIN, LIM_MAX))
          .withColumn("anio", F.year("fecha"))
          .withColumn("mes", F.month("fecha"))
          .withColumn("hora", F.hour("fecha"))
          .cache())
n_limpio = limpio.count()

print(f"[3] Tras quitar nulos: {n_sin_nulos:,} | sin duplicados: {n_sin_dup:,} "
      f"| fuera de rango [{LIM_MIN}, {LIM_MAX}]: {n_atip:,} | finales: {n_limpio:,}")

# 4. EDA
por_estacion = (limpio.groupBy("codigoestacion", "nombreestacion", "municipio")
                .agg(F.count("*").alias("lecturas"),
                     F.round(F.avg("valor"), 2).alias("promedio"),
                     F.round(F.min("valor"), 2).alias("minimo"),
                     F.round(F.max("valor"), 2).alias("maximo"),
                     F.round(F.stddev("valor"), 2).alias("desv"))
                .orderBy(F.desc("lecturas")))
horaria = limpio.groupBy("hora").agg(F.round(F.avg("valor"), 2).alias("promedio")).orderBy("hora")
mensual = limpio.groupBy("mes").agg(F.round(F.avg("valor"), 2).alias("promedio")).orderBy("mes")
por_anio = limpio.groupBy("anio").count().orderBy("anio")
atip_est = (atipicos.groupBy("codigoestacion", "nombreestacion").count()
            .orderBy(F.desc("count")).limit(10))

print("\n[4] Top 10 estaciones por lecturas:")
por_estacion.show(10, truncate=False)
print("Lecturas por año:")
por_anio.show(50)

e = por_estacion.toPandas(); h = horaria.toPandas(); m = mensual.toPandas()
a = por_anio.toPandas(); at = atip_est.toPandas()
for nombre, tabla in [("por_estacion", e), ("horaria", h), ("mensual", m), ("por_anio", a), ("atipicos", at)]:
    tabla.to_csv(f"{OUT}/{nombre}.csv", index=False)


# 4b. CALIDAD: lecturas con valor exactamente 0.0 por estacion
ceros = (limpio.groupBy("codigoestacion", "nombreestacion")
         .agg(F.count("*").alias("lecturas"),
              F.sum((F.col("valor") == 0.0).cast("int")).alias("ceros"))
         .withColumn("pct_ceros", F.round(F.col("ceros") * 100.0 / F.col("lecturas"), 2))
         .orderBy(F.desc("ceros")).limit(10))
print("\n[4b] Estaciones con mas lecturas exactamente en 0.0 C:")
ceros.show(10, truncate=False)
ceros.toPandas().to_csv(f"{OUT}/ceros.csv", index=False)

# 5. GRAFICAS
plt.figure(figsize=(8, 4)); plt.plot(h["hora"], h["promedio"], marker="o")
plt.title("Temperatura promedio por hora del día (Boyacá)")
plt.xlabel("Hora"); plt.ylabel("°C"); plt.grid(alpha=.3); plt.tight_layout()
plt.savefig(f"{OUT}/1_ciclo_horario.png", dpi=130); plt.close()

top = e.head(10)
plt.figure(figsize=(9, 5)); plt.barh(top["nombreestacion"].str[:30], top["promedio"])
plt.title("Temperatura promedio de las 10 estaciones con más lecturas")
plt.xlabel("°C"); plt.gca().invert_yaxis(); plt.tight_layout()
plt.savefig(f"{OUT}/2_promedio_estaciones.png", dpi=130); plt.close()

plt.figure(figsize=(8, 4)); plt.plot(m["mes"], m["promedio"], marker="o", color="tab:green")
plt.title("Temperatura promedio por mes"); plt.xlabel("Mes"); plt.ylabel("°C")
plt.xticks(range(1, 13)); plt.grid(alpha=.3); plt.tight_layout()
plt.savefig(f"{OUT}/3_ciclo_mensual.png", dpi=130); plt.close()

if len(at) > 0:
    plt.figure(figsize=(9, 5)); plt.barh(at["nombreestacion"].str[:30], at["count"], color="tab:red")
    plt.title("Estaciones con más lecturas fuera de rango"); plt.xlabel("Lecturas atípicas")
    plt.gca().invert_yaxis(); plt.tight_layout()
    plt.savefig(f"{OUT}/4_atipicos.png", dpi=130); plt.close()

print(f"\nListo. Resultados y gráficas en {OUT}")
spark.stop()
