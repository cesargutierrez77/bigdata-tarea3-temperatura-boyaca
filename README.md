# Temperatura del aire en Boyacá con Apache Spark

Este proyecto es parte de la Tarea 3 del curso de Big Data (UNAD, código 202016911).
Lo hice yo, Cesar David Gutiérrez Pérez

## ¿Qué quería descubrir?

Quería saber cómo cambia la temperatura a lo largo del día en las estaciones
meteorológicas de Boyacá, en qué se diferencian unas estaciones de otras, y si
algunas de ellas tienen datos que no parecen confiables.

## ¿De dónde salen los datos?

Usé el conjunto de datos **"Temperatura Ambiente del Aire"** del IDEAM, publicado
en el portal de datos abiertos de Colombia (datos.gov.co, identificador `sbwg-7ju4`).
Descargué solo las lecturas de Boyacá: son cerca de **4,76 millones de filas**,
registradas entre 2002 y 2026. Son datos crudos, así que traen errores y valores
raros, y limpiarlos fue parte del trabajo.

## ¿Qué hace el código?

El script `analisis_batch.py` usa PySpark y sigue estos pasos:

1. Lee todos los archivos CSV y convierte cada columna a su tipo correcto.
2. Revisa si hay datos vacíos y elimina los registros repetidos (misma estación
   y misma fecha y hora).
3. Aparta las lecturas fuera del rango de -10 a 40 °C. Elegí ese rango pensando
   en el clima de Boyacá y se puede cambiar en el código.
4. Calcula el promedio, el mínimo y el máximo por estación, por hora del día y por mes.
5. Cuenta cuántas lecturas valen exactamente 0,0 °C en cada estación, porque un cero
   que se repite tanto puede ser un valor de relleno y no una medición real.
6. Genera cuatro gráficas y varias tablas en CSV.

## ¿Qué encontré?

- Se cargaron 4.761.616 filas y se eliminaron 254.455 duplicados.
- Solo 282 lecturas quedaron fuera del rango de -10 a 40 °C.
- La temperatura promedio cambia mucho entre estaciones: Páramo Rabanal ronda
  los 7 °C, mientras que Pauna ronda los 20 °C.
- Algunas estaciones tienen muchas lecturas exactamente en 0,0 °C, por ejemplo
  Páramo Rabanal (8,89 % de sus lecturas) y Tunguavita (5,21 %). Los considero
  valores sospechosos que habría que validar con el IDEAM. No puedo asegurar
  que sean errores, porque en zonas muy altas el frío extremo también es posible.

Las gráficas y tablas están en la carpeta `resultados`.

## ¿Cómo lo ejecuto?

Necesitas Ubuntu con Java 11, Apache Spark 3.5.3 y Python 3.10, más estas librerías:

    pip install matplotlib pandas

Luego:

1. Crea la carpeta de trabajo y copia allí los dos archivos del repositorio:

       mkdir -p ~/proyecto

2. Descarga los datos (tarda un buen rato, porque el servidor es lento):

       cd ~/proyecto
       bash descarga.sh

3. Ejecuta el análisis completo:

       spark-submit --driver-memory 2g analisis_batch.py

   Si solo quieres hacer una prueba rápida con una parte de los datos:

       spark-submit analisis_batch.py parte_0.csv

4. Cuando termine, los resultados quedan en `~/proyecto/resultados`.

## Archivos del repositorio

- `analisis_batch.py`: el análisis con Spark.
- `descarga.sh`: descarga los datos de Boyacá desde datos.gov.co.
- `resultados/`: gráficas y tablas que produjo la ejecución.
