#!/bin/bash
cd ~/proyecto
for i in 0 1 2 3 4; do
  f=parte_$i.csv
  if [ -f "$f" ] && [ "$(wc -l < $f)" -ge 1000001 ]; then
    echo "$f completa, la salto"; continue
  fi
  echo "Descargando $f"
  curl -G 'https://www.datos.gov.co/resource/sbwg-7ju4.csv' \
    --data-urlencode '$select=codigoestacion,nombreestacion,municipio,departamento,latitud,longitud,fechaobservacion,valorobservado' \
    --data-urlencode "\$where=departamento in('BOYACÁ','Boyacá')" \
    --data-urlencode '$order=:id' \
    --data-urlencode '$limit=1000000' \
    --data-urlencode "\$offset=$((i*1000000))" \
    --retry 3 -o $f
done
echo "TERMINADO"
