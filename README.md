# Sistema Recomendador de Taxis para Big Data
> Jorge Barata González, Proyecto Final de Carrera, Ingeniería Informática Superior, UC3M

> Tutor: Pablo Basanta Val



## Análisis


10 millones de trazas de los taxis amarillos de nueva york

![image](pictures/clusters-hours-group.png)

- Se encuentran correlacciones entre beneficio economico y tiempo, distancia y numero de pasajeros.
- Se encuentran que la frecuencia de viajes cambia significativamente segun el dia y la hora. Cada dia tiene un patron distinto excepto martes, miercoles y jueves.
- Se encuentran agrupaciones de trazas en localidades concretas que varian segun el dia de la semana y la hora.
- Sobre una muestra del 1%, comprobamos que podemos encontrar las agrupaciones aplicando el sistema de clusterizacion Gaussian Mixture Model ejecutandolo sobre un cluster spark.
- Podemos valorar los clusters más interesantes dandoles una puntuación basada en las correlacciones encontradas anteriormente con la agregación de tiempo, distancia, y numero de pasajeros

## Diseño

- Para cada dia de la semana y hora, encontrar los clusters de trazas con puntuación más alta, calculado en Spark.
- Guardar los resultados en una base de datos espcial (PostGIS)
- Crear una aplicación web sobre la base de datos para consultar los mejores lugares a los que ir, más cercanos, en un momento dado.


![image](pictures/webapp-smartphone.jpeg)

## Ejecución

Se prueba a computar los clusters en una sola máquina variando el número de registros N y el número de clústers K.

Por otro lado, se prueba a computar los clusters y las puntuaciones de las 24 horas de 1 dia en diferentes configuraciones en los servidores cedidos en la universidad:

- 1 Master, 1 Slave, 1 Worker
- 1 Master, 1 Slave, 2 Workers (2 por esclavo)
- 1 Master, 2 Slaves, 2 Workers
- 1 Master, 2 Slaves, 4 Workers (2 por esclavo)
- 1 Master, 3 Slaves, 3 Workers
- 1 Master, 3 Slaves, 6 Workers (2 por esclavo)
- 1 Master, 4 Slaves, 4 Workers
- 1 Master, 4 Slaves, 8 Workers (2 por esclavo)
- 1 Master, 5 Slaves, 5 Workers
- 1 Master, 5 Slaves, 10 Workers (2 por esclavo)

El sistema esta pensado para computar los 7 dias, pero para estas pruebas se computa solo 1.

En mi portátil, he computado los clusters sobre el 1% de las trazas analizadas, que es lo que se puede consultar en pfc.jorgebg.com.


## Demo

https://pfc.jorgebg.com

### Heroku

[![Deploy](https://www.herokucdn.com/deploy/button.svg)](https://heroku.com/deploy?template=https://github.com/jorgebg/taxi-recommendation-system)

Open a shell into the dyno and run `./manage.py import_recommendations ~/result.pickle` after the deployment.

```
$ heroku pg:psql
create extension postgis
```