# Análisis de datos con Apache Pig

Este proyecto está dedicado al análsis de datos usando la herramienta Apache Pig. Para ello, se proporciona un conjunto de datos y una serie de scripts con las consultas a realizar sobre dicho conjunto de datos. A continuación, se describe detalladamente como ejecutar las consultas siguiendo el manual de usuario.

## 1. Instalación y preparación del entorno

**IMPORTANTE:** El proyecto ha sido desarrollado usando el sistema operativo Ubuntu 22, por lo tanto, se recomienda usar dicho sistema operativo.

Primero, es necesario instalar la herramienta Cloudera para ejecutar los scripts. Para ello, clonaremos la imagen de Docker de Cloudera mediante este comando:

```bash
docker run --hostname=quickstart.cloudera --privileged=true -t -i -p 10001:7180 -p10002:8888 cloudera/quickstart:latest /usr/bin/docker-quickstart
```

Con este comando, se descargará la imagen de Cloudera en caso de que aún no esté descargada e iniciará todos los servicios de Cloudera. Una vez finalizado el proceso, nos encontraremos dentro de la consola de Cloudera dónde podremos ejecutar los comandos necesarios. En caso de querer parar los servicios debemos hacer uso del comando:

```bash
docker stop <id-contenedor>
```

Para volver a iniciar los servicios y acceder a la consola de comandos debemos ejecutar los siguientes comandos:

```bash
docker start <id-contenedor>
```

```bash
docker exec -it <nombre-contenedor> /bin/bash
```

**IMPORTANTE:** Para obtener la información necesaria del contenedor (id y nombre) se puede hacer uso del siguiente comando, que muestra la información de todos los contenedores ya estén en ejecución o no:

```bash
docker ps -a
```
## 2. Carga de datos:

Para trabajar con el conjunto de datos, es necesario cargar los datos en el sistema HDFS, que se encarga de gestionar y almacenar el conjunto de datos que usaremos en nuestro proyecto. Para ello se ejecutarán los siguientes comandos:

- Primero, situaremos el archivo `dataset.csv` en la carpeta principal de nuestro sistema de archivos y ejecutamos el siguiente comando para copiar el archivo al contenedor de Docker donde se está ejecutando Cloudera:

  ```bash
  docker cp dataset.csv <nombre-contenedor>:/
  ```
  Una vez ejecutado, tendremos el archivo en el directorio principal de nuestro contenedor docker.
- Ahora que disponemos del archivo dentro del contenedor, necesitamos cargarlo en el sistema HDFS. Para ello se usará el siguiente comando dentro del propio contenedor:
  ```bash
  hdfs dfs -mkdir /tmp/data
  ```
   ```bash
  hdfs dfs -put dataset.csv /tmp/data
  ```
  Con ello, se creará el directorio /data dentro de la carpeta /tmp del sistema HDFS y luego se cargarán los datos en dicha ruta.

  En caso de tener que eliminar algún directorio dentro de la carpeta data del sistema HDFS, se deberá ejecutar el siguiente comando:
   ```bash
  hdfs dfs -rm -r /tmp/data/[nombre_directorio]
  ```
## 3. Primer script: conteo de datos

Entrada: script conteo_datos.pig 

Ejecución:
  (fuera contenedor)
   ```bash
  docker cp conteo_datos.pig <nombre-contenedor>:/
  ```
  (dentro contenedor)
  ```bash
  pig -x mapreduce conteo_datos.pig 
  ```

Salida: número de filas del conjunto de datos

## 4.	Segundo script: filtrado

Entrada: script filtrado.pig  

Ejecución:
  (fuera contenedor)
   ```bash
  docker cp filtrado.pig <nombre-contenedor>:/
  ```
  (dentro contenedor)
  ```bash
  pig -x mapreduce filtrado.pig 
  ```

Salida: número de filas que se han pagado usando tarjeta de débito

## 5.	Tercer script: producto más frecuente

Entrada: script producto_mas_frecuente.pig  

Ejecución:
  (fuera contenedor)
   ```bash
  docker cp producto_mas_frecuente.pig <nombre-contenedor>:/ 
  ```
  (dentro contenedor)
  ```bash
  pig -x mapreduce producto_mas_frecuente.pig  
  ```

Salida: se devuelven los valores (id_cliente, producto_mas_frecuente, frecuencia)

## 6.	Cuarto script: Rentabilidad de las categorías

Entrada: script rentabilidad_categorias.pig  

Ejecución:
  (fuera contenedor)
   ```bash
  docker cp rentabilidad_categorias.pig <nombre-contenedor>:/  
  ```
  (dentro contenedor)
  ```bash
  pig -x mapreduce rentabilidad_categorias.pig   
  ```

Salida: se almacena en sistema HDFS

Dichos resultados se van a almacenar en una base de datos usando el sistema de almacenamiento de datos Hive, que permite almacenar y hacer consultas SQL a un determinado conjunto de datos. Para acceder a la interfaz de comandos de Hive, basta con introducir hive en la consola del contenedor. Una vez que podamos interactuar con Hive, se ejecutarán las siguientes sentencias SQL:

- Creación de tabla y carga de datos:
  
  ```bash
    CREATE EXTERNAL TABLE category_profitability(category STRING, benefits DOUBLE, sales DOUBLE, margin DOUBLE)
    ROW FORMAT DELIMITED  FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
    LOCATION '/tmp/data/category_profitability';  
  ```
-	Visualizar todo el contenido:
  
  ```bash
    SELECT * FROM category_profitability;  
  ```
-	Obtener la categoría más rentable:
  
  ```bash
    SELECT * FROM category_profitability LIMIT 1;  
  ```

## 7.	Quinto script: Análisis de eficiencia de envío por región

Entrada: script eficiencia_envios.pig  

Ejecución:
  (fuera contenedor)
   ```bash
  docker cp eficiencia_envios.pig <nombre-contenedor>:/   
  ```
  (dentro contenedor)
  ```bash
  pig -x mapreduce eficiencia_envios.pig    
  ```

Salida: se almacena en sistema HDFS

Dichos resultados se van a almacenar en una base de datos usando el sistema de almacenamiento de datos Hive, que permite almacenar y hacer consultas SQL a un determinado conjunto de datos. Para acceder a la interfaz de comandos de Hive, basta con introducir hive en la consola del contenedor. Una vez que podamos interactuar con Hive, se ejecutarán las siguientes sentencias SQL:

- Creación de tabla y carga de datos:
  
  ```bash
    CREATE EXTERNAL TABLE shipping_days_region(region STRING, avg_shipping_days DOUBLE)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
    LOCATION '/tmp/data/avg_shipping_days';  
  ```
-	Visualizar todo el contenido:
  
  ```bash
    SELECT * FROM shipping_days_region;  
  ```
-	Obtener la región más eficiente:
  
  ```bash
    SELECT region FROM shipping_days_region LIMIT 1;  
  ```















