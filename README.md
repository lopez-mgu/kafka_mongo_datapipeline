
# Data Pipleline con Apache Kafka y Mongo

Objetivo
    Aplicar un proceso de Data Pipeline

Actividad
    Construir un Data Pipeline con las siguientes características:
    Utilizar las tres API (Airlabs, Nasa, Openweather )
    Crear un productor de mensajes de las API
    Mediante Apache Kafka administrar los mensajes (crear un topic por cada API)
    Crear en Python un consumidor que tome los datos del topic de Apache Kafka
    Almacenar los datos en MongoDB
    Tomar los datos de MongoDB y gráficar los resultados
    De los datos que ofrecen los APIs seleccione la información a procesar y gráficar

Para lograr el objetivo planteado se utilizp un archivo compose de docker, con el cual se inicializara de manera local Apache Kafka y MongoDB.

En la carpeta kafka-docker-compose encontraran el compose de docker.
En la carpeta kafka-practice-python encontraran los archivos de python con los cuales se creo el data pipeline. A continuacion se describe brevemente el cómo funciona cada uno de los archivos:

config.py: contiene el puerto hacia el cual nos conectaremos para establecer la conexion con el broker de kafka.

kafka_producer.py: contiene el codigo para crear el producer y de este modo los topicos dentro del cluster de kafka, al igual que contiene peticiones a tres APIs distintas para obtener la informacion que sera enviada a los tres diferentes topicos creados.

kafka_consumer.py: contiene el codigo para consumir los datos desde los topicos de kafka, asi como, funciones para guardar dicha informacion dentro de tres distintas colecciones creadas dentro de Mongo.

mongo_consumer.py: contiene el codigo para consumir la informacion que se guardo previamente en Mongo. Tambien se puede hacer una grafica con los datos obtenidos de la API del clima.

mongo_functions.py: contiene las funciones miscelaneas que son usadas en kafka_consumer y mongo_consumer.


Este repositorio es un pequeño ejemplo de como crear un data pipeline con apache kafka y mongo, por lo que hay que tener en cuenta que algunas funciones o porciones de codigo no son las mas optimas, dado que es para un proyecto escolar.