import tkinter as tk
from tkinter import ttk
import threading
from confluent_kafka import Consumer
import pymongo
from pymongo import MongoClient
import json
import matplotlib.pyplot as plt



client = MongoClient("localhost", 27017)
db = client.kafka
clima = db.clima
covid = db.covid

consumer = None
detener_consumo_flag = False  # Bandera para indicar si se debe detener el consumo

def consume_from_kafka(topic):
    global consumer, detener_consumo_flag
    consumer = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'my_consumer_group',
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe([topic])

    while True:
        if detener_consumo_flag:  # Comprueba si se debe detener el consumo
            break

        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print("Error al consumir mensaje: {}".format(msg.error()))
            continue

        print('Mensaje recibido del tópico "{}": {}'.format(topic, msg.value().decode('utf-8')))

        if topic == "clima":
            # JSON como cadena
            json_str = str(msg.value().decode('utf-8'))
            json_str = json_str.replace("'", '"')
            # Convertir JSON en diccionario Python
            data = json.loads(json_str)
            weather = data["weather"][0]["description"]
            feels_like = data["main"]["feels_like"]
            temp_min = data["main"]["temp_min"]
            temp_max = data["main"]["temp_max"]
            name = data["name"]
            country = data["sys"]["country"]
            document = {
                'weather': weather,
                'feels_like': feels_like,
                'temp_min': temp_min,
                'temp_max': temp_max,
                'name': name,
                'country': country
            }
            clima.insert_one(document)
        elif topic == "covid":
            # JSON como cadena
            json_str = str(msg.value().decode('utf-8'))
            json_str = json_str.replace("'", '"')
            # Convertir JSON en diccionario Python
            data = json.loads(json_str)
            cases = data["cases"]
            deaths = data["deaths"]
            recovered = data["recovered"]
            actives = data["active"]
            country = data["country"]
            document = {
                'cases': cases,
                'deaths': deaths,
                'recovered': recovered,
                'actives': actives,
                'country': country
            }
            covid.insert_one(document)

    consumer.close()

def detener_consumo():
    global detener_consumo_flag
    detener_consumo_flag = True  # Establece la bandera para detener el consumo
    print("Consumo detenido")

def iniciar_consumo_clima():
    detener_consumo()
    global detener_consumo_flag
    detener_consumo_flag = False  # Restablece la bandera para permitir el consumo
    threading.Thread(target=consume_from_kafka, args=("clima",), daemon=True).start()

def iniciar_consumo_covid():
    detener_consumo()
    global detener_consumo_flag
    detener_consumo_flag = False  # Restablece la bandera para permitir el consumo
    threading.Thread(target=consume_from_kafka, args=("covid",), daemon=True).start()

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

def graph_covid():
    ultimos_documentos = covid.find().sort([("_id", pymongo.DESCENDING)]).limit(4)
    datos = {"cases": [], "deaths": [], "recovered": [], "actives": []}
    paises = []

    for documento in ultimos_documentos:
        paises.append(documento["country"])
        for clave, valor in datos.items():
            valor.append(documento[clave])
        print(documento)

    # Crear gráfico
    fig, ax = plt.subplots(figsize=(10, 6))

    # Obtener el número de documentos
    num_documentos = len(datos["cases"])

    # Definir la posición de las barras para cada conjunto de datos
    posiciones = list(range(num_documentos))

    # Ancho de las barras
    ancho_barra = 0.2

    # Graficar cada conjunto de datos
    for i, (label, valores) in enumerate(datos.items()):
        ax.bar([pos + i * ancho_barra for pos in posiciones], valores, width=ancho_barra, label=label)

    # Configurar etiquetas y título
    ax.set_xlabel('País')
    ax.set_ylabel('Cantidad')
    ax.set_title('Comparación de Datos de COVID-19')
    ax.set_xticks([pos + (num_documentos * ancho_barra) / 2 for pos in posiciones])
    ax.set_xticklabels(paises)

    # Mostrar leyenda y grilla
    ax.legend()
    ax.grid(True)

    # Formatear números en el eje y con separador de miles
    ax.yaxis.set_major_formatter(ticker.StrMethodFormatter('{x:,.0f}'))

    # Ajustar diseño
    plt.tight_layout()

    # Mostrar gráfico
    plt.show()

def graph_weather():
    ultimos_documentos = clima.find().sort([("_id", pymongo.DESCENDING)]).limit(4)
    datos = {"feels_like": [], "temp_min": [], "temp_max": []}
    ciudades = []

    for documento in ultimos_documentos:
        ciudades.append(documento["name"])
        for clave, valor in datos.items():
            valor.append(documento[clave])
        print(documento)

    # Crear gráfico
    fig, ax = plt.subplots(figsize=(10, 6))

    # Obtener el número de ciudades
    num_ciudades = len(datos["feels_like"])

    # Definir la posición de las barras para cada conjunto de datos
    posiciones = list(range(num_ciudades))

    # Ancho de las barras
    ancho_barra = 0.2

    # Graficar cada conjunto de datos
    for i, (label, valores) in enumerate(datos.items()):
        ax.bar([pos + i * ancho_barra for pos in posiciones], valores, width=ancho_barra, label=label)

    # Configurar etiquetas y título
    ax.set_xlabel('Ciudad')
    ax.set_ylabel('Temperatura (°C)')
    ax.set_title('Comparación de Datos Climáticos')
    ax.set_xticks([pos + (num_ciudades * ancho_barra) / 2 for pos in posiciones])
    ax.set_xticklabels(ciudades)

    # Mostrar leyenda y grilla
    ax.legend()
    ax.grid(True)

    # Ajustar diseño
    plt.tight_layout()

    # Mostrar gráfico
    plt.show()

# Configuración de la ventana
ventana = tk.Tk()
ventana.title("Consumer")
ventana.geometry("300x200")

# Botones de iniciar consumo
boton_iniciar_clima = ttk.Button(ventana, text="Iniciar Consumo de Clima", command=iniciar_consumo_clima)
boton_iniciar_clima.pack(pady=5)

boton_iniciar_covid = ttk.Button(ventana, text="Iniciar Consumo de COVID-19", command=iniciar_consumo_covid)
boton_iniciar_covid.pack(pady=5)

# Botón de detener consumo
boton_detener = ttk.Button(ventana, text="Detener Consumo", command=detener_consumo)
boton_detener.pack(pady=5)

# Botón de graficar clima
graficar_clima = ttk.Button(ventana, text="Graficar Clima", command=graph_weather)
graficar_clima.pack(pady=5)

# Botón de graficar clima
graficar_covid = ttk.Button(ventana, text="Graficar Covid", command=graph_covid)
graficar_covid.pack(pady=5)

# Ejecutar la aplicación
ventana.mainloop()
