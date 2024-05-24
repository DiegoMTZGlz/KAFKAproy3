import tkinter as tk
from tkinter import ttk
from tkinter import messagebox
import requests
from confluent_kafka import Producer

# API Key por defecto para OpenWeatherMap
API_KEY_CLIMA = "9c37a698715d355c2f5f8b36ce974486"

# Ciudades por país para el clima
CIUDADES_CLIMA = {
    "México": ["Ciudad de México", "Guadalajara", "Monterrey"],
    "Estados Unidos": ["New York", "Los Angeles", "Chicago"],
    "España": ["Madrid", "Barcelona", "Valencia"],
    # Agrega más ciudades según sea necesario
}

# Lista de países para COVID-19
PAISES_COVID = ["México", "Estados Unidos", "España", "Argentina", "Brasil", "Colombia", "Chile"]

def obtener_clima(ciudad, api_key):
    try:
        url = f"http://api.openweathermap.org/data/2.5/weather?q={ciudad}&appid={api_key}&units=metric&lang=es"
        respuesta = requests.get(url)
        respuesta.raise_for_status()
        datos = respuesta.json()
        return datos
    except requests.exceptions.RequestException as e:
        print(f"Error al obtener los datos del clima: {e}")
        return None

def obtener_datos_covid(pais):
    try:
        url = f"https://disease.sh/v3/covid-19/countries/{pais}"
        respuesta = requests.get(url)
        respuesta.raise_for_status()
        datos = respuesta.json()
        return datos
    except requests.exceptions.RequestException as e:
        print(f"Error al obtener los datos de COVID-19: {e}")
        return None

def enviar_a_kafka(topic, datos):
    producer = Producer({
        'bootstrap.servers': 'localhost:9092'
    })
    
    def delivery_report(err, msg):
        if err is not None:
            print('Error al entregar mensaje: {}'.format(err))
        else:
            print('Mensaje entregado al topic {} [{}]'.format(msg.topic(), msg.partition()))

    producer.produce(topic, value=datos.encode('utf-8'), callback=delivery_report)
    producer.flush()

def obtener_y_enviar_clima():
    ciudad = combo_ciudad_clima.get()

    if ciudad.strip() == "":
        messagebox.showwarning("Advertencia", "Por favor, seleccione una ciudad para el clima.")
        return

    datos_clima = obtener_clima(ciudad, API_KEY_CLIMA)
    if datos_clima:
        enviar_a_kafka("clima", str(datos_clima))
        messagebox.showinfo("Éxito", "Datos del clima enviados correctamente.")
    else:
        messagebox.showwarning("Advertencia", "No se pudieron obtener los datos del clima.")

def obtener_y_enviar_covid():
    pais = combo_pais_covid.get()

    if pais.strip() == "":
        messagebox.showwarning("Advertencia", "Por favor, seleccione un país para COVID-19.")
        return

    datos_covid = obtener_datos_covid(pais)
    if datos_covid:
        enviar_a_kafka("covid", str(datos_covid))
        messagebox.showinfo("Éxito", "Datos de COVID-19 enviados correctamente.")
    else:
        messagebox.showwarning("Advertencia", "No se pudieron obtener los datos de COVID-19.")

# Configuración de la ventana
ventana = tk.Tk()
ventana.title("Producer")
ventana.geometry("300x300")

# Selector de país para Clima
label_pais_clima = ttk.Label(ventana, text="País para Clima:")
label_pais_clima.grid(row=0, column=0, padx=5, pady=5)
combo_pais_clima = ttk.Combobox(ventana, values=list(CIUDADES_CLIMA.keys()))
combo_pais_clima.grid(row=0, column=1, padx=5, pady=5)
combo_pais_clima.set(list(CIUDADES_CLIMA.values())[0])  # Establece el primer país por defecto

def actualizar_ciudades_clima(event):
    pais_seleccionado = combo_pais_clima.get()
    ciudades = CIUDADES_CLIMA.get(pais_seleccionado, [])
    combo_ciudad_clima['values'] = ciudades
    combo_ciudad_clima.set(ciudades[0] if ciudades else "")  # Establece la primera ciudad por defecto

# Selector de ciudad para Clima
label_ciudad_clima = ttk.Label(ventana, text="Ciudad para Clima:")
label_ciudad_clima.grid(row=1, column=0, padx=5, pady=5)
combo_ciudad_clima = ttk.Combobox(ventana, values=[])
combo_ciudad_clima.grid(row=1, column=1, padx=5, pady=5)

# Actualizar ciudades cuando se cambia el país seleccionado
combo_pais_clima.bind("<<ComboboxSelected>>", actualizar_ciudades_clima)

# Botón de enviar para Clima
boton_enviar_clima = ttk.Button(ventana, text="Enviar Clima", command=obtener_y_enviar_clima)
boton_enviar_clima.grid(row=2, column=0, columnspan=2, padx=5, pady=5)

# Selector de país para COVID-19
label_pais_covid = ttk.Label(ventana, text="País para COVID-19:")
label_pais_covid.grid(row=3, column=0, padx=5, pady=5)
combo_pais_covid = ttk.Combobox(ventana, values=PAISES_COVID)
combo_pais_covid.grid(row=3, column=1, padx=5, pady=5)
combo_pais_covid.set(PAISES_COVID[0])  # Establece el primer país por defecto

# Botón de enviar para COVID-19
boton_enviar_covid = ttk.Button(ventana, text="Enviar COVID-19", command=obtener_y_enviar_covid)
boton_enviar_covid.grid(row=4, column=0, columnspan=2, padx=5, pady=5)

# Ejecutar la aplicación
ventana.mainloop()

