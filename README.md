# README FEGA_DETECT

Este repositorio de GitHub se utilizará para la transferencia de la detección de abandonos y los diferentes algoritmos desarrollados por el Grupo de Investigación **GEOQUBIDY**. 

El repositorio está dividido en dos partes fundamentales:

## 1. FEGAPP
Código fuente de la aplicación desarrollada, que consta de dos módulos:

### a. SIGPAC Crono
Módulo que consiste en el cálculo de las capas procedentes de la base de datos del **SIGPAC** para obtener un seguimiento del funcionamiento de los diferentes recintos declarados.

### b. Descarga de imágenes
Este módulo permite la descarga de diferentes índices para un *tile* o una zona de estudio con Sentinel-2.

## 2. Marcadores
Parte enfocada en los marcadores y la parametrización derivada del análisis de la estabilidad y los ciclos de vegetación de los recintos de estudio.
Se proporciona un test para poder calcular los marcadores de parcelas.

---

## Estructura del repositorio

### 📂 `FEGA_DETECT`


#### 📁 `docs`
Carpeta donde se contiene el manual de uso extenso.
- 📄 `manual_uso.docx`

#### 📁 `img`
Carpeta donde se encuentran las imagenes asociadas con el README.
- 📄 **`descarga.png`**
- 📄 **`FEGAPP.png`**

#### 📁 `src`
Contiene la información necesaria para el correcto funcionamiento del **SIGPAC Crono**:

  - #### 📁 `config`
    - **`CSV_CONFIG.csv`**: Archivo CSV con la información de comunidades autónomas, años de campaña, fechas de inicio y nombre de la base de datos instalada.
  
  - #### 📁 `data`

    - #### 📁 `TILES`
    Carpeta con los *tiles* de Sentinel-2 necesarios para obtener la geometría correcta para la descarga de imágenes, en formato **ShapeFile**.
    - #### 📁 `umbrals`
    Carpeta con los umbrales calculados de los marcadores tras la función parametrización, en formato **CSV**.

  - #### 📁 `img`
    Carpeta con los archivos de imágenes necesarias:
    - 📄 **`composición.png`**: Composición del FEGA y el grupo de investigación.

    - 📄 **`IconoFegaApp.ico`**: Imagen de GEOQUBIDY.

  - #### 📁 `lib`
    Contiene los códigos fuente de las funcionalidades de la aplicación:
    - 📄`__init__.py`: Marca el comienzo de un paquete.

    - #### 📁 `descarga`
      - 📄`__init__.py`: Marca el comienzo de un paquete.
      - 📄`DESCARGA_GUI.py`: Código de la interfaz gráfica para la descarga de imágenes.
      - 📄`descarga_planet.py`: Código para la descarga de imágenes.
      - 📄`Stack_stacks.py`: Algoritmo para el apilado de todas las imágenes descargadas.

    - #### 📁 `procesamiento`
      - #### 📁 `sigpac`
        - 📄`__init__.py`: Marca el comienzo de un paquete.
        - 📄`FEGA_REC_APP.py`: Algoritmos de descarga de la base de datos del SIGPAC y generación del SIGPAC Crono.

      - #### 📁 `upm`
        - 📄`__init__.py`: Marca el comienzo de un paquete.
        - 📄`marcadoresupm.py`: Algoritmos de descarga de la base de datos del SIGPAC y generación del SIGPAC Crono.

  - 📄`__init__.py`: Marca el comienzo de un paquete.

  - 📄 **`Fega.py`**: Código principal que integra todos los algoritmos dentro de una interfaz gráfica para ejecutar la aplicación.

📄 **`LICENSE`**: Licencias de uso de la aplicación.

📄 **`main.py`**: Codigo para ejecutar la aplicación.

📄 **`README.md`**: Texto plano del README.

📄 **`requirements.txt`**: Requisitos de instalación de paquetes de Python.

---

## 🖥️ Interfaz gráfica


![Interfaz SIGPAC Crono](img/FEGAPP.png)

- Este módulo permite la visualización y análisis de las capas derivadas de la base de datos del **SIGPAC**.
- Permite evaluar la evolución y cambios en los recintos declarados a lo largo del tiempo.
- Los datos se pueden visualizar en diferentes formatos y escalas, facilitando la interpretación y el análisis espacial.
- Se integra con la descarga de imágenes y otros módulos de análisis.

### Procesamiento de índices Sentinel-2
![Interfaz Índices](img/descarga.png)

- Se seleccionan las fechas de inicio y fin del período de descarga.
- Permite elegir el índice espectral deseado (NDVI, EVI, etc.).
- Se selecciona la resolución espacial de descarga (10, 20 o 60 m).
- Se define el directorio de salida para guardar las imágenes descargadas.
- Se puede ingresar un tile de Sentinel-2 o un shapefile con el área de estudio.
- Se elige el formato de salida (ENVI o NetCDF).

---

## 📌 Notas adicionales
- Se recomienda asegurarse de que los archivos de configuración están correctamente definidos antes de ejecutar la aplicación.
- Se está trabajando en mejorar la integración del sistema para evitar dependencias externas innecesarias.


## Variables de entorno

Para ejecutar la app correctamente se necesitan las siguientes variable de entorno, que se pueden generar previamente o durante el uso de la app.

`MYUSER` nombre de usuario de la base de datos.

`MYPASSWORD` contraseña del usuario de la base de datos.

`MYHOST` IP de la maquina donde se aloja el servicio de base de datos.

`MYPORT` puerto de la maquina host asociado al servicio de base de datos.


## Ejecutar en local

Clonar el repositorio

```bash
  git clone https://github.com/TomPug/FEGA_DETECT.git
```

Abrir la carpeta

```bash
  cd FEGA_DETECT
```

Instalar las dependencias
```bash
  pip install -r requirements.txt
```

Abrir la app

```bash
  python3 main.py
```


## Ejecutar prueba de marcadores

Para ejecutar la prueba de marcadores (todavia no integrado en la app), use la herramienta por terminal de prueba:

```bash
  python3 src/test/marcadores_trial.py
```


## Soporte
[Informacion sobre el equipo](https://blogs.upm.es/dynamicland/)

© 2025 **GEOQUBIDY**. Todos los derechos reservados.
