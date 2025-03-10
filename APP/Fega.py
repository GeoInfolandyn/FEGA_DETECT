# fega_gui.py
# -*- coding: utf-8 -*-

import customtkinter as ctk
import tkinter as tk
from tkinter import ttk
from tkinter import messagebox
from PIL import Image
from customtkinter import filedialog
from tkinter import simpledialog
from tkcalendar import *
from ttkwidgets.autocomplete import AutocompleteCombobox
import subprocess
import sys, os
from threading import Thread
import lib.FEGA_REC_APP as FEGA_REC_APP
import lib.DESCARGA_GUI as DESCARGA_GUI
from datetime import datetime

# Importamos el módulo de procesado (en otro archivo)
# Ajusta la ruta o nombre según tu proyecto

# DICCIONARIOS PARA LA INTERFAZ
provincias_españa = [
    "Álava", "Albacete", "Alicante", "Almería", "Asturias", "Ávila", 
    "Badajoz", "Baleares", "Barcelona", "Burgos", 
    "Cáceres", "Cádiz", "Cantabria", "Castellón", "Ceuta", "Ciudad Real", 
    "Córdoba", "Cuenca", 
    "Girona", "Granada", "Guadalajara", "Guipúzcoa", 
    "Huelva", "Huesca", 
    "Jaén", 
    "La Coruña", "La Rioja", "Las Palmas", "León", "Lleida", "Lugo", 
    "Madrid", "Málaga", "Melilla", "Murcia", 
    "Navarra", 
    "Orense", 
    "Palencia", "Pontevedra", 
    "Salamanca", "Santa Cruz de Tenerife", "Segovia", "Sevilla", "Soria", 
    "Tarragona", "Teruel", "Toledo", 
    "Valencia", "Valladolid", "Vizcaya", 
    "Zamora", "Zaragoza"
]

provincias_sencillas = {
    'Álava': 'Alava',
    'Albacete': 'Albacete',
    'Alicante': 'Alicante',
    'Almería': 'Almeria',
    'Asturias': 'Asturias',
    'Ávila': 'Avila',
    'Badajoz': 'Badajoz',
    'Baleares': 'Baleares',
    'Barcelona': 'Barcelona',
    'Burgos': 'Burgos',
    'Cáceres': 'Caceres',
    'Cádiz': 'Cadiz',
    'Cantabria': 'Cantabria',
    'Castellón': 'Castellon',
    'Ceuta': 'Ceuta',
    'Ciudad Real': 'CiudadReal',
    'Córdoba': 'Cordoba',
    'Cuenca': 'Cuenca',
    'Girona': 'Girona',
    'Granada': 'Granada',
    'Guadalajara': 'Guadalajara',
    'Guipúzcoa': 'Guipuzcoa',
    'Huelva': 'Huelva',
    'Huesca': 'Huesca',
    'Jaén': 'Jaen',
    'La Coruña': 'Coruña',
    'León': 'Leon',
    'Lleida': 'Lleida',
    'Lugo': 'Lugo',
    'La Rioja': 'LaRioja',
    'Madrid': 'Madrid',
    'Málaga': 'Malaga',
    'Melilla': 'Melilla',
    'Murcia': 'Murcia',
    'Navarra': 'Navarra',
    'Ourense': 'Ourense',
    'Palencia': 'Palencia',
    'Las Palmas': 'LasPalmas',
    'Pontevedra': 'Pontevedra',
    'Salamanca': 'Salamanca',
    'Santa Cruz de Tenerife': 'Tenerife',
    'Segovia': 'Segovia',
    'Sevilla': 'Sevilla',
    'Soria': 'Soria',
    'Tarragona': 'Tarragona',
    'Teruel': 'Teruel',
    'Toledo': 'Toledo',
    'Valencia': 'Valencia',
    'Valladolid': 'Valladolid',
    'Vizcaya': 'Vizcaya',
    'Zamora': 'Zamora',
    'Zaragoza': 'Zaragoza'
}

usos_suelo = {
    "AG": "CORRIENTES Y SUPERFICIES DE AGUA",
    "CA": "VIALES",
    "CI": "CITRICOS",
    "CO": "CONTORNO OLIVAR",
    "ED": "EDIFICACIONES",
    "FO": "FORESTAL",
    "FY": "FRUTALES",
    "IM": "IMPRODUCTIVOS",
    "IV": "INVERNADEROS Y CULTIVOS BAJO PLASTICO",
    "OF": "OLIVAR - FRUTAL",
    "OV": "OLIVAR",
    "PA": "PASTO CON ARBOLADO",
    "PR": "PASTO ARBUSTIVO",
    "PS": "PASTIZAL",
    "TA": "TIERRAS ARABLES",
    "TH": "HUERTA",
    "VF": "VIÑEDO - FRUTAL",
    "VI": "VIÑEDO",
    "VO": "VIÑEDO - OLIVAR",
    "ZC": "ZONA CONCENTRADA NO INCLUIDA EN LA ORTOF",
    "ZU": "ZONA URBANA",
    "ZV": "ZONA CENSURADA",
    "FS": "FRUTOS SECOS",
    "FL": "FRUTOS SECOS Y OLIVAR",
    "FV": "FRUTOS SECOS Y VIÑEDO",
    "IS": "ISLAS",
    "OC": "Asociación Olivar-Cítricos",
    "CV": "Asociación Cítricos-Viñedo",
    "CF": "Asociación Cítricos-Frutales",
    "CS": "Asociación Cítricos-Frutales de cáscara",
    "FF": "Asociación Frutales-Frutales de cáscara",
    "EP": "ELEMENTO DEL PAISAJE",
    "MT": "MATORRAL",
    "OP": "Otros cultivos Permanentes", 
    'TODOS': 'TODOS'
}

# Variables globales para la configuración
user_sql_url = None
ogr_path = None

# ----------------------------------------------------------------
# CLASE PRINCIPAL DE LA APP
# ----------------------------------------------------------------

ctk.set_default_color_theme("green")
ctk.set_appearance_mode('light')

class Fega(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("FEGAPP")
        self.geometry("750x550")
        # Ajusta tu ruta de icono si la tienes
        # self.iconbitmap("./img/IconoFegaApp.ico")  
        self.resizable(0,0)
        
        self.grid_columnconfigure((0,1), weight=1)
        self.grid_rowconfigure([i for i in range(10)], weight=1)
        
        self.menu = Menu(self)
        self.menu.grid(row=1, column=0, rowspan=9, sticky='nwes', padx=5, pady=5, columnspan=2)
        
        self.createWidgets()
        
    def createWidgets(self):
        # Ajusta tu imagen y su ruta si la tienes
        try:
            img_logo = ctk.CTkImage(light_image=Image.open("./img/composicion.png"), size=(300,100))
        except:
            img_logo = None
        
        self.logo = ctk.CTkLabel(self, image=img_logo, text="") if img_logo else ctk.CTkLabel(self, text="FEGAApp")
        self.logo.grid(row=0, column=0, padx=10, sticky='wns')
        
        self.btn_config = ctk.CTkButton(
            master=self,
            width=80,
            text="Image Download",
            font=("Helvetica", 20),
            fg_color="Grey",
            command=self.open_sentinel_app
        )
        self.btn_config.grid(row=0, column=1, sticky="w", padx=40)
    
    def setConfig(self):
        global user_sql_url
        conf = open('./config/config.txt', 'w')
        user_sql_url = simpledialog.askstring('SQL user URL', 'Write the database url for the features extraction')
        conf.write(user_sql_url)
    
    def open_sentinel_app(self):
        top = ctk.CTkToplevel(self)
        top.title("Sentinel-2 Index Processor")
        top.geometry("540x350")
        SentinelIndexProcessorApp(top).pack(expand=True, fill="both")
        top.wm_transient(self)

# ----------------------------------------------------------------
# CLASE PARA DESCARGA Y PROCESADO SENTINEL
# ----------------------------------------------------------------

class SentinelIndexProcessorApp(ctk.CTkFrame):
    """Ventana secundaria para procesar / descargar imágenes Sentinel."""
    def __init__(self, master):
        super().__init__(master)
        self.master = master
        
        self.start_date_var = tk.StringVar()
        self.end_date_var = tk.StringVar()
        self.index_type_var = tk.StringVar(value="NDVI")
        self.resolution_var = tk.IntVar(value=10)
        self.output_dir_var = tk.StringVar()
        self.tile_var = tk.StringVar()
        self.clip_path_var = tk.StringVar()
        self.driver_var = tk.StringVar(value="ENVI")
        
        self.index_options = [
            'NDVI', 'GNDVI', 'LAI', 'EVI', 'LAI_EVI', 'SR', 'NIR', 'RED',
            'SAVI', 'fAPAR', 'AR', 'AS1', 'SASI', 'ANIR', 'ARE1', 'ARE2',
            'ARE3', 'NBR','CONC', 'B1', 'B2', 'B3', 'B4', 'B5', 'B6',
            'B7', 'B8', 'B9', 'B10', 'B11', 'B12', 'SCL'
        ]
        self.create_widgets()

    def create_widgets(self):
        ctk.CTkLabel(self, text="Start Date:").grid(row=0, column=0, pady=(25,5), padx=(30,10), sticky="w")
        ctk.CTkEntry(self, textvariable=self.start_date_var, width=200).grid(row=0, column=1, pady=(25,5), padx=(0,5))
        ctk.CTkButton(self, text="📅", command=lambda: self.open_calendar('start')).grid(row=0, column=2, padx=(5,5), pady=(25,5))

        ctk.CTkLabel(self, text="End Date:").grid(row=1, column=0, pady=5, padx=(30,10), sticky="w")
        ctk.CTkEntry(self, textvariable=self.end_date_var, width=200).grid(row=1, column=1, pady=5, padx=(0,5))
        ctk.CTkButton(self, text="📅", command=lambda: self.open_calendar('end')).grid(row=1, column=2, padx=5)

        ctk.CTkLabel(self, text="Index Type:").grid(row=2, column=0, pady=5, padx=(30,10), sticky="w")
        ctk.CTkComboBox(self, values=self.index_options, variable=self.index_type_var).grid(row=2, column=1, pady=5, padx=(0,5), sticky="we")

        ctk.CTkLabel(self, text="Resolution:").grid(row=3, column=0, pady=5, padx=(30,10), sticky="w")
        ctk.CTkComboBox(self, values=["10", "20", "60"], variable=self.resolution_var).grid(row=3, column=1, pady=5, padx=(0,5), sticky="we")

        ctk.CTkLabel(self, text="Output Directory:").grid(row=4, column=0, pady=5, padx=(30,10), sticky="w")
        ctk.CTkEntry(self, textvariable=self.output_dir_var, width=200).grid(row=4, column=1, pady=5, padx=(0,5))
        ctk.CTkButton(self, text="📁", command=self.select_output_directory).grid(row=4, column=2, padx=5)

        ctk.CTkLabel(self, text="ROI:").grid(row=5, column=0, pady=5, padx=(30,10), sticky="w")
        self.roi = ctk.CTkEntry(self, width=100, placeholder_text='Write the Tile name or shapefile')
        self.roi.grid(row=5, column=1, pady=5, padx=(0,5), sticky="we")
        self.check = ctk.CTkCheckBox(self, text="Shapefile", command=self.select_clip_path)
        self.check.grid(row=5, column=2, padx=5)

        ctk.CTkLabel(self, text="Output Format:").grid(row=7, column=0, pady=5, sticky="w", padx=(30,10))
        ctk.CTkComboBox(self, values=["ENVI", "NetCDF",], variable=self.driver_var).grid(row=7, column=1, pady=5, padx=(0,5), sticky="we")

        ctk.CTkButton(self, text="Process Sentinel-2 Data", fg_color="Blue", command=self.process_data).grid(row=8, column=0, columnspan=3, pady=20)

    def process_data(self):
        """Ejemplo de comando para lanzar un script externo que haga la descarga/procesado."""
        if not all([self.start_date_var.get(), self.end_date_var.get(), self.output_dir_var.get(), self.roi.get()]):
            messagebox.showerror("Error", "Please fill all required fields")
            return
        try:
            cmd = [
                sys.executable,
                r"lib/descarga_planet.py",  # Ajusta la ruta al script que hagas servir
                self.output_dir_var.get(),
                self.start_date_var.get(),
                self.end_date_var.get(),
                self.index_type_var.get(),
                str(self.resolution_var.get()),
                self.driver_var.get()
            ]
            if not self.check.get():
                cmd.extend(["--tile", self.roi.get()])
            else:
                cmd.extend(["--clip_path", self.roi.get()])

            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            
            output_window = tk.Toplevel(self.master)
            output_window.title("Processing Output")
            output_window.geometry("600x400")
            
            text_area = tk.Text(output_window, wrap=tk.WORD)
            text_area.pack(expand=True, fill='both')
            
            def update_text():
                for line in process.stdout:
                    text_area.insert(tk.END, line)
                    text_area.see(tk.END)
                    text_area.update()
                for line in process.stderr:
                    text_area.insert(tk.END, line)
                    text_area.see(tk.END)
                    text_area.update()
            
            import threading
            threading.Thread(target=update_text, daemon=True).start()

        except Exception as e:
            messagebox.showerror("Error", str(e))
    
    def select_output_directory(self):
        directory = filedialog.askdirectory()
        if directory:
            self.output_dir_var.set(directory)
    
    def select_clip_path(self):
        if self.check.get():
            filepath = filedialog.askopenfilename(filetypes=[("Shapefile", "*.shp")])
            self.roi.configure(state="normal")
            self.roi.delete(0, tk.END)
            self.roi.insert(0, filepath)
            self.roi.configure(state="disabled")
        else:
            self.roi.configure(state="normal")
            self.roi.delete(0, tk.END)

    def open_calendar(self, date_type):
        """Ejemplo de calendario sencillo para elegir fecha."""
        top = ctk.CTkToplevel(self)
        top.title("Select Date")
        cal_frame = ctk.CTkFrame(top)
        cal_frame.pack(padx=10, pady=10)

        year = ctk.CTkComboBox(cal_frame, values=list(map(str, range(datetime.now().year, 2015, -1))))
        year.set(datetime.now().year)
        year.grid(row=0, column=0, padx=5)

        month = ctk.CTkComboBox(cal_frame, values=[f"{i:02d}" for i in range(1, 13)])
        month.set(datetime.now().month)
        month.grid(row=0, column=1, padx=5)

        day = ctk.CTkComboBox(cal_frame, values=[f"{i:02d}" for i in range(1, 32)])
        day.set(datetime.now().day)
        day.grid(row=0, column=2, padx=5)
        
        top.wm_transient(self)

        def set_date():
            selected_date = f"{year.get()}-{month.get()}-{day.get()}"
            if date_type == 'start':
                self.start_date_var.set(selected_date)
            else:
                self.end_date_var.set(selected_date)
            top.destroy()

        ctk.CTkButton(cal_frame, text="Select", command=set_date).grid(row=1, column=1, pady=10)


# ----------------------------------------------------------------
# MENÚ LATERAL
# ----------------------------------------------------------------

class Menu(ctk.CTkFrame):
    """Menú principal para configurar parámetros y lanzar el proceso FEGA_REC_APP."""
    def __init__(self, master):
        super().__init__(master)
        
        self.grid_columnconfigure([i for i in range(4)], weight=1)
        self.grid_rowconfigure([i for i in range(8)], weight=1)
        
        self.selected_usos = []
        self.clip = None
        self.directory = ""
        
        self.usos_open = False  # para controlar despliegue de frame con usos
        
        self.createWidgets()
    
    def setConfig(self):
        global user_sql_url, ogr_path
        conf = open('./config/config.txt', 'w')
        user_sql_url = simpledialog.askstring('SQL user URL', 'Write the database url for the features extraction')
        ogr_path = simpledialog.askstring('OGR Path', 'Write the OGR path')
        text = f"{user_sql_url}\n{ogr_path}"
        conf.write(text)
    
    def createWidgets(self):
        # Botón Set Configuration
        self.btn_config = ctk.CTkButton(
            master=self, 
            width=80, 
            text="Set Configuration", 
            font=("Helvetica", 15, "bold"), 
            command=self.setConfig
        )
        self.btn_config.grid(row=0, column=0, pady=6, padx=40, sticky="we")
        
        # Botón "Select Directory"
        self.btn_dir = ctk.CTkButton(
            master=self, 
            width=160, 
            text="Select Directory", 
            font=("Helvetica", 15, "bold"), 
            command=self.selectdir
        )
        self.btn_dir.grid(row=1, column=0, sticky="we", padx=40)
        
        self.entry_dir = ctk.CTkEntry(self, placeholder_text="No directory selected")
        self.entry_dir.grid(row=1, column=1, sticky="we", columnspan=3, padx=10)
        self.entry_dir.configure(state="disabled")
        
        # Provincias
        self.prov_label = ctk.CTkLabel(self, text="Select Province", font=('Helvetica', 15, "bold"))
        self.prov_label.grid(row=2, column=0, sticky="we", padx=40)
        
        self.provincias = AutocompleteCombobox(
            self, 
            completevalues=provincias_españa, 
            height=10, 
            font=('Helvetica', 15)
        )
        self.provincias.set("Select province")
        self.provincias.grid(row=2, column=1, sticky="we", padx=10, columnspan=3)
        self.provincias.set("")
        
        # ROI
        self.clipLabel = ctk.CTkButton(
            self,
            text="Select ROI (optional)",
            font=('Helvetica', 15, "bold"),
            command=self.selectclip
        )
        self.clipLabel.grid(row=3, column=0, sticky="we", padx=40)
        
        self.entry_clip = ctk.CTkEntry(self, placeholder_text="No file selected")
        self.entry_clip.grid(row=3, column=1, sticky="we", columnspan=3, padx=10)
        self.entry_clip.configure(state="disabled")
        
        # DESPLEGABLE PARA USOS
        self.usos_button = ctk.CTkButton(
            self,
            text="▼  Select Land Uses",
            font=("Helvetica", 15, "bold"),
            command=self.toggle_usos
        )
        self.usos_button.grid(row=4, column=0, sticky="we", padx=40, columnspan=4, pady=(5,5))
        
        self.usos_frame = ctk.CTkScrollableFrame(self, width=320, height=120, label_text="Select Land Uses:")
        self.usos_frame.grid(row=5, column=0, columnspan=4, sticky="nwe", padx=40, pady=5)
        self.usos_frame.grid_remove()
        
        self.usos_vars = {}
        for code, desc in usos_suelo.items():
            var = tk.BooleanVar(value=False)
            chk = ctk.CTkCheckBox(
                master=self.usos_frame,
                text=f"{code} - {desc}",
                variable=var
            )
            chk.pack(anchor="w", pady=2, padx=5)
            self.usos_vars[code] = var
        
        # Intervalo de años
        self.start_years = self.generate_years(2020, 2024)
        self.end_years = self.generate_years(2020, 2024)
        
        self.yearsLabel = ctk.CTkLabel(self, text="Year Interval", font=('Helvetica', 15, "bold"), width=20)
        self.yearsLabel.grid(row=6, column=0, sticky="we", padx=10)
        
        self.start_year_selector = ctk.CTkComboBox(self, values=self.start_years, font=('Helvetica', 15), width=180)
        self.start_year_selector.set("INITIAL YEAR")
        self.start_year_selector.grid(row=6, column=1, sticky="we", padx=10)
        
        self.end_year_selector = ctk.CTkComboBox(self, values=self.end_years, font=('Helvetica', 15), width=180)
        self.end_year_selector.set("END YEAR")
        self.end_year_selector.grid(row=6, column=2, sticky="we", padx=10)
        
        # Barra de progreso
        self.progressbar = ctk.CTkProgressBar(self, orientation="horizontal", determinate_speed=.5, mode="determinate", height=12)
        self.progressbar.grid(row=7, column=1, sticky="we", padx=10, columnspan=3)
        self.progressbar.set(0)
        
        self.porcentaje = ctk.CTkLabel(self, text="0%", font=('Helvetica', 14))
        self.porcentaje.grid(row=7, column=0, sticky="e", padx=10)
        
        # Botón Start Process
        self.start_process = ctk.CTkButton(
            self,
            width=80,
            text="Start Process",
            command=self.startProcess,
            fg_color="Blue",
            font=('Helvetica', 18, "bold")
        )
        self.start_process.grid(row=8, column=1, sticky="we", padx=10, pady=15, columnspan=2)
    
    def toggle_usos(self):
        """Muestra/oculta el frame con checkboxes de usos."""
        if self.usos_open:
            self.usos_frame.grid_remove()
            self.usos_button.configure(text="▼  Select Land Uses")
            self.usos_open = False
            # Guardamos la selección
            self.selected_usos.clear()
            for code, var in self.usos_vars.items():
                if var.get():
                    self.selected_usos.append(code)
            print("Usos seleccionados:", self.selected_usos)
        else:
            self.usos_frame.grid()
            self.usos_button.configure(text="▲  Select Land Uses")
            self.usos_open = True
    
    def selectdir(self):
        self.directory = filedialog.askdirectory(title="Select Directory")
        self.entry_dir.configure(state="normal")
        self.entry_dir.delete(0, tk.END)
        self.entry_dir.insert(0, str(self.directory))
        self.entry_dir.configure(state="disabled")
    
    def selectclip(self):
        self.clip = filedialog.askopenfilename(title="Select File", filetypes=(('ShapeFile', '.shp'),('All files','*.*')))
        self.entry_clip.configure(state="normal")
        self.entry_clip.delete(0, tk.END)
        self.entry_clip.insert(0, self.clip)
        self.entry_clip.configure(state="disabled")
    
    def generate_years(self, start, end):
        return [str(year) for year in range(start, end + 1)]
    
    def startProcess(self):
        global user_sql_url, ogr_path
        
        # Si el desplegable de usos está abierto, lo cerramos para que se guarde la selección
        if self.usos_open:
            self.toggle_usos()
        
        if user_sql_url:
            if (self.start_year_selector.get() != "INITIAL YEAR" and
                self.end_year_selector.get() != "END YEAR" and
                self.directory != "" and
                self.provincias.get() != ""):
                
                if self.start_year_selector.get() >= self.end_year_selector.get():
                    messagebox.showwarning('Year Selector', 'The end year must be bigger than the initial year')
                    return
                
                self.progressbar.set(0)
                self.porcentaje.configure(text='0%')
                
                # Iniciamos el proceso en otro hilo
                t = Thread(
                    target=FEGA_REC_APP.main,
                    args=(
                        int(self.start_year_selector.get()),
                        int(self.end_year_selector.get()),
                        str(self.directory),
                        str(provincias_sencillas[self.provincias.get()]),
                        str(user_sql_url),
                        self.clip,
                        # str(ogr_path),
                        self.selected_usos
                    )
                )
                t.start()
                self.check_thread(t)
            else:
                messagebox.showwarning('Atribute not set', 'Select all the parameters to proceed')
        else:
            messagebox.showerror('Configuration alert', 'You must set the configuration path before starting the process')
    
    def check_thread(self, thread):
        """Comprueba estado del hilo y actualiza la barra de progreso usando FEGA_REC_APP."""
        if thread.is_alive():
            self.progressbar.set(FEGA_REC_APP.percentaje / 100)
            self.porcentaje.configure(text=f'{FEGA_REC_APP.message} - {round(FEGA_REC_APP.percentaje,1)}%')
            self.after(500, lambda: self.check_thread(thread))
        else:
            self.progressbar.set(FEGA_REC_APP.percentaje / 100)
            self.porcentaje.configure(text=f'{FEGA_REC_APP.message} - {FEGA_REC_APP.percentaje}%')
            if FEGA_REC_APP.percentaje != 100:
                messagebox.showerror('ERROR', 'The process has failed')
            else:
                messagebox.showinfo('Process completed', f'Data stored at: {FEGA_REC_APP.outpath}')
    
    def borrar(self):
        self.provincias.set("")


# ----------------------------------------------------------------
# EJECUCIÓN PRINCIPAL (solo la GUI)
# ----------------------------------------------------------------
if __name__ == '__main__':
    # Si existe config.txt, lo leemos para cargar user_sql_url y ogr_path
    if os.path.exists('./config/config.txt'):
        with open('./config/config.txt') as fd:
            lines = fd.readlines()
            if len(lines) == 2:
                user_sql_url = lines[0].strip()
                ogr_path = lines[1].strip()

    app = Fega()
    app.mainloop()
