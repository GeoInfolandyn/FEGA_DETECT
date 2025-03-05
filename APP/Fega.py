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

# Variables globales
ctk.set_default_color_theme("green")
ctk.set_appearance_mode('light')

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
    "Zamora", "Zaragoza"]

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


python_path = None
# arcpy_path = None
user_sql_url = None

# Creación de clases
class Fega(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("FEGAPP")
        self.geometry("750x550")
        self.iconbitmap("./img/Icono FegaApp.ico")
        self.resizable(0,0)
        # self._set_appearance_mode("system")
        self.grid_columnconfigure((0,1), weight=1)
        self.grid_rowconfigure([i for i in range(10)], weight=1)
        self.menu = Menu(self)
        self.menu.grid(row=1, column=0, rowspan=9, sticky='nwes',padx=5, pady=5, columnspan=2)
        self.createWidgets()
        
    def createWidgets(self):
        img_logo = ctk.CTkImage(light_image= Image.open("./img/composicion.png"), size=(300,100))
        self.logo = ctk.CTkLabel(self, image=img_logo, text="")
        self.logo.grid(row=0, column=0,padx=10, sticky='wns')
        self.btn_config = ctk.CTkButton(master=self, width=80, text="Image Download", font=("Helvetica", 20), fg_color="Grey", command=self.open_sentinel_app)
        self.btn_config.grid(row=0, column=1, sticky="w", padx=40)
        
    def setConfig(self):
        global user_sql_url
        conf = open('./config/config.txt', 'w')
        user_sql_url =simpledialog.askstring('SQL user URL', 'Write the database url for the features extraction')
        conf.write(user_sql_url)
        
    def open_sentinel_app(self):
        top = ctk.CTkToplevel(self)  # Crear una ventana Toplevel
        top.title("Sentinel-2 Index Processor")
        top.geometry("540x350")
        SentinelIndexProcessorApp(top).pack(expand=True, fill="both")
        top.wm_transient(self)     
    
class SentinelIndexProcessorApp(ctk.CTkFrame):
    def __init__(self, master):
        super().__init__(master)

        self.master = master
        # Usar el Toplevel como master de los widgets
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
            'ARE3', 'NBR','CONC', 'B1', 'B2', 'B3', 'B4', 'B5', 'B6', 'B7', 'B8',
            'B9', 'B10', 'B11', 'B12', 'SCL'
        ]
        self.create_widgets()

    def create_widgets(self):
        # Crear los widgets en el Toplevel, no en 'self' (CTkFrame)
        ctk.CTkLabel(self, text="Start Date:").grid(row=0, column=0, pady=(25,5), padx=(30,10), sticky="w")
        ctk.CTkEntry(self, textvariable=self.start_date_var, width=200).grid(row=0, column=1, pady=(25,5), padx=(0,5))
        ctk.CTkButton(self, text="📅", command=lambda: self.open_calendar('start')).grid(row=0, column=2, padx=(5,5),pady=(25,5))

        # End Date
        ctk.CTkLabel(self, text="End Date:").grid(row=1, column=0, pady=5, padx=(30,10), sticky="w")
        ctk.CTkEntry(self, textvariable=self.end_date_var, width=200).grid(row=1, column=1, pady=5, padx=(0,5))
        ctk.CTkButton(self, text="📅", command=lambda: self.open_calendar('end')).grid(row=1, column=2, padx=5)

        # Index Type
        ctk.CTkLabel(self, text="Index Type:").grid(row=2, column=0, pady=5, padx=(30,10), sticky="w")
        ctk.CTkComboBox(self, values=self.index_options, variable=self.index_type_var).grid(row=2, column=1, pady=5,padx=(0,5), sticky="we")

        # Resolution
        ctk.CTkLabel(self, text="Resolution:").grid(row=3, column=0, pady=5, padx=(30,10), sticky="w")
        ctk.CTkComboBox(self, values=["10", "20", "60"], variable=self.resolution_var).grid(row=3, column=1, pady=5,padx=(0,5), sticky="we")

        # Output Directory
        ctk.CTkLabel(self, text="Output Directory:").grid(row=4, column=0, pady=5, padx=(30,10), sticky="w")
        ctk.CTkEntry(self, textvariable=self.output_dir_var, width=200).grid(row=4, column=1, pady=5,padx=(0,5))
        ctk.CTkButton(self, text="📁", command=self.select_output_directory).grid(row=4, column=2, padx=5)

        # Tile
        ctk.CTkLabel(self, text="ROI:").grid(row=5, column=0, pady=5, padx=(30,10), sticky="w")
        self.roi = ctk.CTkEntry(self, width=100,placeholder_text='Write the Tile name or shapefile')
        self.roi.grid(row=5, column=1, pady=5,padx=(0,5), sticky="we")
        self.check = ctk.CTkCheckBox(self, text="Shapefile", command=self.select_clip_path)
        self.check.grid(row=5, column=2, padx=5)

        # Clip Path
        # ctk.CTkLabel(self, text="Clip Path (Optional):").grid(row=6, column=0, pady=5, padx=(30,10), sticky="w")
        # ctk.CTkEntry(self, textvariable=self.clip_path_var, width=200).grid(row=6, column=1, pady=5)
        # ctk.CTkButton(self, text="📁", command=self.select_clip_path).grid(row=6, column=2, padx=5)

        # Output Format
        ctk.CTkLabel(self, text="Output Format:").grid(row=7, column=0, pady=5, sticky="w",padx=(30,10))
        ctk.CTkComboBox(self, values=["ENVI", "NetCDF"], variable=self.driver_var).grid(row=7, column=1, pady=5,padx=(0,5), sticky="we")

        # Process Button
        ctk.CTkButton(self, text="Process Sentinel-2 Data", fg_color="Blue", command=self.process_data).grid(row=8, column=0, columnspan=3, pady=20)

    def process_data(self):
        # Validate inputs
        if not all([self.start_date_var.get(), self.end_date_var.get(), 
                    self.output_dir_var.get(), self.roi.get()]):
            messagebox.showerror("Error", "Please fill all required fields")
            return

        try:
            # Construct command
            cmd = [
                sys.executable,  # Current Python interpreter
                r"lib/descarga_planet.py",  # Your main script
                self.output_dir_var.get(),
                self.start_date_var.get(),
                self.end_date_var.get(),
                self.index_type_var.get(),
                str(self.resolution_var.get()),
                self.driver_var.get()
            ]

            # Optional tile
            if not self.check.get():
                cmd.extend(["--tile", self.roi.get()])
            # Optional clip path
            else:
                cmd.extend(["--clip_path", self.roi.get()])

            # Run the script
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            
            # Create output window
            output_window = tk.Toplevel(self.master)
            output_window.title("Processing Output")
            output_window.geometry("600x400")
            
            text_area = tk.Text(output_window, wrap=tk.WORD)
            text_area.pack(expand=True, fill='both')
            
            # Function to update text area
            def update_text():
                for line in process.stdout:
                    text_area.insert(tk.END, line)
                    text_area.see(tk.END)
                    text_area.update()
                
                for line in process.stderr:
                    text_area.insert(tk.END, line)
                    text_area.see(tk.END)
                    text_area.update()
            
            # Start updating in a separate thread
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

    # def process_data(self):
    #     print(f"Start Date: {self.start_date_var.get()}")
    #     print(f"End Date: {self.end_date_var.get()}")
        def set_date():
            selected_date = f"{year.get()}-{month.get()}-{day.get()}"
            if date_type == 'start':
                self.start_date_var.set(selected_date)
            else:
                self.end_date_var.set(selected_date)
            top.destroy()

        ctk.CTkButton(cal_frame, text="Select", command=set_date).grid(row=1, column=1, pady=10)



class Menu(ctk.CTkFrame):
    def __init__(self,master):
        super().__init__(master)
        self.grid_columnconfigure([i for i in range(4)], weight=1)
        self.grid_rowconfigure([i for i in range(7)], weight=1)
        self.createWidgets()
        self.clip = None
    
    def setConfig(self):
        global user_sql_url
        conf = open('./config/config.txt', 'w')
        user_sql_url =simpledialog.askstring('SQL user URL', 'Write the database url for the features extraction')
        ogr_path =simpledialog.askstring('OGR Path', 'Write the OGR path')
        text = f"{user_sql_url}\n{ogr_path}"
        conf.write(text)
    
    def createWidgets(self):
        #texto
        self.btn_config = ctk.CTkButton(master=self, width=80, text="Set Configuration", font=("Helvetica", 15, "bold"), command=self.setConfig)
        self.btn_config.grid(row=0, column=0,pady=6,padx=40, sticky="we")
        
        # botones
        
        self.btn_dir = ctk.CTkButton(master=self, width=160, text="Select Directory", font=("Helvetica", 15, "bold"), command=self.selectdir)
        self.btn_dir.grid(row=1, column=0, sticky="we", padx=40)
        self.entry_dir = ctk.CTkEntry(self, placeholder_text="No directory selected")
        self.entry_dir.grid(row=1, column=1,sticky="we",columnspan=2, padx=10)
        self.entry_dir.configure(state="disabled")
        
        
        self.provincias = AutocompleteCombobox(self, completevalues=provincias_españa, height=10,font=('Helvetica', 15))
        self.provincias.set("Select province")
        self.provincias.grid(row=2, column=1, sticky="we", padx=10, columnspan=2)
        self.provincias.set("")
        self.prov_label = ctk.CTkLabel(self, text="Select Province", font=('Helvetica', 15, "bold"))
        self.prov_label.grid(row=2, column=0, sticky="we", padx=40)

        self.clipLabel = ctk.CTkButton(self, text="Select ROI (optional)", font=('Helvetica', 15, "bold"), command=self.selectclip)
        self.clipLabel.grid(row=3, column=0, sticky="we", padx=40)
        self.entry_clip = ctk.CTkEntry(self, placeholder_text="No file selected")
        self.entry_clip.grid(row=3, column=1,sticky="we",columnspan=2, padx=10)
        self.entry_clip.configure(state="disabled")
        
        # selector fechas
        self.start_years = self.generate_years(2020, 2024)
        self.end_years = self.generate_years(2020, 2024)
        
        self.yearsLabel = ctk.CTkLabel(self, text="Year Interval", font=('Helvetica', 15, "bold"), width=20)
        self.yearsLabel.grid(row=4, column=0, sticky="we", padx=10)
        self.start_year_selector = ctk.CTkComboBox(self, values=self.start_years, font=('Helvetica', 15), width=180)
        self.start_year_selector.set("INITIAL YEAR") 
        self.start_year_selector.grid(row=4, column=1, sticky="we", padx=10)
        self.end_year_selector = ctk.CTkComboBox(self, values=self.end_years, font=('Helvetica', 15), width=180)
        self.end_year_selector.set("END YEAR")
        self.end_year_selector.grid(row=4, column=2, sticky="we", padx=10)

        # progress bar
        self.progressbar = ctk.CTkProgressBar(self, orientation="horizontal", determinate_speed=.5, mode= "determinate", height=12 )
        self.progressbar.grid(row=5, column=1, sticky="we", padx=10, columnspan=3)
        self.progressbar.set(0)
        self.porcentaje = ctk.CTkLabel(self, text="0%", font=('Helvetica', 14))
        self.porcentaje.grid(row=5, column=0, sticky="e", padx=10)
        self.espacio = ctk.CTkLabel(self, text="", font=('Helvetica', 15, "bold"), width=20)
        self.espacio.grid(row=5, column=4, sticky="we", padx=10)
        
        # iniciar proceso
        self.start_process = ctk.CTkButton(self, width=80, text="Start Process",command=self.startProcess,fg_color="Blue", font=('Helvetica', 18, "bold"))
        self.start_process.grid(row=6, column=1, sticky="we", padx=10,pady=15, columnspan=2)
        
          
        
    def selectfile(self):
        self.filename = filedialog.askopenfilename(title="Select File", filetypes=(('GeoPackage', '.gpkg'),('All files','*.*')))
        print(self.filename)
        self.entry_file.configure(state="normal")
        self.entry_file.insert(1,self.filename)
        self.entry_file.configure(state="disabled")
        
    def selectdir(self):
        self.directory = filedialog.askdirectory(title="Select Directory")
        print(self.directory)
        self.entry_dir.configure(state="normal")
        self.entry_dir.insert(1,str(self.directory))
        self.entry_dir.configure(state="disabled")

    def selectclip(self):
        self.clip = filedialog.askopenfilename(title="Select File", filetypes=(('ShapeFile', '.shp'),('All files','*.*')))
        print(self.clip)
        self.entry_clip.configure(state="normal")
        self.entry_clip.insert(1,self.clip)
        self.entry_clip.configure(state="disabled")
   
    def grab_date(self):
        fecha = self.calendar.get_date()
        if self.pulsado: #esto es como poner
            self.date_fin.configure(state="normal")
            self.date_fin.delete(0, 9999)
            self.date_fin.insert(1, fecha)
            self.date_fin.configure(state="disabled")
            
        else:
            self.date_ini.configure(state="normal")
            self.date_ini.delete(0, 9999)
            self.date_ini.insert(1, fecha)
            self.date_ini.configure(state="disabled")
        self.pulsado = not self.pulsado
        
    def generate_years(self, start, end):
        return [str(year) for year in range(start, end + 1)]  
       
    def startProcess(self):
        global user_sql_url
        print(user_sql_url)
        if user_sql_url:
            if self.start_year_selector.get() != "INITIAL YEAR" and self.end_year_selector.get() != "END YEAR"  and self.directory != "" and self.provincias.get() != "":
                print(self.start_year_selector.get(), self.end_year_selector.get(), self.directory, self.provincias.get())
                
                self.progressbar.set(0)
                # comando = f"start /wait C:/Users/GeoQuBiDy/AppData/Local/Programs/Python/Python310/python.exe FEGA_REC_APP.py {str(self.start_year_selector.get())} {str(self.end_year_selector.get())} {str(self.directory)} {str(self.provincias.get())}"
                if self.start_year_selector.get() >= self.end_year_selector.get():
                    messagebox.showwarning('Year Selector', 'The end year must be bigger than the initial year')
                    return
                # comando = f"{python_path} FEGA_REC_APP.py {str(self.start_year_selector.get())} {str(self.end_year_selector.get())} {str(self.directory)} {str(provincias_sencillas[self.provincias.get()])} {user_sql_url}"
                # process = subprocess.call(comando, shell=True)
                t = Thread(target=FEGA_REC_APP.main, args=(int(self.start_year_selector.get()), int(self.end_year_selector.get()), str(self.directory), str(provincias_sencillas[self.provincias.get()]), str(user_sql_url), self.clip, str(ogr_path)))
                t.start()
                
                while t.is_alive():
                    self.progressbar.set(FEGA_REC_APP.percentaje/100)
                    self.progressbar.update()
                    self.porcentaje.configure(text=f'{FEGA_REC_APP.message} - {round(FEGA_REC_APP.percentaje,1)}%')
                    self.porcentaje.update()
                    self.master.update()
                    
                t.join()
                self.progressbar.set(FEGA_REC_APP.percentaje/100)
                self.progressbar.update()
                self.porcentaje.configure(text=f'{FEGA_REC_APP.message} - {FEGA_REC_APP.percentaje}%')
                self.porcentaje.update()
            
                if not t.is_alive() and FEGA_REC_APP.percentaje != 100:
                    messagebox.showerror('ERROR', 'The process has failed')
                else:
                    messagebox.showinfo('Process completed', f'Data stored at: {FEGA_REC_APP.outpath}') 
            else:
                messagebox.showwarning('Atribute not set', 'Select all the parameters to proceed')
        else:
            messagebox.showerror('Configuration alert', 'You must set the configuration path before starting the process')
     
    def borrar(self):
         self.provincias.set("")
       
if __name__ == '__main__':
    app = Fega()
    if os.path.exists('./config/config.txt'):
        fd = open('./config/config.txt')
        lines = fd.readlines()
        print(lines)
        if len(lines) == 2:
            user_sql_url = lines[0]
            ogr_path = lines[1]
            
    app.mainloop()