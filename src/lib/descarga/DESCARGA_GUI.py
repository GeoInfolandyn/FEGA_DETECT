import customtkinter as ctk
import tkinter as tk
from tkinter import filedialog, messagebox
from datetime import datetime
import subprocess
import sys
import threading

class SentinelIndexProcessorApp(ctk.CTkFrame):
    def __init__(self):
        super().__init__()
        self.title("Sentinel-2 Index Processor")
        self.geometry("550x450")
        self.resizable(False, False)
        self.iconbitmap(r'Icono FegaApp.ico')
        ctk.set_default_color_theme("green")
        ctk.set_appearance_mode('light')

        # Variables
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
            'ARE3', 'NBR', 'CONC', 'B1', 'B2', 'B3', 'B4', 'B5', 'B6', 'B7', 'B8',
            'B9', 'B10', 'B11', 'B12', 'SCL'
        ]

        self.create_widgets()

    def create_widgets(self):
        main_frame = ctk.CTkFrame(self, corner_radius=10)
        main_frame.pack(fill="both", expand=True)

        # Start Date
        ctk.CTkLabel(main_frame, text="Start Date:").grid(row=0, column=0, pady=(25,5), padx=(30,10), sticky="w")
        ctk.CTkEntry(main_frame, textvariable=self.start_date_var, width=200).grid(row=0, column=1, pady=(25,5), padx=(0,5))
        ctk.CTkButton(main_frame, text="📅", command=lambda: self.open_calendar('start')).grid(row=0, column=2, padx=(5,5),pady=(25,5))

        # End Date
        ctk.CTkLabel(main_frame, text="End Date:").grid(row=1, column=0, pady=5, padx=(30,10), sticky="w")
        ctk.CTkEntry(main_frame, textvariable=self.end_date_var, width=200).grid(row=1, column=1, pady=5, padx=(0,5))
        ctk.CTkButton(main_frame, text="📅", command=lambda: self.open_calendar('end')).grid(row=1, column=2, padx=5)

        # Index Type
        ctk.CTkLabel(main_frame, text="Index Type:").grid(row=2, column=0, pady=5, padx=(30,10), sticky="w")
        ctk.CTkComboBox(main_frame, values=self.index_options, variable=self.index_type_var).grid(row=2, column=1, pady=5)

        # Resolution
        ctk.CTkLabel(main_frame, text="Resolution:").grid(row=3, column=0, pady=5, padx=(30,10), sticky="w")
        ctk.CTkComboBox(main_frame, values=["10", "20", "60"], variable=self.resolution_var).grid(row=3, column=1, pady=5)

        # Output Directory
        ctk.CTkLabel(main_frame, text="Output Directory:").grid(row=4, column=0, pady=5, padx=(30,10), sticky="w")
        ctk.CTkEntry(main_frame, textvariable=self.output_dir_var, width=200).grid(row=4, column=1, pady=5)
        ctk.CTkButton(main_frame, text="📁", command=self.select_output_directory).grid(row=4, column=2, padx=5)

        # Tile
        ctk.CTkLabel(main_frame, text="Tile:").grid(row=5, column=0, pady=5, padx=(30,10), sticky="w")
        ctk.CTkEntry(main_frame, textvariable=self.tile_var, width=200).grid(row=5, column=1, pady=5)

        # Clip Path
        ctk.CTkLabel(main_frame, text="Clip Path (Optional):").grid(row=6, column=0, pady=5, padx=(30,10), sticky="w")
        ctk.CTkEntry(main_frame, textvariable=self.clip_path_var, width=200).grid(row=6, column=1, pady=5)
        ctk.CTkButton(main_frame, text="📁", command=self.select_clip_path).grid(row=6, column=2, padx=5)
        

        # Output Format
        ctk.CTkLabel(main_frame, text="Output Format:").grid(row=7, column=0, pady=5, sticky="w",padx=(30,10) )
        ctk.CTkComboBox(main_frame, values=["ENVI", "NetCDF"], variable=self.driver_var).grid(row=7, column=1, pady=5)

        # Process Button
        ctk.CTkButton(main_frame, text="Process Sentinel-2 Data",fg_color="Blue", command=self.process_data).grid(row=8, column=0, columnspan=3, pady=20)

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
        
        def set_date():
            selected_date = f"{year.get()}-{month.get()}-{day.get()}"
            if date_type == 'start':
                self.start_date_var.set(selected_date)
            else:
                self.end_date_var.set(selected_date)
            top.destroy()

        ctk.CTkButton(cal_frame, text="Select", command=set_date).grid(row=1, column=1, pady=10)

    def select_output_directory(self):
        directory = filedialog.askdirectory()
        if directory:
            self.output_dir_var.set(directory)

    def select_clip_path(self):
        filepath = filedialog.askopenfilename(filetypes=[("Shapefile", "*.shp")])
        if filepath:
            self.clip_path_var.set(filepath)

    def process_data(self):
        # Validate inputs
        print(self.start_date_var.get(), self.end_date_var.get(), self.output_dir_var.get(), self.tile_var.get())
        if not all([self.start_date_var.get(), self.end_date_var.get(), 
                    self.output_dir_var.get(), self.tile_var.get(), self.resolution_var.get()]):
            messagebox.showerror("Error", "Please fill all required fields")
            return

        try:
            # Construct command
            cmd = [
                sys.executable,  # Current Python interpreter
                r"lib\descarga\descarga_planet.py",  # Your main script
                self.output_dir_var.get(),
                self.start_date_var.get(),
                self.end_date_var.get(),
                self.index_type_var.get(),
                str(self.resolution_var.get()),
                self.driver_var.get()
            ]

            # Optional tile
            if self.tile_var.get():
                cmd.extend(["--tile", self.tile_var.get()])

            # Optional clip path
            if self.clip_path_var.get():
                cmd.extend(["--clip_path", self.clip_path_var.get()])

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
            threading.Thread(target=update_text, daemon=True).start()

        except Exception as e:
            messagebox.showerror("Error", str(e))

   
def main():
    app = SentinelIndexProcessorApp()
    app.mainloop()
    

if __name__ == "__main__":
    main()
