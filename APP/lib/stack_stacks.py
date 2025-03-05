from osgeo import gdal
import numpy as np
import sys, os
from tqdm import tqdm
import json
 
def find_envi_files(input_dir):
    envi_files = []
    for file in os.listdir(input_dir):
        if file.endswith(".hdr"):
            base_name = file[:-4]  # Remove .hdr extension
            data_file = os.path.join(input_dir, base_name)
            if os.path.exists(data_file):
                envi_files.append(data_file)
    return envi_files
 
def save_layout(input_dir, stacks, stack_name):
    layout = {}
    band_count = 1
    for i, ds in enumerate(stacks):
        file_name = os.path.basename(ds.GetDescription())
        for j in range(ds.RasterCount):
            band_name = f"Band_{band_count}"
            layout[band_name] = {
                "file": file_name,
                "band_index": j + 1,
                "stack_index": band_count
            }
            band_count += 1
   
    layout_file = os.path.join(input_dir, f"{stack_name}_layout.json")
    with open(layout_file, 'w') as f:
        json.dump(layout, f, indent=2)
    print(f"Layout saved to {layout_file}")
 
def stackdestack(input_dir, stack_name):
    envi_files = find_envi_files(input_dir)
    if not envi_files:
        print("No valid ENVI files found.")
        return
 
    stacks = []
    size = 0
    for file in envi_files:
        ds = gdal.Open(file)
        if ds is not None:
            size += ds.RasterCount
            stacks.append(ds)
        else:
            print(f"Couldn't open {file}")
 
    if not stacks:
        print("No valid ENVI datasets could be opened.")
        return
 
    driver = gdal.GetDriverByName("ENVI")
    stack_path = os.path.join(input_dir, stack_name)
    stack = driver.Create(stack_path, stacks[0].RasterXSize, stacks[0].RasterYSize, size, gdal.GDT_Int16)
    pbar = tqdm(total=size)
    count = 1
    for ds in stacks:
        for i in range(ds.RasterCount):
            band = ds.GetRasterBand(i + 1)
            stack.GetRasterBand(count).WriteArray(band.ReadAsArray())
            count += 1
            pbar.update(1)
   
    stack.SetProjection(stacks[0].GetProjection())
    stack.SetGeoTransform(stacks[0].GetGeoTransform())
    stack.FlushCache()
   
    # Save the layout
    save_layout(input_dir, stacks, stack_name)
   
    stack = None
    for ds in stacks:
        ds = None
    pbar.close()
    files = os.listdir(input_dir)
    for file in files:
        if os.path.splitext(os.path.basename(file))[0] != os.path.splitext(stack_name)[0] and not file.endswith('.csv') :
            os.remove(os.path.join(input_dir, file))
           
    print(f"Stack '{stack_path}' created successfully.")
 
def main():
    if len(sys.argv) != 3:
        print("Usage: python envi_stackdestack.py <stacks_dir> <stack_name>")
        sys.exit(1)
    input_dir = str(sys.argv[1])
    stack_name = str(sys.argv[2])
    stackdestack(input_dir, stack_name)
 
if __name__ == "__main__":
    main()