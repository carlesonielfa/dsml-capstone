import os
import requests
import py7zr

# Define los meses
months = [
    (1, 'Gener'), (2, 'Febrer'), (3, 'Marc'), (4, 'Abril'),
    (5, 'Maig'), (6, 'Juny'), (7, 'Juliol'), (8, 'Agost'),
    (9, 'Setembre'), (10, 'Octubre'), (11, 'Novembre'), (12, 'Desembre')
]

# Define la URL base
base_url = "https://opendata-ajuntament.barcelona.cat/resources/bcn/BicingBCN/"

# Define la carpeta de destino
output_folder = "data/"

# Loop a través de los años y meses
for year in [2023, 2022, 2021, 2020]:
    for month, month_name in months:
        # Construye la URL y el nombre del archivo
        url = f"{base_url}{year}_{month:02d}_{month_name}_BicingNou_ESTACIONS.7z"
        filename = f"{year}_{month:02d}_{month_name}_BicingNou_ESTACIONS.7z"
        output_path = os.path.join(output_folder, filename)
        
        # Descarga el archivo
        print(f"Descargando {filename}...")
        response = requests.get(url)
        with open(output_path, 'wb') as f:
            f.write(response.content)
        
        # Descomprime el archivo
        print(f"Descomprimiendo {filename}...")
        with py7zr.SevenZipFile(output_path, 'r') as archive:
            archive.extractall(output_folder)
        
        # Elimina el archivo comprimido
        os.remove(output_path)
        print(f"Archivo comprimido {filename} eliminado.")
