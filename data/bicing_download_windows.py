import os
import requests
import py7zr

# Define the months
months = [
    (1, 'Gener'), (2, 'Febrer'), (3, 'Marc'), (4, 'Abril'),
    (5, 'Maig'), (6, 'Juny'), (7, 'Juliol'), (8, 'Agost'),
    (9, 'Setembre'), (10, 'Octubre'), (11, 'Novembre'), (12, 'Desembre')
]

# Define the base URL
base_url = "https://opendata-ajuntament.barcelona.cat/resources/bcn/BicingBCN/"

# Define the output folder
output_folder = "data_csv_bicing"

# Ensure the output folder exists
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

# Loop through the years and months
for year in [2023, 2022, 2021, 2020]:
    for month, month_name in months:
        # Construct the URL and the file name
        url = f"{base_url}{year}_{month:02d}_{month_name}_BicingNou_ESTACIONS.7z"
        filename = f"{year}_{month:02d}_{month_name}_BicingNou_ESTACIONS.7z"

        # Download the file
        print(f"Downloading {filename}...")
        response = requests.get(url)

        # Define the file path inside the output folder
        output_path = os.path.join(output_folder, filename)

        # Save the downloaded file
        with open(output_path, 'wb') as f:
            f.write(response.content)

        # Extract the file
        print(f"Extracting {filename}...")
        with py7zr.SevenZipFile(output_path, 'r') as archive:
            archive.extractall(output_folder)

        # Remove the compressed file
        os.remove(output_path)
        print(f"Compressed file {filename} removed.")

