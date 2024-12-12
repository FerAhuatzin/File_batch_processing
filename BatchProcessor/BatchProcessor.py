import os
import pandas as pd
import json
import xml.etree.ElementTree as ET
from datetime import datetime

input_folder = 'BatchProcessor/raw'
output_folder = 'BatchProcessor/transform_data'

def observeData (consolidacion: pd.DataFrame):
    print("\nOBSERVING DATA\n",
          "\nHEAD\n", consolidacion.head(), 
          "\n\nDESCRIBE\n",consolidacion.describe(),
          "\n\nNULL VALUES\n",consolidacion.isnull().sum(),
          "\n\nINFO")
    consolidacion.info()

def processJson (file_path:str)-> pd.DataFrame:
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return pd.json_normalize(data)

def processXml (file_path:str)-> pd.DataFrame:
    file = ET.parse(file_path)
    root = file.getroot()
    data = []
    for child in root:
        data.append({subchild.tag: subchild.text for subchild in child})
    return pd.DataFrame(data)

def processCsv (file_path:str)-> pd.DataFrame:
    return pd.read_csv(file_path)

def transformData () -> pd.DataFrame:
    join_data = []
    for file_name in os.listdir(input_folder):
        file_path = os.path.join(input_folder, file_name)
        if os.path.isfile(file_path):
            if file_path.endswith('.json'):
                join_data.append(processJson(file_path))
            elif file_path.endswith('.csv'):
                join_data.append(processCsv(file_path))
            elif file_path.endswith('.xml'):
                join_data.append(processXml(file_path))
    return join_data

def preprocess (consolidacion: pd.DataFrame)  -> pd.DataFrame:
    consolidacion = consolidacion.dropna(subset=["fecha", "cantidad", "total_venta"])
    consolidacion["cantidad"] = pd.to_numeric(consolidacion["cantidad"], errors="coerce").astype("int")
    consolidacion["total_venta"] = pd.to_numeric(consolidacion["total_venta"], errors="coerce").astype("int")
    consolidacion["fecha"] = pd.to_datetime(consolidacion["fecha"])
    consolidacion["cantidad"] = consolidacion["cantidad"].abs()
    consolidacion["total_venta"] = consolidacion["total_venta"].abs()
    return consolidacion

def getMonthlySales (consolidacion:pd.DataFrame) -> pd.DataFrame:
    consolidacion["mes"] = consolidacion["fecha"].dt.to_period("M")
    monthly_sales = consolidacion.groupby(["mes", "producto_id"])["cantidad"].sum().reset_index()
    return monthly_sales

def getTop10Products(consolidacion: pd.DataFrame, year: int):
    consolidacion["year"] = consolidacion["fecha"].dt.year
    year_sales = consolidacion[consolidacion["year"] == year]
    top_sales = year_sales.groupby("producto_id")["cantidad"].sum().reset_index()
    top_sales_sorted = top_sales.sort_values(by="cantidad", ascending=False)
    top_10 = top_sales_sorted.head(10)
    return top_10


def process_batch ():

    fecha_de_ejecucion = datetime.now().strftime('%Y-%m-%d')
    
    #CONSOLIDACIÓN (Join files in raw folder)
    consolidacion = pd.concat(transformData())
    print(f"Data consolidated...")
    #Used to analyze how to clean data
    #observeData(consolidacion)
    consolidacion = preprocess(consolidacion.copy())
    #observeData(consolidacion)
    output_file_name = f'CONSOLIDACIÓN_{fecha_de_ejecucion}.csv'
    output_file_path = os.path.join(output_folder, output_file_name)
    consolidacion.to_csv(output_file_path, index=False)
    print(f"Consolidated data saved in {output_file_path}")

    #VENTAS_MES (monthly sales per product)
    monthly_sales = getMonthlySales(consolidacion.copy())
    print(f"Monthly sales obtained...")
    output_file_name = f'VENTAS_MES_{fecha_de_ejecucion}.csv'
    output_file_path = os.path.join(output_folder, output_file_name)
    monthly_sales.to_csv(output_file_path, index=False)
    print(f"Monthly sales saved in {output_file_path}")

    #PRODUCTOS_TOP (top 10 saled products)
    top_products = getTop10Products(consolidacion.copy(),2024)
    print(f"Top 10 products per year obtained...")
    output_file_name = f'PRODUCTOS_TOP_{fecha_de_ejecucion}.csv'
    output_file_path = os.path.join(output_folder, output_file_name)
    top_products.to_csv(output_file_path, index=False)
    print(f"Top 10 products sales saved in {output_file_path}")

process_batch()