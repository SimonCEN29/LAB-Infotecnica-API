# %%
# Cambio directorio de trabajo a subcarpeta Datos
import os
os.chdir('Datos')
#print(os.getcwd())

# %%
#Importación de librerías y definición de funciones
import requests
import json
import pandas as pd
import time
import numpy as np

# multithread
from concurrent.futures import ThreadPoolExecutor

# para saltarse los warnings de los requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# Función que maneja la solicitud para un ID específico
def fetch_data(id, categorias, slug_tipo_ficha):
    url = f"https://api-infotecnica.coordinador.cl/v1/secciones-tramos/{id}/fichas-tecnicas/{slug_tipo_ficha}/"
    response = requests.get(url, verify=False)
    valores = {"id": id}

    if response.status_code == 200:
        data = response.json()
        for categoria in categorias:
            if categoria in data:
                valores[categoria] = data[categoria]["valor_texto"]
            else:
                valores[categoria] = np.nan
        return valores
    else:
        print(f"Error: {response.status_code} para el ID {id}")
        return None


# Establecer un ThreadPoolExecutor para manejar el paralelismo
def secciones_tramos_detalle(
    ids_list, categories_list, ficha_type, column_map=None, max_workers=40
):
    """Fetch data in parallel using ThreadPoolExecutor and return a DataFrame.

    Args:
    - ids_list: List of IDs for data fetching.
    - categories_list: List of categories to fetch.
    - ficha_type: Type of ficha to fetch.
    - column_map: Dictionary to rename columns.
    - max_workers: Number of parallel workers for ThreadPoolExecutor. Default is 40.

    Returns:
    - df: DataFrame containing fetched data for each ID.
    - elapsed_time: Time taken to fetch all data in seconds.
    """
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        start_time = time.time()
        results = list(
            executor.map(
                fetch_data,
                ids_list,
                [categories_list] * len(ids_list),
                [ficha_type] * len(ids_list),
            )
        )
        end_time = time.time()

    df = pd.DataFrame(results)
    if column_map:
        df.rename(columns=column_map, inplace=True)

    elapsed_time = end_time - start_time
    return df, elapsed_time


def get_data_from_infotecnica(url_parte_final):
    url_completa = f"https://api-infotecnica.coordinador.cl/v1/{url_parte_final}"
    try:
        # Intenta realizar la solicitud HTTP
        response = requests.get(url_completa, verify=False)
        response.raise_for_status()  # Genera una excepción si la solicitud no es exitosa

        # Si la solicitud fue exitosa, convierte los datos en un DataFrame
        data = json.loads(response.text)
        df = pd.DataFrame(data)
        return df
    except requests.exceptions.RequestException as e:
        # Captura cualquier excepción relacionada con la solicitud HTTP
        print(f"Error de solicitud HTTP: {e}")

# %%
# Consulta datos Secciones Tramos
# Llama a la función con la parte final de la URL como argumento
ext_secc_tramos = "secciones-tramos/"
df_secc_tramos = get_data_from_infotecnica(ext_secc_tramos)
df_secc_tramos.to_excel("df_secciones_tramos_IT_0.xlsx", index=False)

# Obtengo los ID de todas las secciones
ids_secciones_tramos = df_secc_tramos["id"].tolist()

# %% 
# DATOS GENERALES
categorias_secciones_tramos_general = ["5917", "5895", "1005", "5902"]

tipo_ficha = "general"

column_map_secciones_tramos_general = {
    "5917": "Fecha EO",
    "5895": "Tensión nominal (kV)",
    "1005": "Longitud Conductor (km)",
    "5902": "Tipo de conductor",
}

# Usage
df_secc_tramos_general, time_taken_general = secciones_tramos_detalle(
    ids_secciones_tramos,
    categorias_secciones_tramos_general,
    tipo_ficha,
    column_map_secciones_tramos_general,
)
print(f"Tiempo total para la busqueda: {time_taken_general:.2f} segundos")
df_secc_tramos_general.to_excel("df_secciones_tramos_IT_1.xlsx", index=False)


# %%
# DATOS Termicos
categorias_secciones_tramos_termicos = [
    "1561",
    "1563",
    "1565",
    "1567",
    "1569",
    "1571",
    "1573",
    "1575",
]

tipo_ficha = "limites-termicos"

column_map_secciones_tramos_termicos = {
    "1561": "0°C",
    "1563": "5°C",
    "1565": "10°C",
    "1567": "15°C",
    "1569": "20°C",
    "1571": "25°C",
    "1573": "30°C",
    "1575": "35°C",
}

# Usage
df_secc_tramos_termicos, time_taken_termico = secciones_tramos_detalle(
    ids_secciones_tramos,
    categorias_secciones_tramos_termicos,
    tipo_ficha,
    column_map_secciones_tramos_termicos,
)
print(f"Tiempo total secciones tramos termicos: {time_taken_termico:.6f} segundos")
df_secc_tramos_termicos.to_excel("df_secciones_tramos_IT_2.xlsx", index=False)


# %%
# Limpieza y merge de datos Infotécnica
# Limpio los df para hacer el merge
df_secc_tramos = df_secc_tramos[["id", "nombre", "linea_nombre", "circuito_nombre", "id_tramo"]]

# Start with the first dataframe
merged_df = df_secc_tramos

# List of all dataframes to be merged
dataframes_to_merge = [df_secc_tramos_general, df_secc_tramos_termicos]

# Iteratively merge each dataframe based on the 'id' column
for df in dataframes_to_merge:
    merged_df = merged_df.merge(df, on="id", how="outer")

merged_df.to_excel("df_secciones_tramos.xlsx", index=False)


# %%
# Filtrado por Vnom
# 154; 220; 500; vacío y que nombre contenga 154, 220 o 500; o 110 y que ID esté en tablas ERST año anterior.
# merged_df = pd.read_excel("df_secciones_tramos.xlsx")

# Reemplazar comas por puntos
merged_df["Tensión nominal (kV)"] = merged_df["Tensión nominal (kV)"].str.replace(",", ".")
merged_df["Tensión nominal (kV)"] = merged_df["Tensión nominal (kV)"].replace("", np.nan)
merged_df["Tensión nominal (kV)"] = merged_df["Tensión nominal (kV)"].astype(float)

#Obtención líneas 110 kV ERST anterior
df_lineas_ERST = pd.ExcelFile("Lineas_ERST_2_ant.xlsx")
zonas = df_lineas_ERST.sheet_names
num_zonas = len(zonas)
ID_lineas_110kV = []
for izona in range(0,num_zonas):
    zona = zonas[izona]
    df_Zona = pd.read_excel(df_lineas_ERST, sheet_name=izona)
    ID_lineas_110kV_Zona = df_Zona[df_Zona["Tensión nominal (kV)"]==110]["ID"].tolist()
    ID_lineas_110kV = ID_lineas_110kV + ID_lineas_110kV_Zona
#print(ID_lineas_110kV)
#print(merged_df.dtypes)
merged_df = merged_df[(merged_df["Tensión nominal (kV)"].isin([154,220,500]))|((merged_df["Tensión nominal (kV)"].isna())&\
    (merged_df["linea_nombre"].str.contains('154|220|500')))|((merged_df["Tensión nominal (kV)"]==110)&\
    (merged_df["id"].isin(ID_lineas_110kV)))]

merged_df.to_excel("df_secciones_tramos_2.xlsx", index=False)
df_lineas_ERST.close()

# %%
# Reordenamiento y renombrado columnas
#merged_df = pd.read_excel("df_secciones_tramos_2.xlsx")
df_cleaned_lines = merged_df

#Se reordenan columnas para tabla líneas y se deja al final Fecha EO (para obtener tramos nuevos) e
#id_tramo (para rescatar datos TTCC).
lista_reordenada = [
    "id",
    "linea_nombre",
    "circuito_nombre",
    "nombre",
    "Tensión nominal (kV)",
    "Longitud Conductor (km)",
    "Tipo de conductor",
    "0°C",
    "5°C",
    "10°C",
    "15°C",
    "20°C",
    "25°C",
    "30°C",
    "35°C",
    "id_tramo",
    "Fecha EO"
]

df_cleaned_lines = df_cleaned_lines[lista_reordenada]

dict_nombres_final = {
    "id": "ID",
    "linea_nombre": "Nombre Línea",
    "circuito_nombre": "Nombre Circuito",
    "nombre": "Nombre Tramo"
}
df_cleaned_lines.rename(columns=dict_nombres_final, inplace=True)

df_cleaned_lines.to_excel("df_secciones_tramos_3.xlsx", index=False)
# df_cleaned_lines.dropna(subset=["Tensión Nominal (kV)", "Nombre Línea"], inplace=True)

# %%
# Conversión de datos numéricos a tipo float y transformación de capacidades de kA a MVA
#df_cleaned_lines = pd.read_excel("df_secciones_tramos_3.xlsx")

columns_to_convert = [
    "Tensión nominal (kV)",
    "Longitud Conductor (km)",
    "0°C",
    "5°C",
    "10°C",
    "15°C",
    "20°C",
    "25°C",
    "30°C",
    "35°C",
]

# Reemplazar las comas por puntos y convertir a float
for col in columns_to_convert:
    # Reemplazar comas por puntos
    df_cleaned_lines[col] = df_cleaned_lines[col].astype(str).str.replace(",", ".")
    # Convertir cadenas no numéricas a NaN, para luego convertir columnas a tipo float
    df_cleaned_lines[col] = pd.to_numeric(df_cleaned_lines[col], errors="coerce")
    # Convertir a float
    df_cleaned_lines[col] = df_cleaned_lines[col].astype(float)

# Lista de columnas a transformar
temperature_columns = [
    "0°C",
    "5°C",
    "10°C",
    "15°C",
    "20°C",
    "25°C",
    "30°C",
    "35°C",
]

# Multiplicar cada columna de temperatura por 'Tensión Nominal (kV)' y por sqrt(3)
for col in temperature_columns:
    # multiplico
    df_cleaned_lines[col] = (
        df_cleaned_lines[col] * df_cleaned_lines["Tensión nominal (kV)"] * np.sqrt(3)
    )
    # redondeo de capacidades termicas
    df_cleaned_lines[col] = df_cleaned_lines[col].round()

df_cleaned_lines.to_excel("df_secciones_tramos_4.xlsx", index=False)

# %% 
# Lectura ID´s tramos por zona de ERST anterior y merge con datos de Infotécnica
#df_cleaned_lines = pd.read_excel("df_secciones_tramos_4.xlsx")
file_lineas_ERST = pd.ExcelFile("Lineas_ERST_2_ant.xlsx")
zonas = file_lineas_ERST.sheet_names
#print(zonas)
num_zonas = len(zonas)
writer = pd.ExcelWriter("Lineas_ERST.xlsx")
for izona in range(0,num_zonas):
    zona = zonas[izona]
    #print(zona)
    df_Zona = pd.read_excel(file_lineas_ERST, sheet_name=izona)
    df_ID_Zona = df_Zona.loc[:,["ID"]] #Extrae columna "ID", pero manteniendo formato Dataframe
    #print(df_ID_Zona.head())
    #print(df_ID_Zona.shape)
    df_ID_Zona = df_ID_Zona.merge(
        df_cleaned_lines,
        on="ID",
        how="left" # Se usa "left" para ver los índices de los tramos que ya no existen.
        #Al incorporar manualmente Tramos Nuevos se debe eliminar las filas en blanco correspondientes a estos índices.
    )
    df_lineas_Zona = df_ID_Zona.drop(labels=["Fecha EO"], axis='columns')
    df_lineas_Zona.to_excel(writer, sheet_name=zona, na_rep="-", index=False)
writer.close()
file_lineas_ERST.close()


# %%
# Obtención tabla Tramos Nuevos
# Filtrado de tramos con ID's que no estaban en df_secciones_tramos_4.xlsx de consulta anterior.
#df_cleaned_lines = pd.read_excel("df_secciones_tramos_4.xlsx")
df_cleaned_lines_ant = pd.read_excel("df_secciones_tramos_4_ant.xlsx")
df_cleaned_lines.drop(df_cleaned_lines[df_cleaned_lines["ID"].isin(df_cleaned_lines_ant["ID"])].index, inplace=True)

#Se transforma columna "Fecha EO" a tipo datetime. Si no se reconoce como tipo fecha, queda vacío (NaT):
df_cleaned_lines["Fecha EO 2"] = pd.to_datetime(df_cleaned_lines["Fecha EO"], dayfirst=True, errors="coerce")

df_cleaned_lines.to_excel("Tramos_Nuevos.xlsx", na_rep="-", index=False)

# -----------------------------------------------------------------------------------------------------
# Generar manualmente "Tramos_Nuevos_2.xlsx", identificando tramos de STN y de STZ/STD considerados en ERST,
# y clasificándolos por Zona. Para identificar ST se considera último informe de calificación de instalaciones 
# de Tx de la CNE (cuadrienal) y BD OP.
# Luego generar manualmente "Lineas_ERST_2.xlsx", incorporando dichos tramos y corrigiendo 
# errores de magnitud de valores de capacidad térmica. Este es el archivo de entrada al códido de TTCC.
# También se debe eliminar las filas en blanco con IDs de tramos que ya no existen.
# -----------------------------------------------------------------------------------------------------


# %%
# Eliminación columnas "id_tramo" y "Tensión nominal (kV)" para obtener tabla final de líneas
file_lineas_ERST = pd.ExcelFile("Lineas_ERST_2.xlsx")
zonas = file_lineas_ERST.sheet_names
num_zonas = len(zonas)
writer = pd.ExcelWriter("Lineas_ERST_final.xlsx")
for izona in range(0,num_zonas):
    zona = zonas[izona]
    df_Zona = pd.read_excel(file_lineas_ERST, sheet_name=izona)
    df_Zona.drop(["id_tramo", "Tensión nominal (kV)"], axis=1, inplace=True)
    df_Zona.to_excel(writer, sheet_name=zona, index=False)
writer.close()
file_lineas_ERST.close()


# %%
