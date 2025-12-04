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
def fetch_data_TTCC(id, categorias, slug_tipo_ficha):
    url = f"https://api-infotecnica.coordinador.cl/v1/transformadores-corrientes/{id}/fichas-tecnicas/{slug_tipo_ficha}/"
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
        return {key: np.nan for key in categorias}


# Establecer un ThreadPoolExecutor para manejar el paralelismo
def TTCC_detalle(
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
                fetch_data_TTCC,
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


# Función que consulta datos de Infotécnica por páginas, para listas que son muy largas, como la de TTCC
def get_data_by_pages_from_infotecnica(url_parte_final):
    url_completa = f"https://api-infotecnica.coordinador.cl/v1/{url_parte_final}"
    try:
        # Intenta realizar la solicitud HTTP. Se hace la consulta por páginas de 1000 filas.
        # Se consulta 1era página
        response = requests.get(url_completa, params = {'page': 1, 'page_size': 1000}, verify=False)
        response.raise_for_status()  # Genera una excepción si la solicitud no es exitosa
        data = response.json().get('results')
        # Se calcula N° páginas a consultar a partir de número total de filas
        nfilas = response.json().get("count")
        npaginas = (nfilas//1000) + ((nfilas%1000)>0)*1 #cociente nfilas/1000 (entero) y si resto es > 0, suma 1
        print(npaginas)
        # Se hace loop para leer el resto de las páginas y se van agregando a objeto data
        for page_number in range(2, npaginas + 1): 
            response = requests.get(url_completa, params = {'page': page_number, 'page_size': 1000}, verify=False)
            response.raise_for_status()  # Genera una excepción si la solicitud no es exitosa
            data += response.json().get('results')   
        # Si las solicitudes fueron exitosas, convierte los datos en un DataFrame
        df = pd.DataFrame(data)
        return df
    except requests.exceptions.RequestException as e:
        # Captura cualquier excepción relacionada con la solicitud HTTP
        print(f"Error de solicitud HTTP: {e}")


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
# Definición de funciones para limpiar columnas extremo
import re

def delete_prefix(text):
    lista_excepciones = ["Tap: ", "Paño: ", "Paño : ", "Tap : ", "Punto: "]
    str_text = str(text)  # Convert the input to string to avoid any errors.
    for excepcion in lista_excepciones:
        if str_text.startswith(excepcion):
            return re.sub(excepcion, "", str_text)
    return str_text

def process_string(text):
    elements = str(text).split()
    if elements and "/" in elements[-1]:
        elements[-1] = elements[-1].split("/")[0]
    return " ".join(elements)

def quita_tildes(text):
    text = re.sub("Á", "A", text)
    text = re.sub("É", "E", text)
    text = re.sub("Í", "I", text)
    text = re.sub("Ó", "O", text)
    text = re.sub("Ú", "U", text)
    return text

def process_and_clean_text(text):
    """Chains delete_prefix and process_string functions."""
    return process_string(delete_prefix(text))


# %%
# Definición de función para imprimir dataframe separando en pestañas por "Zona".
def impresion_por_zona(df_SEN, file):
    zonas = pd.unique(df_SEN["Zona"])
    num_zonas = len(zonas)
    #print(zonas)
    writer = pd.ExcelWriter(file)
    for izona in range(0,num_zonas):
        zona = zonas[izona]
        df_TTCC_Zona = df_SEN.loc[df_SEN["Zona"]==zona]
        df_TTCC_Zona = df_TTCC_Zona.drop("Zona", axis=1)
        df_TTCC_Zona.to_excel(writer, sheet_name=zona, na_rep="-", index=False)
    writer.close()
    return


# %% 
# Creación dataframe TTCC con datos de tramos de "Lineas_ERST_2.xlsx"
# Este último corresponde a archivo de lineas incluyendo columna "id_tramo".
file_lineas_ERST = pd.ExcelFile("Lineas_ERST_2.xlsx")
zonas = file_lineas_ERST.sheet_names
num_zonas = len(zonas)
df_TTCC_SEN = pd.DataFrame()
for izona in range(0,num_zonas):
    zona = zonas[izona]
    df_lineas_Zona = pd.read_excel(file_lineas_ERST, sheet_name=izona)
    df_TTCC_Zona = df_lineas_Zona[["Nombre Línea", "Nombre Circuito", "Tensión nominal (kV)", "id_tramo"]]
    df_TTCC_Zona = df_TTCC_Zona.dropna(how='all') #Elimina filas sin datos (IDs que ya no existen) si no se eliminaron antes.
    df_TTCC_Zona = df_TTCC_Zona.drop_duplicates(subset=['id_tramo']) #Tramos con subtramos se reducen a una sola fila.
    df_TTCC_Zona['Zona'] = zona
    df_TTCC_SEN = pd.concat([df_TTCC_SEN, df_TTCC_Zona], ignore_index=True) #ignoro índice para que queden índices correlativos
file_lineas_ERST.close()
df_TTCC_SEN.to_excel("df_TTCC_SEN.xlsx", index=False)

# %%
# Obtención datos Tramos
# Busco los tramos
extension_tramos = "tramos/"
df_tramos = get_data_from_infotecnica(extension_tramos)
df_tramos.to_excel("df_tramos_IT.xlsx", index=False)

# %%
# Merge de dataframe TTCC con datos tramos
#df_tramos = pd.read_excel("df_tramos_IT.xlsx")
#df_TTCC_SEN = pd.read_excel("df_TTCC_SEN.xlsx")

#Renombra id por id_tramo y filtra columnas, creando 1 dataframe por cada extremo,
#para luego hacer merge con ambos y así queden en distintas filas:
df_tramos.rename(columns={"id":"id_tramo", "nombre":"nombre_tramo"}, inplace=True)
df_tramos_ext1 = df_tramos[["id_tramo", "nombre_tramo", "extremo1_descripcion"]]
df_tramos_ext2 = df_tramos[["id_tramo", "nombre_tramo", "extremo2_descripcion"]]
df_tramos_ext1 = df_tramos_ext1.rename(columns={"extremo1_descripcion":"extremo"})
df_tramos_ext2 = df_tramos_ext2.rename(columns={"extremo2_descripcion":"extremo"})

#Merge de df_TTCC_SEN con dataframes de tramos:
df_TTCC_SEN_ext1 = df_TTCC_SEN.merge(df_tramos_ext1, on="id_tramo", how="left")
df_TTCC_SEN_ext2 = df_TTCC_SEN.merge(df_tramos_ext2, on="id_tramo", how="left")
df_TTCC_SEN = pd.concat([df_TTCC_SEN_ext1, df_TTCC_SEN_ext2])
df_TTCC_SEN.sort_index(inplace=True, kind='mergesort') #'mergesort' mantiene orden relativo de elementos iguales
#(para que quede primero extremo1 y luego extremo2)

# Limpieza columna "extremo":
df_TTCC_SEN["extremo"] = df_TTCC_SEN["extremo"].apply(process_and_clean_text)
# Filtrado de extremos correspondientes a taps y estructuras, que no tienen TTCC:
df_TTCC_SEN = df_TTCC_SEN[(df_TTCC_SEN["extremo"].str[0:3]!="TAP")&(df_TTCC_SEN["extremo"].str[0:3]!="EST")]
#Se elimina prefijo "PA ", que en Tramos se agregó a tramos nuevos:
#(Uso función loc en vez de asignar directamente a df_TTCC_SEN["extremo"] para evitar SettingWithCopyWarning)
df_TTCC_SEN.loc[:, "extremo"] = df_TTCC_SEN["extremo"].str.replace("PA S/E", "S/E")
#Quito tildes por si acaso, porque encontré un caso con tilde (RÏO MALLECO) en pano_nombre de datos TTCC IT:
df_TTCC_SEN["extremo"] = df_TTCC_SEN["extremo"].apply(quita_tildes)

df_TTCC_SEN.to_excel("df_TTCC_SEN_2.xlsx", index=False)


# %%
# -----------------------------------------------------------------------------------------------------
# Generar df_TTCC_SEN_2_2.xlsx haciendo lo siguiente:
# - Corrección extremos duplicados
# - Eliminación filas correspondientes a tap-offs, extremos sin TTCC o tramos inexistentes
# -----------------------------------------------------------------------------------------------------


# %%
# Consulta datos TTCC
ext_TTCC = "transformadores-corrientes/"
df_TTCC = get_data_by_pages_from_infotecnica(ext_TTCC)
df_TTCC.to_excel("df_TTCC_IT_0.xlsx", index=False)

# %%
# DATOS GENERALES TTCC
# Obtengo los ID de todos los TTCC
#df_TTCC = pd.read_excel("df_TTCC_IT_0.xlsx")
ids_TTCC = df_TTCC["id"].tolist()

categorias_TTCC_general = ["458", "6177"]
tipo_ficha = "general"
column_map_TTCC_general = {
    "458": "Razón(es) de transformación",
    "6177": "TAP seleccionado del primario",
}
max_workers = 60
# Usage
df_TTCC_general, time_taken_general = TTCC_detalle(
    ids_TTCC, categorias_TTCC_general, tipo_ficha, column_map_TTCC_general, max_workers
)
print(f"Tiempo total para la busqueda: {time_taken_general:.2f} segundos")

# DF TTCC
merged_df_TTCC = df_TTCC[["id", "subestacion_nombre", "pano_nombre", "nombre"]]
merged_df_TTCC = merged_df_TTCC.merge(df_TTCC_general, on="id", how="outer")
merged_df_TTCC = merged_df_TTCC.rename(columns={"id": "id_TC", "nombre": "nombre_TC"})
merged_df_TTCC.to_excel("df_TTCC_IT.xlsx", index=False)
df_TTCC = merged_df_TTCC


# %%
# Merge de dataframe TTCC con datos TTCC Infotécnica
# df_TTCC = pd.read_excel("df_TTCC_IT.xlsx")

df_TTCC_SEN = pd.read_excel("df_TTCC_SEN_2_2.xlsx")

#Antes del merge quito duplicados de df_TTCC según "pano_nombre", ya que puede haber varios TTCC por paño,
#como en SSEE de interruptor y medio, por ejemplo.
df_TTCC = df_TTCC.drop_duplicates(subset=['pano_nombre']) #Se deja 1er TC del paño.
#(En general en IT están datos TAP pri. de núcleos de protección. Dejando 1er TC se evitan TTCC sin dato y algunos datos 
# que corresponden a TTCC de medida, que son menores que los de protección.
# En v2025 sólo se detectó casos S/E SECCIONADORA EL ROSAL J1 y J4 con 1er valor correspondiente a TC medida < TC protección.
# Pero estos valores no resultan limitantes.
# Además, en los pocos casos en que hay 2 TTCC de protección con distinto valor primario, 1er TC corresponde al mínimo TAP pri.)

#Se elimina prefijo "PA ", que en TTCC se agregó a muchos TTCC:
#(Uso función loc en vez de asignar directamente a df_TTCC["pano_nombre"] para evitar SettingWithCopyWarning)
df_TTCC.loc[:, "pano_nombre"] = df_TTCC["pano_nombre"].str.replace("PA S/E", "S/E")
#Quito tildes, porque encontré un caso con tilde (RÏO MALLECO) en pano_nombre de datos TTCC IT:
df_TTCC["pano_nombre"] = df_TTCC["pano_nombre"].apply(quita_tildes)

#Merge:
df_TTCC_SEN = df_TTCC_SEN.merge(
    df_TTCC,
    left_on="extremo",
    right_on="pano_nombre",
    how="left"
)
df_TTCC_SEN.to_excel("df_TTCC_SEN_3.xlsx", index=False)


# %%
# Procesamiento columnas.
# Obtención paño, subestación, TAP en A, chequeo si TAP está contenido en Razón(es) de transformación, y Relación Transformación.
#df_TTCC_SEN = pd.read_excel("df_TTCC_SEN_3.xlsx")

# Obtención de "Subestación" y "Paño" de columna "extremo" (esto se puede hacer antes)
df_TTCC_SEN["Paño"] = df_TTCC_SEN["extremo"].str.split().str[-1]
df_TTCC_SEN["Subestación"] = df_TTCC_SEN["extremo"].str.split().str[1:-1].str.join(" ")
# Se procesa Tap, filtrando valores no numéricos, y se crea nueva columna "Tap transformado" pasando de kA a A
df_TTCC_SEN["TAP seleccionado del primario"] = df_TTCC_SEN["TAP seleccionado del primario"].astype(str)
df_TTCC_SEN["Tap transformado"] = df_TTCC_SEN["TAP seleccionado del primario"].str.replace(",", ".")
df_TTCC_SEN["Tap transformado"] = pd.to_numeric(df_TTCC_SEN["Tap transformado"], errors="coerce")
df_TTCC_SEN["Tap transformado"] = df_TTCC_SEN["Tap transformado"].astype(float)
df_TTCC_SEN["Tap transformado"] = df_TTCC_SEN["Tap transformado"] * 1000
# Se crea columna Contenido para ver si valor de columna "Tap transformado"
# se encuentra en columna "Razón(es) de transformación"
df_TTCC_SEN["Contenido"] = df_TTCC_SEN.apply(
    lambda x: False if pd.isnull(x["Tap transformado"])
    else str(int(x["Tap transformado"])) in x["Razón(es) de transformación"], axis=1)

df_TTCC_SEN["Tap transformado"] = df_TTCC_SEN["Tap transformado"].astype('Int64')
# Crea columna con Razón(es) de transformación depurada
# Extrae primer número después de primer "/":
df_TTCC_SEN["A sec completo"] = df_TTCC_SEN["Razón(es) de transformación"].str.split("/", expand=True)[1]
df_TTCC_SEN["A sec completo"] = df_TTCC_SEN["A sec completo"].str.replace(",", ".")
df_TTCC_SEN["A sec"] = df_TTCC_SEN["A sec completo"].str.extract('([-+]?\d*\.?\d+)')
# Sólo si Contenido es True y además Tap transformado y A sec son ambos no NA,
# concatena en nueva columna Relacion Tr:
df_TTCC_SEN["Relación de transformación_IT"] = (df_TTCC_SEN["Contenido"]&(~df_TTCC_SEN["Tap transformado"].isna())&(~df_TTCC_SEN["A sec"].isna()))*(
    df_TTCC_SEN["Tap transformado"].astype(str) + "/" + df_TTCC_SEN["A sec"])
#Elimina columnas auxiliares:
df_TTCC_SEN.drop(columns=["A sec completo", "A sec"], inplace=True)

df_TTCC_SEN.to_excel("df_TTCC_SEN_4.xlsx", index=False)


# %%
# Reordenamiento y filtrado columnas
#df_TTCC_SEN = pd.read_excel("df_TTCC_SEN_4.xlsx")

lista_reordenada = [
    "Zona",
    "Nombre Línea",
    "Nombre Circuito",
    "nombre_tramo",
    "Tensión nominal (kV)",
    "Subestación",
    "Paño",
    "id_TC",
    "Razón(es) de transformación",
    "TAP seleccionado del primario",
    "Tap transformado",
    "Contenido",
    "Relación de transformación_IT"
]
df_TTCC_SEN = df_TTCC_SEN[lista_reordenada]
df_TTCC_SEN.to_excel("df_TTCC_SEN_5.xlsx", index=False)


# %%
# Merge dataframe TTCC con tablas TTCC de corrida anterior
# Para usar cuando no hay datos del TC en Infotécnica o los datos son inconsistentes.
file_TTCC = pd.ExcelFile("TTCC_ERST_final_ant.xlsx")
zonas = file_TTCC.sheet_names
num_zonas = len(zonas)
df_TTCC_SEN_ant = pd.DataFrame()
for izona in range(0,num_zonas):
    df_TTCC_Zona_ant = pd.read_excel(file_TTCC, sheet_name=izona, 
                                     usecols=["Subestación", "Paño", "Relación de transformación"])
    df_TTCC_SEN_ant = pd.concat([df_TTCC_SEN_ant, df_TTCC_Zona_ant], ignore_index=True) #ignoro índice para que queden índices correlativos
file_TTCC.close()

#Separa pares de paños de SSEE de interruptor y medio ("paño1/paño2") en dos filas
df_TTCC_SEN_ant["Paño"] = df_TTCC_SEN_ant["Paño"].str.split("/")
df_TTCC_SEN_ant = df_TTCC_SEN_ant.explode("Paño")
#Remueve duplicados de "Subestación" y "Paño", que ocurre en SSEE de interruptor y medio
df_TTCC_SEN_ant = df_TTCC_SEN_ant.drop_duplicates(subset=['Subestación', 'Paño'])

#Merge de df_TTCC_SEN con datos ERST anterior:
df_TTCC_SEN = df_TTCC_SEN.merge(
    df_TTCC_SEN_ant,
    on=["Subestación", "Paño"],
    how="left"
)

df_TTCC_SEN = df_TTCC_SEN.rename(columns={"Relación de transformación": "Relación de transformación_ant"})
df_TTCC_SEN["Nros en razon"] = df_TTCC_SEN["Razón(es) de transformación"].str.findall(r'(\d+(?:\.\d+)?)')

df_TTCC_SEN["Apri_ant"] = df_TTCC_SEN["Relación de transformación_ant"].str.split("/", expand=True)[0]
df_TTCC_SEN["Asec_ant"] = df_TTCC_SEN["Relación de transformación_ant"].str.split("/", expand=True)[1]

df_TTCC_SEN["Contenido_ant"] = df_TTCC_SEN.apply(
    lambda x: False if pd.isnull(x["Razón(es) de transformación"])
    else (x["Apri_ant"] in x["Nros en razon"]) & 
    (x["Asec_ant"] in x["Nros en razon"]), axis=1)

df_TTCC_SEN["Apri_ant"] = pd.to_numeric(df_TTCC_SEN["Apri_ant"], errors="coerce")
#Elimina columnas auxiliares:
df_TTCC_SEN.drop(columns=["Nros en razon", "Asec_ant"], inplace=True)

df_TTCC_SEN.to_excel("df_TTCC_SEN_6.xlsx", index=False)


# %%
#Creación columna "Relación de transformación" a partir de las obtenidas de Infotécnica y de ERST anterior.
#df_TTCC_SEN = pd.read_excel("df_TTCC_SEN_6.xlsx")

#Orden de decisiones:
# 1. Si no hay dato del TC en Infotécnica --> se deja preliminarmente Relación transf. ERST anterior (luego se verifica con print out relés)
# 2. Si hay dato Infotécnia -->
  # 2.1 Si TAP Primario es consistente con Razones transformación --> se adopta dato IT (ojo con Asec cuando separador es otro, ej. ":")
  # 2.2 Si TAP Primario NO es consistente con Razones transformación --> verificar si dato ERST anterior es consistente con raoznes transf.
    # 2.2.1 Si Apri y Asec ERST anterior se encuentran en Razones transformación --> se mantiene dato ERST anterior
    # 2.2.2 Si Apri o Asec ERST anterior no se encuentran en Razones transformación --> se deja vacío (luego se revisan print out relés)

df_TTCC_SEN["Relación de transformación"] = df_TTCC_SEN.apply(
    lambda x: x["Relación de transformación_ant"] if pd.isnull(x["Razón(es) de transformación"])
    else x["Relación de transformación_IT"] if x["Contenido"]
    else x["Relación de transformación_ant"] if x["Contenido_ant"]
    else np.nan, axis=1)

df_TTCC_SEN.to_excel("df_TTCC_SEN_7.xlsx", index=False)


# --------------------------------------------------------------------------------------------------------

# Entre estos dos bloques de código se debe rellenar manualmente lo que quede vacío
# de columna "Relación de transformación" y guardar como "df_TTCC_SEN_7_2.xlsx",
# para luego calcular las capacidades definitivas en A y MVA.
# También hay que rellenar datos de Vnom faltantes, porque luego se usarán para calcular Capacidad (MVA).
# También eliminar filas correspondientes a tap-offs, sin interruptores o si tramo no existe
# (por ej. El Cobre - Esperanza en el Norte Grande).

# --------------------------------------------------------------------------------------------------------


# %%
#Cálculo capacidad en A y en MVA.
df_TTCC_SEN = pd.read_excel("df_TTCC_SEN_7_2.xlsx")

df_TTCC_SEN["Apri"] = df_TTCC_SEN["Relación de transformación"].str.split("/", expand=True)[0]
df_TTCC_SEN["Apri"] = pd.to_numeric(df_TTCC_SEN["Apri"], errors="coerce")
df_TTCC_SEN["Capacidad (A)"] = df_TTCC_SEN["Apri"] * 1.2
df_TTCC_SEN["Capacidad (MVA)"] = df_TTCC_SEN["Tensión nominal (kV)"] * (df_TTCC_SEN["Capacidad (A)"] / 1000) * np.sqrt(3)
df_TTCC_SEN["Capacidad (MVA)"] = df_TTCC_SEN["Capacidad (MVA)"].round()

#Lista final se deja sin id_TC, ya que hay varios TTCC que no están en IT y
#cuyos datos se obtienen o validan (datos ERST anterior) con printouts relés. Además, en SS/EE de interruptor y medio
#se selecciona solo el 1ero de los TTCC de cada paño.
lista_filtrada = [
    "Zona",
    "Nombre Línea",
    "Nombre Circuito",
    "Subestación",
    "Paño",
    "Relación de transformación",
    "Capacidad (A)",
    "Capacidad (MVA)"
]
df_TTCC_SEN = df_TTCC_SEN[lista_filtrada]
df_TTCC_SEN.to_excel("df_TTCC_SEN_final.xlsx", index=False)

impresion_por_zona(df_TTCC_SEN, "TTCC_ERST_final.xlsx")

# %%
