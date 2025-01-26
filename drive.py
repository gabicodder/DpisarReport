import os
import io
import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import pickle

# Configuración
SCOPES = ['https://www.googleapis.com/auth/drive']
FOLDER_ID = '1u9VMnyrvH5kdgWxfrdKYI1LIphZwdqhL'  # Reemplaza con el ID de tu carpeta en Google Drive

def authenticate_drive():
    """Autenticar con la API de Google Drive."""
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    return build('drive', 'v3', credentials=creds)

def list_files(service, folder_id):
    """Listar archivos en una carpeta específica."""
    query = f"'{folder_id}' in parents and trashed=false"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    return results.get('files', [])

def download_file(service, file_id, file_name, output_dir):
    """Descargar un archivo de Google Drive."""
    request = service.files().get_media(fileId=file_id)
    file_path = os.path.join(output_dir, file_name)
    with io.FileIO(file_path, 'wb') as file:
        downloader = MediaIoBaseDownload(file, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
    return file_path

def unify_files(input_dir, output_file):
    """Unificar todos los archivos de texto o CSV en uno solo."""
    
    report_dir = 'report'
    os.makedirs(report_dir, exist_ok=True)
        
    all_data = []
    all_data_tienda = []
    all_data_vendedor = []
    all_data_asistencia = []
    
    for file_name in os.listdir(input_dir):
        file_path = os.path.join(input_dir, file_name)
        if file_name.endswith('.csv'):
            data = pd.read_csv(file_path)
            all_data.append(data)
        if file_name.endswith('.xlsx') or file_name.endswith('.xls'):
            
            # Leer todas las hojas
            all_sheets = pd.read_excel(file_path, sheet_name=None)
            
            # Iterar sobre cada hoja
            for sheet_name, sheet_data in all_sheets.items():
                
                if sheet_name.strip().lower() == "metas":
                    print(f"Procesando hoja: {sheet_name} - archivo {file_name}")
                    data = pd.read_excel(file_path,sheet_name=sheet_name)
                    data['Archivo_origen'] = file_name
                    all_data_tienda.append(data)
                                
                elif sheet_name.strip().lower() == "valores ganando":
                    print(f"Procesando hoja: {sheet_name} - archivo {file_name}")
                    data = pd.read_excel(file_path,sheet_name=sheet_name)
                    data['Archivo_origen'] = file_name
                    all_data_vendedor.append(data)
                
                elif sheet_name.strip().lower() == "asistencia":
                    print(f"Procesando hoja: {sheet_name} - archivo {file_name}")
                    data = pd.read_excel(file_path,sheet_name=sheet_name)
                    data['Archivo_origen'] = file_name
                    all_data_asistencia.append(data)
                
        elif file_name.endswith('.txt'):
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
                all_data.append(pd.DataFrame({'content': [content]}))
                
    if all_data_tienda or all_data_vendedor or all_data_asistencia:
        
        file_path = os.path.join(report_dir, 'TIENDA.csv')
        unified_data = pd.concat(all_data_tienda, ignore_index=True)
        unified_data.to_csv(file_path, index=False)
        print(f"Se ha creado el archivo TIENDA")
        
        file_path = os.path.join(report_dir, 'VENDEDOR.csv')
        unified_data = pd.concat(all_data_vendedor, ignore_index=True)
        unified_data.to_csv(file_path, index=False)
        print(f"Se ha creado el archivo VENDEDOR")
        
        file_path = os.path.join(report_dir, 'ASISTENCIA.csv')
        unified_data = pd.concat(all_data_asistencia, ignore_index=True)
        unified_data.to_csv(file_path, index=False)
        print(f"Se ha creado el archivo ASISTENCIA")
        
        print(f"Archivos unificados en la carpeta: {report_dir}")
    else:
        print("No se encontraron archivos para unificar.")

def main():
    # service = authenticate_drive()
    # files = list_files(service, FOLDER_ID)
    
    # if not files:
    #     print("No se encontraron archivos en la carpeta.")
    #     return

    output_dir = 'downloads'
    # os.makedirs(output_dir, exist_ok=True)

    # for file in files:
    #     print(f"Descargando: {file['name']}")
    #     download_file(service, file['id'], file['name'], output_dir)

    output_file = 'unified_output.csv'
    unify_files(output_dir, output_file)
    ##print(f"Proceso completado. Archivos unificados en {output_file}.")

if __name__ == '__main__':
    main()
