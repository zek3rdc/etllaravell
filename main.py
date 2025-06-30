from fastapi import FastAPI, File, UploadFile, HTTPException, Form
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import pandas as pd
import numpy as np
import uuid
import os
import json
from datetime import datetime
from typing import Dict, List, Optional, Any
import psycopg2
from psycopg2.extras import RealDictCursor
import logging
from pathlib import Path

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="ETL Tool API", version="1.0.0")

# Configurar CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # En producción, especificar dominios específicos
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

from config import DATABASE_CONFIG as DB_CONFIG, APP_CONFIG, UPLOAD_DIR, LOGGING_CONFIG
import logging.config

# Configurar logging
logging.config.dictConfig(LOGGING_CONFIG)

# Almacenamiento en memoria para sesiones ETL
etl_sessions = {}

class ETLSession:
    def __init__(self, session_id: str):
        self.session_id = session_id
        self.file_path = None
        self.file_type = None
        self.sheets = []
        self.selected_sheet = None
        self.dataframe = None
        self.columns = []
        self.preview_data = []
        self.column_mapping = {}
        self.transformations = {}
        self.created_at = datetime.now()

def get_db_connection():
    """Crear conexión a la base de datos"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        logger.error(f"Error conectando a la base de datos: {e}")
        raise HTTPException(status_code=500, detail="Error de conexión a la base de datos")

def detect_file_type(filename: str) -> str:
    """Detectar tipo de archivo por extensión"""
    ext = filename.lower().split('.')[-1]
    if ext in ['xlsx', 'xls']:
        return 'excel'
    elif ext == 'csv':
        return 'csv'
    else:
        raise HTTPException(status_code=400, detail="Tipo de archivo no soportado")

def read_excel_sheets(file_path: str) -> List[str]:
    """Obtener lista de hojas de un archivo Excel"""
    try:
        excel_file = pd.ExcelFile(file_path)
        return excel_file.sheet_names
    except Exception as e:
        logger.error(f"Error leyendo hojas de Excel: {e}")
        raise HTTPException(status_code=400, detail="Error leyendo archivo Excel")

def read_file_data(file_path: str, file_type: str, sheet_name: Optional[str] = None, encoding: str = 'latin1') -> pd.DataFrame:
    """Leer datos del archivo"""
    try:
        if file_type == 'excel':
            if sheet_name:
                df = pd.read_excel(file_path, sheet_name=sheet_name)
            else:
                df = pd.read_excel(file_path)
        elif file_type == 'csv':
            # Intentar detectar el separador automáticamente
            with open(file_path, 'r', encoding=encoding) as f:
                first_line = f.readline()
                if ';' in first_line:
                    separator = ';'
                elif ',' in first_line:
                    separator = ','
                elif '\t' in first_line:
                    separator = '\t'
                else:
                    separator = ','
            
            df = pd.read_csv(file_path, sep=separator, encoding=encoding)
        
        # Limpiar nombres de columnas
        df.columns = df.columns.astype(str).str.strip()
        
        return df
    except Exception as e:
        logger.error(f"Error leyendo archivo: {e}")
        raise HTTPException(status_code=400, detail=f"Error leyendo archivo: {str(e)}")

def apply_transformations(df: pd.DataFrame, transformations: Dict) -> pd.DataFrame:
    """Aplicar transformaciones a los datos"""
    df_transformed = df.copy()
    
    for column, transform_config in transformations.items():
        if column not in df_transformed.columns:
            continue
            
        transform_type = transform_config.get('type')
        options = transform_config.get('options', {})
        
        try:
            if transform_type == 'date':
                df_transformed[column] = transform_date_column(df_transformed[column], options)
            elif transform_type == 'number':
                df_transformed[column] = transform_number_column(df_transformed[column], options)
            elif transform_type == 'text':
                df_transformed[column] = transform_text_column(df_transformed[column], options)
            elif transform_type == 'replace':
                df_transformed[column] = transform_replace_column(df_transformed[column], options)
        except Exception as e:
            logger.warning(f"Error transformando columna {column}: {e}")
    
    return df_transformed

def transform_date_column(series: pd.Series, options: Dict) -> pd.Series:
    """Transformar columna de fechas"""
    date_format_from = options.get('date_format_from', 'auto')
    date_format_to = options.get('date_format_to', '%Y-%m-%d')
    
    if date_format_from == 'auto':
        # Intentar detectar formato automáticamente
        return pd.to_datetime(series, infer_datetime_format=True).dt.strftime(date_format_to)
    else:
        return pd.to_datetime(series, format=date_format_from).dt.strftime(date_format_to)

def transform_number_column(series: pd.Series, options: Dict) -> pd.Series:
    """Transformar columna numérica"""
    decimal_separator = options.get('decimal_separator', '.')
    
    if decimal_separator == ',':
        # Reemplazar comas por puntos para conversión numérica
        series = series.astype(str).str.replace(',', '.')
    
    return pd.to_numeric(series, errors='coerce')

def transform_text_column(series: pd.Series, options: Dict) -> pd.Series:
    """Transformar columna de texto"""
    text_transform = options.get('text_transform', 'none')
    
    series = series.astype(str)
    
    if text_transform == 'upper':
        return series.str.upper()
    elif text_transform == 'lower':
        return series.str.lower()
    elif text_transform == 'title':
        return series.str.title()
    elif text_transform == 'trim':
        return series.str.strip()
    
    return series

def transform_replace_column(series: pd.Series, options: Dict) -> pd.Series:
    """Reemplazar valores en columna"""
    replace_from = options.get('replace_from', '')
    replace_to = options.get('replace_to', '')
    
    if replace_from:
        return series.astype(str).str.replace(replace_from, replace_to)
    
    return series

def get_database_tables() -> List[Dict]:
    """Obtener lista de tablas disponibles en la base de datos"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Obtener tablas del esquema public
        cursor.execute("""
            SELECT table_name, 
                   COALESCE(obj_description(c.oid), table_name) as display_name
            FROM information_schema.tables t
            LEFT JOIN pg_class c ON c.relname = t.table_name
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """)
        
        tables = []
        for row in cursor.fetchall():
            tables.append({
                'name': row['table_name'],
                'display_name': row['display_name']
            })
        
        cursor.close()
        conn.close()
        
        return tables
    except Exception as e:
        logger.error(f"Error obteniendo tablas: {e}")
        raise HTTPException(status_code=500, detail="Error obteniendo tablas de la base de datos")

def get_table_columns_info(table_name: str) -> List[Dict]:
    """Obtener información detallada de las columnas de una tabla"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Obtener información detallada de las columnas
        cursor.execute("""
            SELECT 
                cols.column_name as name,
                cols.data_type,
                cols.is_nullable = 'NO' as required,
                cols.column_default,
                cols.character_maximum_length,
                cols.numeric_precision,
                cols.numeric_scale,
                COALESCE(pgd.description, '') as description,
                CASE 
                    WHEN cols.data_type = 'character varying' THEN 'text'
                    WHEN cols.data_type = 'timestamp without time zone' THEN 'datetime'
                    WHEN cols.data_type = 'double precision' THEN 'number'
                    WHEN cols.data_type = 'integer' THEN 'number'
                    WHEN cols.data_type = 'boolean' THEN 'boolean'
                    ELSE cols.data_type
                END as type
            FROM information_schema.columns cols
            LEFT JOIN pg_class pgc ON pgc.relname = cols.table_name
            LEFT JOIN pg_description pgd ON pgd.objoid = pgc.oid 
                AND pgd.objsubid = cols.ordinal_position
            WHERE cols.table_name = %s 
            AND cols.table_schema = 'public'
            AND cols.column_name NOT IN ('id', 'created_at', 'updated_at')
            ORDER BY cols.ordinal_position
        """, (table_name,))
        
        columns = []
        for row in cursor.fetchall():
            columns.append({
                'name': row['name'],
                'type': row['type'],
                'required': row['required'],
                'description': row['description'],
                'default': row['column_default'],
                'max_length': row['character_maximum_length'],
                'precision': row['numeric_precision'],
                'scale': row['numeric_scale']
            })
        
        cursor.close()
        conn.close()
        
        return columns
    except Exception as e:
        logger.error(f"Error obteniendo columnas de tabla {table_name}: {e}")
        raise HTTPException(status_code=500, detail=f"Error obteniendo columnas: {str(e)}")

@app.get("/api/etl/columns/{table_name}")
async def get_table_columns(table_name: str):
    """Endpoint para obtener columnas de una tabla específica"""
    try:
        columns = get_table_columns_info(table_name)
        return {"columns": columns}
    except Exception as e:
        logger.error(f"Error obteniendo columnas de tabla {table_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

def clean_table_for_insert(table_name: str, conn) -> None:
    """Limpiar tabla para inserción en modo 'insert' (subir en limpio)"""
    cursor = conn.cursor()
    try:
        # Cerrar cualquier transacción pendiente
        conn.rollback()
        
        # Iniciar nueva transacción con autocommit para operaciones DDL
        conn.autocommit = True
        
        # Deshabilitar triggers temporalmente
        cursor.execute(f'ALTER TABLE "{table_name}" DISABLE TRIGGER ALL')
        
        # Volver a modo transaccional
        conn.autocommit = False
        cursor.execute("BEGIN")
        
        # Deshabilitar restricciones de llave foránea temporalmente
        cursor.execute("SET CONSTRAINTS ALL DEFERRED")
        
        # Truncar la tabla y sus dependientes
        cursor.execute(f'TRUNCATE TABLE "{table_name}" CASCADE')
        
        # Confirmar la transacción de limpieza
        conn.commit()
        
        # Rehabilitar triggers en modo autocommit
        conn.autocommit = True
        cursor.execute(f'ALTER TABLE "{table_name}" ENABLE TRIGGER ALL')
        conn.autocommit = False
        
        logger.info(f"Tabla {table_name} truncada exitosamente")
        
    except Exception as e:
        # Asegurar que volvemos al modo transaccional
        conn.autocommit = False
        conn.rollback()
        # Intentar rehabilitar triggers en caso de error
        try:
            conn.autocommit = True
            cursor.execute(f'ALTER TABLE "{table_name}" ENABLE TRIGGER ALL')
            conn.autocommit = False
        except:
            pass
        logger.error(f"Error truncando tabla {table_name}: {e}")
        raise HTTPException(status_code=500, detail=f"Error limpiando tabla: {str(e)}")
    finally:
        # Asegurar que estamos en modo transaccional
        conn.autocommit = False

def insert_data_to_table(df: pd.DataFrame, table_name: str, column_mapping: Dict, mode: str = 'insert') -> Dict:
    """Insertar datos en la tabla de destino"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Si el modo es 'insert' (subir en limpio), truncar la tabla primero
        if mode == 'insert':
            clean_table_for_insert(table_name, conn)
            # Iniciar nueva transacción para la inserción
            cursor.execute("BEGIN")
        
        # Obtener columnas de la tabla destino
        cursor.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns 
            WHERE table_name = %s AND table_schema = 'public'
            AND column_name NOT IN ('id', 'created_at', 'updated_at')
            ORDER BY ordinal_position
        """, (table_name,))
        
        table_columns = {row[0]: {'type': row[1], 'nullable': row[2]} for row in cursor.fetchall()}
        
        if not table_columns:
            raise HTTPException(status_code=400, detail=f"No se encontraron columnas válidas en la tabla {table_name}")
        
        # Mapear columnas según el mapping
        df_mapped = df.copy()
        
        # Si hay mapeo, aplicarlo
        if column_mapping:
            # Crear un mapeo inverso para encontrar las columnas del DataFrame
            reverse_mapping = {v: k for k, v in column_mapping.items()}
            
            # Filtrar solo las columnas que están mapeadas y existen en la tabla
            valid_mapped_columns = {}
            for table_col, df_col in reverse_mapping.items():
                if table_col in table_columns and df_col in df_mapped.columns:
                    valid_mapped_columns[df_col] = table_col
            
            if not valid_mapped_columns:
                raise HTTPException(status_code=400, detail="No hay columnas válidas para mapear")
            
            # Renombrar solo las columnas válidas
            df_final = df_mapped[list(valid_mapped_columns.keys())].rename(columns=valid_mapped_columns)
        else:
            # Sin mapeo, usar columnas que coincidan directamente
            valid_columns = [col for col in df_mapped.columns if col in table_columns]
            if not valid_columns:
                raise HTTPException(status_code=400, detail="No hay columnas que coincidan con la tabla destino")
            df_final = df_mapped[valid_columns]
        
        if df_final.empty or len(df_final.columns) == 0:
            raise HTTPException(status_code=400, detail="No hay datos válidos para insertar")
        
        # Preparar datos para inserción/actualización
        rows_inserted = 0
        rows_updated = 0
        rows_with_errors = 0
        
        # Obtener columnas clave para actualización/sincronización
        key_columns = []
        if mode in ['update', 'sync']:
            cursor.execute("""
                SELECT a.attname
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = %s::regclass AND i.indisprimary
            """, (table_name,))
            key_columns = [row[0] for row in cursor.fetchall()]
            
            # Si no hay clave primaria, buscar columnas únicas
            if not key_columns:
                cursor.execute("""
                    SELECT a.attname
                    FROM pg_index i
                    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                    WHERE i.indrelid = %s::regclass AND i.indisunique
                    LIMIT 1
                """, (table_name,))
                key_columns = [row[0] for row in cursor.fetchall()]
            
            # Filtrar solo las columnas clave que están en los datos mapeados
            available_key_columns = [col for col in key_columns if col in df_final.columns]
            
            if not available_key_columns:
                logger.warning(f"No se encontraron columnas clave mapeadas para la tabla {table_name}. Columnas clave disponibles: {key_columns}, Columnas mapeadas: {list(df_final.columns)}")
                raise HTTPException(status_code=400, detail=f"No se encontraron columnas clave mapeadas en la tabla {table_name}. Asegúrese de mapear al menos una columna clave como 'cedula' o 'id'. Columnas clave disponibles: {key_columns}")
            
            key_columns = available_key_columns
        
        for index, row in df_final.iterrows():
            # Filtrar valores no nulos o usar valores por defecto
            row_data = {}
            for col in df_final.columns:
                value = row[col]
                if pd.isna(value):
                    # Si la columna permite NULL, usar None, sino saltar la fila
                    if table_columns[col]['nullable'] == 'YES':
                        row_data[col] = None
                    else:
                        # Si es una columna requerida y está vacía, saltar esta fila
                        logger.warning(f"Fila {index}: columna requerida '{col}' está vacía")
                        rows_with_errors += 1
                        break
                else:
                    row_data[col] = value
            
            # Si no se completó el row_data (por columnas requeridas vacías), continuar
            if len(row_data) != len(df_final.columns):
                continue
            
            # Construir query de inserción
            columns = list(row_data.keys())
            values = list(row_data.values())
            
            if not columns:  # Verificación adicional
                logger.warning(f"Fila {index}: no hay columnas válidas para insertar")
                rows_with_errors += 1
                continue
            
            placeholders = ', '.join(['%s'] * len(values))
            columns_str = ', '.join(f'"{col}"' for col in columns)  # Escapar nombres de columnas
            
            insert_query = f'INSERT INTO "{table_name}" ({columns_str}) VALUES ({placeholders})'
            
            try:
                if mode == 'insert':
                    # Inserción normal
                    cursor.execute(insert_query, values)
                    rows_inserted += 1
                elif mode in ['update', 'sync']:
                    # Construir condición WHERE para actualización
                    where_conditions = []
                    where_values = []
                    for key in key_columns:
                        if key in row_data:
                            where_conditions.append(f'"{key}" = %s')
                            where_values.append(row_data[key])
                    
                    if not where_conditions:
                        logger.warning(f"Fila {index}: no hay valores para columnas clave")
                        rows_with_errors += 1
                        continue
                    
                    # Verificar si el registro existe
                    check_query = f'SELECT 1 FROM "{table_name}" WHERE {" AND ".join(where_conditions)}'
                    cursor.execute(check_query, where_values)
                    exists = cursor.fetchone() is not None
                    
                    if exists:
                        # Actualizar registro existente
                        set_values = []
                        update_values = []
                        for col, val in row_data.items():
                            if col not in key_columns:
                                set_values.append(f'"{col}" = %s')
                                update_values.append(val)
                        
                        if set_values:
                            update_query = f'UPDATE "{table_name}" SET {", ".join(set_values)} WHERE {" AND ".join(where_conditions)}'
                            cursor.execute(update_query, update_values + where_values)
                            rows_updated += 1
                    elif mode == 'sync':
                        # En modo sync, insertar si no existe
                        cursor.execute(insert_query, values)
                        rows_inserted += 1
                    else:
                        rows_with_errors += 1
            except Exception as e:
                logger.warning(f"Error procesando fila {index}: {e}")
                rows_with_errors += 1
                continue
        
        conn.commit()
        cursor.close()
        conn.close()
        
        if rows_inserted == 0 and rows_updated == 0:
            raise HTTPException(status_code=400, detail=f"No se pudieron procesar datos. Errores en {rows_with_errors} filas.")
        
        logger.info(f"Procesadas {rows_inserted + rows_updated} filas. Insertadas: {rows_inserted}, Actualizadas: {rows_updated}, Errores: {rows_with_errors}")
        return {
            "inserted": rows_inserted,
            "updated": rows_updated,
            "errors": rows_with_errors,
            "total": rows_inserted + rows_updated
        }
        
    except Exception as e:
        logger.error(f"Error insertando datos: {e}")
        if "HTTPException" in str(type(e)):
            raise e
        raise HTTPException(status_code=500, detail=f"Error insertando datos: {str(e)}")

@app.post("/api/etl/upload")
async def upload_file(file: UploadFile = File(...)):
    """Endpoint para cargar archivo"""
    try:
        # Generar ID de sesión único
        session_id = str(uuid.uuid4())
        
        # Guardar archivo temporalmente
        file_extension = file.filename.split('.')[-1]
        temp_filename = f"{session_id}.{file_extension}"
        file_path = UPLOAD_DIR / temp_filename
        
        with open(file_path, "wb") as buffer:
            content = await file.read()
            buffer.write(content)
        
        # Detectar tipo de archivo
        file_type = detect_file_type(file.filename)
        
        # Crear sesión ETL
        session = ETLSession(session_id)
        session.file_path = str(file_path)
        session.file_type = file_type
        
        # Si es Excel, obtener hojas
        if file_type == 'excel':
            session.sheets = read_excel_sheets(str(file_path))
        else:
            session.sheets = ['default']
        
        etl_sessions[session_id] = session
        
        return {
            "session_id": session_id,
            "file_id": session_id,
            "file_type": file_type,
            "sheets": session.sheets
        }
    except Exception as e:
        logger.error(f"Error en upload: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/etl/preview")
async def get_preview(session_id: str = Form(...), sheet: str = Form(...)):
    """Endpoint para obtener vista previa de datos"""
    try:
        if session_id not in etl_sessions:
            raise HTTPException(status_code=404, detail="Sesión no encontrada")
        
        session = etl_sessions[session_id]
        
        # Leer datos del archivo
        sheet_name = sheet if sheet != 'default' else None
        df = read_file_data(session.file_path, session.file_type, sheet_name)
        
        # Guardar en sesión
        session.dataframe = df
        session.selected_sheet = sheet
        session.columns = list(df.columns)
        
        # Obtener vista previa (primeras 5 filas)
        preview_data = df.head(5).fillna('').to_dict('records')
        session.preview_data = preview_data
        
        return {
            "columns": session.columns,
            "preview_data": preview_data,
            "total_rows": len(df)
        }
    except Exception as e:
        logger.error(f"Error en preview: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/api/etl/tables")
async def get_tables():
    """Endpoint para obtener tablas disponibles"""
    try:
        tables = get_database_tables()
        return {"tables": tables}
    except Exception as e:
        logger.error(f"Error obteniendo tablas: {e}")
        raise HTTPException(status_code=500, detail=str(e))

class ETLConfig(BaseModel):
    name: str
    description: str
    column_mapping: Dict[str, str]
    transformations: Dict[str, Dict]
    target_table: str
    mode: str = 'insert'
    encoding: str = 'latin1'

@app.post("/api/etl/config/save")
async def save_config(config: ETLConfig):
    """Guardar configuración de ETL"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO etl_configs (name, description, config_data)
            VALUES (%s, %s, %s)
            ON CONFLICT (name) DO UPDATE 
            SET description = EXCLUDED.description,
                config_data = EXCLUDED.config_data
        """, (config.name, config.description, json.dumps(config.dict())))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return {"message": "Configuración guardada exitosamente"}
    except Exception as e:
        logger.error(f"Error guardando configuración: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/etl/configs")
async def get_configs():
    """Obtener lista de configuraciones de ETL"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        cursor.execute("SELECT name, description, created_at FROM etl_configs ORDER BY created_at DESC")
        configs = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return {"configs": configs}
    except Exception as e:
        logger.error(f"Error obteniendo configuraciones: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/etl/config/{name}")
async def get_config(name: str):
    """Obtener configuración de ETL"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        cursor.execute("SELECT * FROM etl_configs WHERE name = %s", (name,))
        config = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        if not config:
            raise HTTPException(status_code=404, detail="Configuración no encontrada")
        
        return config
    except Exception as e:
        logger.error(f"Error obteniendo configuración: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/etl/process")
async def process_data(
    session_id: str = Form(...),
    sheet: str = Form(...),
    column_mapping: str = Form(...),
    transformations: str = Form(...),
    target_table: str = Form(...),
    mode: str = Form(default='insert'),
    encoding: str = Form(default='latin1'),
    config_name: Optional[str] = Form(default=None)
):
    """Endpoint para procesar y cargar datos"""
    try:
        if session_id not in etl_sessions:
            raise HTTPException(status_code=404, detail="Sesión no encontrada")
        
        session = etl_sessions[session_id]
        
        # Parsear JSON
        column_mapping_dict = json.loads(column_mapping) if column_mapping else {}
        transformations_dict = json.loads(transformations) if transformations else {}
        
        # Obtener DataFrame
        df = session.dataframe
        if df is None:
            raise HTTPException(status_code=400, detail="No hay datos cargados")
        
        # Aplicar transformaciones
        df_transformed = apply_transformations(df, transformations_dict)
        
        # Insertar datos en la tabla
        result = insert_data_to_table(df_transformed, target_table, column_mapping_dict, mode)
        
        # Guardar configuración si se especifica
        if config_name:
            try:
                config_data = {
                    "name": config_name,
                    "description": f"Configuración para tabla {target_table}",
                    "column_mapping": column_mapping_dict,
                    "transformations": transformations_dict,
                    "target_table": target_table,
                    "mode": mode,
                    "encoding": encoding
                }
                
                conn = get_db_connection()
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO etl_configs (name, description, config_data, created_at)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (name) DO UPDATE 
                    SET description = EXCLUDED.description,
                        config_data = EXCLUDED.config_data,
                        updated_at = %s
                """, (config_name, config_data["description"], json.dumps(config_data), datetime.now(), datetime.now()))
                conn.commit()
                cursor.close()
                conn.close()
            except Exception as e:
                logger.warning(f"Error guardando configuración: {e}")
        
        # Limpiar archivos temporales
        if os.path.exists(session.file_path):
            os.remove(session.file_path)
        
        # Limpiar sesión
        del etl_sessions[session_id]
        
        return {
            "success": True,
            "result": result,
            "target_table": target_table,
            "mode": mode,
            "report": {
                "total_rows": result["total"],
                "inserted_rows": result["inserted"],
                "updated_rows": result["updated"],
                "error_rows": result["errors"],
                "success_rate": round((result["total"] / (result["total"] + result["errors"]) * 100), 2) if (result["total"] + result["errors"]) > 0 else 0
            }
        }
    except Exception as e:
        logger.error(f"Error procesando datos: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/")
async def root():
    """Endpoint raíz"""
    return {"message": "ETL Tool API", "version": "1.0.0"}

@app.get("/health")
async def health_check():
    """Endpoint de salud"""
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host=APP_CONFIG["host"], 
        port=APP_CONFIG["port"],
        reload=APP_CONFIG["reload"]
    )
