"""
API ETL Avanzada con todas las mejoras implementadas
"""

from fastapi import FastAPI, File, UploadFile, HTTPException, Form, BackgroundTasks
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

# Importar módulos personalizados
from modules.validator import DataValidator
from modules.transformer import DataTransformer
from modules.processor import DataProcessor
from modules.notifier import NotificationManager
from modules.job_manager import JobManager
from models.config import ETLConfig, ETLConfigVersion
from models.history import ETLLoadHistory
from models.validation import ETLDataValidation
from models.notification import ETLNotificationConfig

# Configuración
from config import DATABASE_CONFIG as DB_CONFIG, APP_CONFIG, UPLOAD_DIR, LOGGING_CONFIG
import logging.config

# Configurar logging
logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger(__name__)

# Crear aplicación FastAPI
app = FastAPI(
    title="ETL Tool API Avanzada", 
    version="2.0.0",
    description="Sistema ETL avanzado con validaciones, transformaciones personalizadas, notificaciones y más"
)

# Configurar CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Inicializar gestores
job_manager = JobManager(max_workers=3)
notification_manager = NotificationManager()

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
        self.validation_results = None
        self.created_at = datetime.now()

# Modelos Pydantic
class ETLConfigRequest(BaseModel):
    name: str
    description: str
    column_mapping: Dict[str, str]
    transformations: Dict[str, Dict]
    target_table: str
    mode: str = 'insert'
    encoding: str = 'latin1'
    version: Optional[str] = None

class ValidationRequest(BaseModel):
    session_id: str
    sheet: str

class NotificationConfigRequest(BaseModel):
    name: str
    type: str  # email, slack, telegram, webhook
    config: Dict
    events: Dict

# ==================== ENDPOINTS BÁSICOS ====================

@app.get("/")
async def root():
    """Endpoint raíz"""
    return {
        "message": "ETL Tool API Avanzada",
        "version": "2.0.0",
        "features": [
            "Validaciones avanzadas de datos",
            "Transformaciones personalizadas",
            "Procesamiento asíncrono",
            "Sistema de notificaciones",
            "Historial y rollback",
            "Configuraciones versionadas"
        ]
    }

@app.get("/api/etl/health")
async def health_check():
    """Endpoint de salud"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "queue_status": job_manager.get_queue_status()
    }

# ==================== ENDPOINTS DE CARGA ====================

@app.post("/api/etl/upload")
async def upload_file(file: UploadFile = File(...)):
    """Endpoint para cargar archivo"""
    try:
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
            "sheets": session.sheets,
            "file_size": os.path.getsize(file_path)
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
        
        # Obtener vista previa (primeras 10 filas)
        preview_data = df.head(10).fillna('').to_dict('records')
        session.preview_data = preview_data
        
        return {
            "columns": session.columns,
            "preview_data": preview_data,
            "total_rows": len(df),
            "data_types": {col: str(df[col].dtype) for col in df.columns}
        }
    except Exception as e:
        logger.error(f"Error en preview: {e}")
        raise HTTPException(status_code=400, detail=str(e))

# ==================== ENDPOINTS DE VALIDACIÓN ====================

@app.post("/api/etl/validate")
async def validate_data(request: ValidationRequest):
    """Endpoint para validar datos"""
    try:
        if request.session_id not in etl_sessions:
            raise HTTPException(status_code=404, detail="Sesión no encontrada")
        
        session = etl_sessions[request.session_id]
        
        if session.dataframe is None:
            raise HTTPException(status_code=400, detail="No hay datos cargados")
        
        # Crear validador y ejecutar validaciones
        validator = DataValidator(request.session_id)
        validation_results = validator.validate_dataframe(session.dataframe)
        
        # Guardar resultados en sesión
        session.validation_results = validation_results
        
        # Enviar notificaciones si hay errores críticos
        if validation_results['summary']['errors'] > 0:
            notification_manager.send_notifications('validation_error', {
                'session_id': request.session_id,
                'error_count': validation_results['summary']['errors'],
                'warning_count': validation_results['summary']['warnings']
            })
        
        return validation_results
        
    except Exception as e:
        logger.error(f"Error en validación: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/etl/validation-history/{session_id}")
async def get_validation_history(session_id: str):
    """Obtener historial de validaciones"""
    try:
        return DataValidator.get_validation_history(session_id)
    except Exception as e:
        logger.error(f"Error obteniendo historial de validaciones: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ==================== ENDPOINTS DE TRANSFORMACIONES ====================

@app.get("/api/etl/transformations/available")
async def get_available_transformations():
    """Obtener transformaciones disponibles"""
    transformer = DataTransformer("temp")
    return transformer.get_available_transformations()

@app.post("/api/etl/transformations/custom")
async def create_custom_transformation(
    name: str = Form(...),
    description: str = Form(...),
    code: str = Form(...),
    parameters: str = Form(default="{}")
):
    """Crear transformación personalizada"""
    try:
        transformer = DataTransformer("temp")
        params = json.loads(parameters) if parameters else {}
        
        # Verificar si transformación ya existe
        existing = transformer.get_custom_transformation_by_name(name)
        if existing:
            # Actualizar transformación existente
            existing.code = code
            existing.description = description
            existing.parameters = params
            existing.save()
            return {"message": "Transformación personalizada actualizada exitosamente"}
        
        success = transformer.load_custom_transformation(name, code, description, params)
        
        if success:
            return {"message": "Transformación personalizada creada exitosamente"}
        else:
            raise HTTPException(status_code=400, detail="Error validando código personalizado")
            
    except Exception as e:
        logger.error(f"Error creando transformación personalizada: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ==================== ENDPOINTS DE PROCESAMIENTO ====================

@app.post("/api/etl/process-async")
async def process_data_async(
    background_tasks: BackgroundTasks,
    session_id: str = Form(...),
    sheet: str = Form(...),
    column_mapping: str = Form(...),
    transformations: str = Form(...),
    target_table: str = Form(...),
    mode: str = Form(default='insert'),
    encoding: str = Form(default='latin1'),
    config_name: Optional[str] = Form(default=None),
    chunk_size: int = Form(default=1000)
):
    """Endpoint para procesar datos de forma asíncrona"""
    try:
        if session_id not in etl_sessions:
            raise HTTPException(status_code=404, detail="Sesión no encontrada")
        
        # Parsear configuración
        column_mapping_dict = json.loads(column_mapping) if column_mapping else {}
        transformations_dict = json.loads(transformations) if transformations else {}
        
        # Crear configuración de procesamiento
        config = {
            'name': config_name,
            'target_table': target_table,
            'mode': mode,
            'encoding': encoding,
            'column_mapping': column_mapping_dict,
            'transformations': transformations_dict,
            'created_by': f'session_{session_id}'
        }
        
        # Enviar trabajo a la cola
        job_id = job_manager.submit_job(
            process_file_job,
            session_id,
            config,
            chunk_size,
            priority=1
        )
        
        # Enviar notificación de inicio
        notification_manager.send_notifications('load_started', {
            'session_id': session_id,
            'table': target_table,
            'job_id': job_id
        })
        
        return {
            "job_id": job_id,
            "message": "Procesamiento iniciado en segundo plano",
            "status_url": f"/api/etl/job-status/{job_id}"
        }
        
    except Exception as e:
        logger.error(f"Error iniciando procesamiento asíncrono: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/etl/job-status/{job_id}")
async def get_job_status(job_id: str):
    """Obtener estado de un trabajo"""
    return job_manager.get_job_status(job_id)

@app.post("/api/etl/job-cancel/{job_id}")
async def cancel_job(job_id: str):
    """Cancelar un trabajo"""
    success = job_manager.cancel_job(job_id)
    if success:
        return {"message": "Trabajo cancelado exitosamente"}
    else:
        raise HTTPException(status_code=400, detail="No se pudo cancelar el trabajo")

# ==================== ENDPOINTS DE CONFIGURACIONES ====================

@app.post("/api/etl/config/save-versioned")
async def save_versioned_config(config: ETLConfigRequest):
    """Guardar configuración con versionado"""
    try:
        # Buscar configuración existente
        existing_config = ETLConfig.find_by_name(config.name)
        
        if existing_config:
            # Crear nueva versión
            version = config.version or ETLConfigVersion.get_latest_version_number(existing_config.id)
            existing_config.create_version(
                version=version,
                config_data=config.dict(),
                created_by=config.dict().get('created_by'),
                make_active=True
            )
            return {"message": f"Nueva versión {version} creada para configuración existente"}
        else:
            # Crear nueva configuración
            new_config = ETLConfig(
                name=config.name,
                description=config.description,
                config_data=config.dict()
            )
            new_config.save()
            
            # Crear primera versión
            new_config.create_version(
                version="1.0",
                config_data=config.dict(),
                created_by=config.dict().get('created_by'),
                make_active=True
            )
            
            return {"message": "Nueva configuración creada con versión 1.0"}
            
    except Exception as e:
        logger.error(f"Error guardando configuración versionada: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/etl/config/{name}/versions")
async def get_config_versions(name: str):
    """Obtener versiones de una configuración"""
    try:
        config = ETLConfig.find_by_name(name)
        if not config:
            raise HTTPException(status_code=404, detail="Configuración no encontrada")
        
        versions = config.get_all_versions()
        return {
            "config_name": name,
            "versions": [
                {
                    "version": v.version,
                    "is_active": v.is_active,
                    "created_by": v.created_by,
                    "created_at": v.created_at.isoformat() if v.created_at else None
                }
                for v in versions
            ]
        }
    except Exception as e:
        logger.error(f"Error obteniendo versiones: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/etl/config/{name}/activate-version")
async def activate_config_version(name: str, version: str = Form(...)):
    """Activar una versión específica de configuración"""
    try:
        config = ETLConfig.find_by_name(name)
        if not config:
            raise HTTPException(status_code=404, detail="Configuración no encontrada")
        
        version_obj = ETLConfigVersion.find_by_config_and_version(config.id, version)
        if not version_obj:
            raise HTTPException(status_code=404, detail="Versión no encontrada")
        
        version_obj.activate()
        return {"message": f"Versión {version} activada exitosamente"}
        
    except Exception as e:
        logger.error(f"Error activando versión: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/etl/config/{name}/compare")
async def compare_config_versions(name: str, version_a: str, version_b: str):
    """Comparar dos versiones de configuración"""
    try:
        config = ETLConfig.find_by_name(name)
        if not config:
            raise HTTPException(status_code=404, detail="Configuración no encontrada")
        
        version_a_obj = ETLConfigVersion.find_by_config_and_version(config.id, version_a)
        version_b_obj = ETLConfigVersion.find_by_config_and_version(config.id, version_b)
        
        if not version_a_obj or not version_b_obj:
            raise HTTPException(status_code=404, detail="Una o ambas versiones no encontradas")
        
        comparison = version_a_obj.compare_with(version_b_obj.id)
        return comparison
        
    except Exception as e:
        logger.error(f"Error comparando versiones: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ==================== ENDPOINTS DE HISTORIAL ====================

@app.get("/api/etl/history")
async def get_load_history(limit: int = 50):
    """Obtener historial de cargas"""
    try:
        history = ETLLoadHistory.get_recent_loads(limit)
        return {
            "history": [h.get_summary() for h in history]
        }
    except Exception as e:
        logger.error(f"Error obteniendo historial: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/etl/history/statistics")
async def get_load_statistics(days: int = 30):
    """Obtener estadísticas de cargas"""
    try:
        return ETLLoadHistory.get_statistics(days)
    except Exception as e:
        logger.error(f"Error obteniendo estadísticas: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/etl/history/{history_id}/rollback")
async def rollback_load(history_id: int):
    """Hacer rollback de una carga"""
    try:
        history = ETLLoadHistory.find_by_id(history_id)
        if not history:
            raise HTTPException(status_code=404, detail="Historial no encontrado")
        
        result = history.execute_rollback()
        
        if result['success']:
            # Enviar notificación de rollback
            notification_manager.send_notifications('rollback_completed', {
                'history_id': history_id,
                'table': history.target_table,
                'deleted_count': result.get('deleted_count', 0)
            })
        
        return result
        
    except Exception as e:
        logger.error(f"Error en rollback: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ==================== ENDPOINTS DE NOTIFICACIONES ====================

@app.post("/api/etl/notifications/config")
async def create_notification_config(config: NotificationConfigRequest):
    """Crear configuración de notificación"""
    try:
        notification_config = NotificationManager.create_notification_config(
            name=config.name,
            notification_type=config.type,
            config=config.config,
            events=config.events
        )
        
        return {
            "message": "Configuración de notificación creada exitosamente",
            "config_id": notification_config.id
        }
        
    except Exception as e:
        logger.error(f"Error creando configuración de notificación: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/etl/notifications/templates")
async def get_notification_templates():
    """Obtener plantillas de configuración de notificaciones"""
    return NotificationManager.get_notification_templates()

@app.get("/api/etl/notifications/configs")
async def get_notification_configs():
    """Obtener configuraciones de notificación"""
    try:
        configs = ETLNotificationConfig.find_all()
        return {
            "configs": [
                {
                    "id": c.id,
                    "name": c.name,
                    "type": c.type,
                    "is_active": c.is_active,
                    "created_at": c.created_at.isoformat() if c.created_at else None
                }
                for c in configs
            ]
        }
    except Exception as e:
        logger.error(f"Error obteniendo configuraciones de notificación: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ==================== FUNCIONES AUXILIARES ====================

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

def process_file_job(session_id: str, config: Dict, chunk_size: int):
    """Función de trabajo para procesar archivo"""
    try:
        session = etl_sessions.get(session_id)
        if not session:
            raise Exception("Sesión no encontrada")
        
        # Crear procesador
        processor = DataProcessor(session_id, chunk_size)
        
        # Procesar archivo
        result = processor.process_file(session.file_path, config)
        
        # Enviar notificación de finalización
        if result['success']:
            notification_manager.send_notifications('load_completed', {
                'session_id': session_id,
                'table': config['target_table'],
                'total_rows': result['total_rows'],
                'processed_rows': result['processed_rows'],
                'error_rows': result['error_rows'],
                'success_rate': round((result['processed_rows'] / max(result['total_rows'], 1)) * 100, 2),
                'execution_time': result['execution_time'],
                'load_history_id': result['history_id']
            })
        else:
            notification_manager.send_notifications('load_failed', {
                'session_id': session_id,
                'table': config['target_table'],
                'error_message': result['error']
            })
        
        # Limpiar archivos temporales
        if os.path.exists(session.file_path):
            os.remove(session.file_path)
        
        # Limpiar sesión
        if session_id in etl_sessions:
            del etl_sessions[session_id]
        
        return result
        
    except Exception as e:
        logger.error(f"Error en trabajo de procesamiento: {e}")
        raise e

# ==================== ENDPOINTS LEGACY (COMPATIBILIDAD) ====================

@app.get("/api/etl/tables")
async def get_tables():
    """Endpoint para obtener tablas disponibles (compatibilidad)"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
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
        
        return {"tables": tables}
    except Exception as e:
        logger.error(f"Error obteniendo tablas: {e}")
        raise HTTPException(status_code=500, detail="Error obteniendo tablas de la base de datos")

@app.get("/api/etl/columns/{table_name}")
async def get_table_columns(table_name: str):
    """Endpoint para obtener columnas de una tabla específica (compatibilidad)"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
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
        
        return {"columns": columns}
    except Exception as e:
        logger.error(f"Error obteniendo columnas de tabla {table_name}: {e}")
        raise HTTPException(status_code=500, detail=f"Error obteniendo columnas: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main_advanced:app",
        host=APP_CONFIG["host"], 
        port=APP_CONFIG["port"],
        reload=APP_CONFIG["reload"]
    )
