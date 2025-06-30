"""
Módulo de procesamiento de datos para ETL
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from .validator import DataValidator
from .transformer import DataTransformer
from models.history import ETLLoadHistory


class DataProcessor:
    """Procesador principal de datos ETL"""
    
    def __init__(self, session_id: str, chunk_size: int = 1000):
        self.session_id = session_id
        self.chunk_size = chunk_size
        self.validator = DataValidator(session_id)
        self.transformer = DataTransformer(session_id)
        self.logger = logging.getLogger(__name__)
        self.load_history = None
    
    def process_file(self, file_path: str, config: Dict) -> Dict:
        """Procesar archivo completo con configuración dada"""
        try:
            # Iniciar registro de historial
            self.load_history = ETLLoadHistory(
                session_id=self.session_id,
                config_name=config.get('name'),
                source_file=file_path,
                target_table=config.get('target_table'),
                mode=config.get('mode', 'insert'),
                created_by=config.get('created_by')
            )
            self.load_history.save()
            
            # Leer archivo en chunks
            chunks = pd.read_csv(file_path, chunksize=self.chunk_size)
            total_rows = 0
            processed_rows = 0
            error_rows = 0
            
            # Procesar cada chunk
            start_time = datetime.now()
            
            for chunk_num, chunk in enumerate(chunks, 1):
                chunk_result = self._process_chunk(
                    chunk, 
                    config, 
                    chunk_num
                )
                
                total_rows += chunk_result['total']
                processed_rows += chunk_result['processed']
                error_rows += chunk_result['errors']
                
                # Actualizar progreso
                self._update_progress(processed_rows, total_rows)
            
            # Calcular tiempo total
            execution_time = int((datetime.now() - start_time).total_seconds())
            
            # Actualizar historial con resultados finales
            self.load_history.complete_successfully(
                total_rows=total_rows,
                inserted_rows=processed_rows,
                updated_rows=0,  # TODO: Implementar conteo de actualizaciones
                error_rows=error_rows,
                execution_time=execution_time
            )
            
            return {
                "success": True,
                "total_rows": total_rows,
                "processed_rows": processed_rows,
                "error_rows": error_rows,
                "execution_time": execution_time,
                "history_id": self.load_history.id
            }
            
        except Exception as e:
            error_msg = f"Error procesando archivo: {str(e)}"
            self.logger.error(error_msg)
            
            if self.load_history:
                self.load_history.fail_with_error(
                    error_message=error_msg,
                    execution_time=int((datetime.now() - start_time).total_seconds())
                )
            
            return {
                "success": False,
                "error": error_msg
            }
    
    def _process_chunk(self, chunk: pd.DataFrame, config: Dict, chunk_num: int) -> Dict:
        """Procesar un chunk de datos"""
        try:
            # 1. Validar datos
            validation_results = self.validator.validate_dataframe(chunk)
            
            # Si hay errores críticos, registrar y continuar con siguiente chunk
            if validation_results['summary']['errors'] > 0:
                self.logger.warning(f"Chunk {chunk_num}: {validation_results['summary']['errors']} errores encontrados")
            
            # 2. Aplicar transformaciones
            if config.get('transformations'):
                chunk = self.transformer.apply_transformations(
                    chunk,
                    config['transformations']
                )
            
            # 3. Aplicar mapeo de columnas
            if config.get('column_mapping'):
                chunk = self._apply_column_mapping(chunk, config['column_mapping'])
            
            # 4. Procesar según el modo
            processed_chunk = self._process_chunk_by_mode(
                chunk,
                config['target_table'],
                config.get('mode', 'insert')
            )
            
            return {
                "total": len(chunk),
                "processed": processed_chunk['processed'],
                "errors": processed_chunk['errors']
            }
            
        except Exception as e:
            self.logger.error(f"Error procesando chunk {chunk_num}: {e}")
            return {
                "total": len(chunk),
                "processed": 0,
                "errors": len(chunk)
            }
    
    def _apply_column_mapping(self, df: pd.DataFrame, mapping: Dict) -> pd.DataFrame:
        """Aplicar mapeo de columnas al DataFrame"""
        # Crear nuevo DataFrame con columnas mapeadas
        mapped_df = pd.DataFrame()
        
        for source_col, target_col in mapping.items():
            if source_col in df.columns:
                mapped_df[target_col] = df[source_col]
            else:
                self.logger.warning(f"Columna origen '{source_col}' no encontrada")
        
        return mapped_df
    
    def _process_chunk_by_mode(self, chunk: pd.DataFrame, table: str, mode: str) -> Dict:
        """Procesar chunk según el modo especificado"""
        from models.base import BaseModel
        
        processed = 0
        errors = 0
        
        try:
            conn = BaseModel.get_connection()
            cursor = conn.cursor()
            
            if mode == 'insert':
                # Inserción en lote
                columns = chunk.columns.tolist()
                values = [tuple(x) for x in chunk.values]
                
                placeholders = ','.join(['%s'] * len(columns))
                columns_str = ','.join([f'"{col}"' for col in columns])
                
                query = f"""
                    INSERT INTO {table} ({columns_str})
                    VALUES ({placeholders})
                """
                
                cursor.executemany(query, values)
                processed = cursor.rowcount
                
            elif mode in ['update', 'sync']:
                # Actualización/sincronización basada en clave primaria
                key_columns = self._get_primary_key_columns(cursor, table)
                
                if not key_columns:
                    raise ValueError(f"No se encontró clave primaria para tabla {table}")
                
                for _, row in chunk.iterrows():
                    try:
                        if mode == 'update':
                            self._update_record(cursor, table, row, key_columns)
                        else:  # sync
                            self._sync_record(cursor, table, row, key_columns)
                        processed += 1
                    except Exception as e:
                        self.logger.error(f"Error procesando fila: {e}")
                        errors += 1
            
            conn.commit()
            
        except Exception as e:
            self.logger.error(f"Error en procesamiento por modo '{mode}': {e}")
            conn.rollback()
            errors = len(chunk)
            
        finally:
            cursor.close()
            conn.close()
        
        return {
            "processed": processed,
            "errors": errors
        }
    
    def _get_primary_key_columns(self, cursor, table: str) -> List[str]:
        """Obtener columnas de clave primaria de una tabla"""
        cursor.execute("""
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = %s::regclass AND i.indisprimary
        """, (table,))
        
        return [row[0] for row in cursor.fetchall()]
    
    def _update_record(self, cursor, table: str, row: pd.Series, key_columns: List[str]):
        """Actualizar un registro existente"""
        # Construir condición WHERE basada en clave primaria
        where_conditions = []
        where_values = []
        
        for key in key_columns:
            if key in row:
                where_conditions.append(f'"{key}" = %s')
                where_values.append(row[key])
        
        if not where_conditions:
            raise ValueError("No se encontraron valores para columnas clave")
        
        # Construir SET para columnas no clave
        set_items = []
        set_values = []
        
        for col in row.index:
            if col not in key_columns:
                set_items.append(f'"{col}" = %s')
                set_values.append(row[col])
        
        # Construir y ejecutar query
        query = f"""
            UPDATE {table}
            SET {', '.join(set_items)}
            WHERE {' AND '.join(where_conditions)}
        """
        
        cursor.execute(query, set_values + where_values)
    
    def _sync_record(self, cursor, table: str, row: pd.Series, key_columns: List[str]):
        """Sincronizar un registro (actualizar si existe, insertar si no)"""
        # Verificar si el registro existe
        where_conditions = []
        where_values = []
        
        for key in key_columns:
            if key in row:
                where_conditions.append(f'"{key}" = %s')
                where_values.append(row[key])
        
        cursor.execute(f"""
            SELECT 1 FROM {table}
            WHERE {' AND '.join(where_conditions)}
        """, where_values)
        
        exists = cursor.fetchone() is not None
        
        if exists:
            self._update_record(cursor, table, row, key_columns)
        else:
            columns = row.index.tolist()
            values = row.values.tolist()
            placeholders = ','.join(['%s'] * len(columns))
            columns_str = ','.join([f'"{col}"' for col in columns])
            
            cursor.execute(f"""
                INSERT INTO {table} ({columns_str})
                VALUES ({placeholders})
            """, values)
    
    def _update_progress(self, processed: int, total: int):
        """Actualizar progreso del procesamiento"""
        if self.load_history:
            progress = round((processed / max(total, 1)) * 100, 2)
            self.logger.info(f"Progreso: {progress}% ({processed}/{total} filas)")
    
    @classmethod
    def get_processing_status(cls, history_id: int) -> Dict:
        """Obtener estado del procesamiento"""
        history = ETLLoadHistory.find_by_id(history_id)
        if not history:
            return {"error": "Historial no encontrado"}
        
        return history.get_summary()
