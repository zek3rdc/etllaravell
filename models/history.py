"""
Modelo para historial de cargas ETL
"""

from datetime import datetime
from typing import Dict, List, Optional
from .base import BaseModel
import json


class ETLLoadHistory(BaseModel):
    """Modelo para historial de cargas ETL"""
    
    table_name = "etl_load_history"
    
    def __init__(self, session_id: str = None, config_name: str = None, source_file: str = None,
                 target_table: str = None, mode: str = None, total_rows: int = 0,
                 inserted_rows: int = 0, updated_rows: int = 0, error_rows: int = 0,
                 success_rate: float = 0.0, execution_time: int = None, status: str = "pending",
                 error_message: str = None, rollback_data: Dict = None, created_by: str = None,
                 completed_at: datetime = None, **kwargs):
        super().__init__(**kwargs)
        self.session_id = session_id
        self.config_name = config_name
        self.source_file = source_file
        self.target_table = target_table
        self.mode = mode
        self.total_rows = total_rows
        self.inserted_rows = inserted_rows
        self.updated_rows = updated_rows
        self.error_rows = error_rows
        self.success_rate = success_rate
        self.execution_time = execution_time
        self.status = status
        self.error_message = error_message
        self.rollback_data = rollback_data or {}
        self.created_by = created_by
        self.completed_at = completed_at
    
    @classmethod
    def find_by_session(cls, session_id: str):
        """Buscar historial por session_id"""
        histories = cls.find_all("session_id = %s", (session_id,))
        return histories[0] if histories else None
    
    @classmethod
    def get_recent_loads(cls, limit: int = 50):
        """Obtener cargas recientes"""
        return cls.find_all("", (), f"created_at DESC LIMIT {limit}")
    
    @classmethod
    def get_loads_by_table(cls, table_name: str):
        """Obtener cargas por tabla"""
        return cls.find_all("target_table = %s", (table_name,), "created_at DESC")
    
    @classmethod
    def get_failed_loads(cls):
        """Obtener cargas fallidas"""
        return cls.find_all("status = 'failed'", (), "created_at DESC")
    
    @classmethod
    def get_statistics(cls, days: int = 30) -> Dict:
        """Obtener estadísticas de cargas de los últimos N días"""
        conn = cls.get_connection()
        cursor = conn.cursor()
        
        try:
            # Estadísticas generales
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_loads,
                    COUNT(CASE WHEN status = 'completed' THEN 1 END) as successful_loads,
                    COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed_loads,
                    AVG(success_rate) as avg_success_rate,
                    AVG(execution_time) as avg_execution_time,
                    SUM(total_rows) as total_rows_processed,
                    SUM(inserted_rows) as total_inserted,
                    SUM(updated_rows) as total_updated,
                    SUM(error_rows) as total_errors
                FROM etl_load_history 
                WHERE created_at >= NOW() - INTERVAL '%s days'
            """, (days,))
            
            stats = cursor.fetchone()
            
            # Estadísticas por tabla
            cursor.execute("""
                SELECT 
                    target_table,
                    COUNT(*) as loads_count,
                    AVG(success_rate) as avg_success_rate,
                    MAX(created_at) as last_load
                FROM etl_load_history 
                WHERE created_at >= NOW() - INTERVAL '%s days'
                GROUP BY target_table
                ORDER BY loads_count DESC
            """, (days,))
            
            table_stats = cursor.fetchall()
            
            return {
                "period_days": days,
                "general": {
                    "total_loads": stats[0] or 0,
                    "successful_loads": stats[1] or 0,
                    "failed_loads": stats[2] or 0,
                    "success_percentage": round((stats[1] or 0) / max(stats[0] or 1, 1) * 100, 2),
                    "avg_success_rate": round(stats[3] or 0, 2),
                    "avg_execution_time": round(stats[4] or 0, 2),
                    "total_rows_processed": stats[5] or 0,
                    "total_inserted": stats[6] or 0,
                    "total_updated": stats[7] or 0,
                    "total_errors": stats[8] or 0
                },
                "by_table": [
                    {
                        "table": row[0],
                        "loads_count": row[1],
                        "avg_success_rate": round(row[2] or 0, 2),
                        "last_load": row[3].isoformat() if row[3] else None
                    }
                    for row in table_stats
                ]
            }
        finally:
            cursor.close()
            conn.close()
    
    def start_processing(self):
        """Marcar como iniciado el procesamiento"""
        self.status = "processing"
        self.started_at = datetime.now()
        return self.save()
    
    def complete_successfully(self, total_rows: int, inserted_rows: int, updated_rows: int, error_rows: int, execution_time: int):
        """Marcar como completado exitosamente"""
        self.status = "completed"
        self.total_rows = total_rows
        self.inserted_rows = inserted_rows
        self.updated_rows = updated_rows
        self.error_rows = error_rows
        self.success_rate = round((total_rows - error_rows) / max(total_rows, 1) * 100, 2)
        self.execution_time = execution_time
        self.completed_at = datetime.now()
        return self.save()
    
    def fail_with_error(self, error_message: str, execution_time: int = None):
        """Marcar como fallido"""
        self.status = "failed"
        self.error_message = error_message
        self.execution_time = execution_time
        self.completed_at = datetime.now()
        return self.save()
    
    def can_rollback(self) -> bool:
        """Verificar si se puede hacer rollback"""
        return (
            self.status == "completed" and 
            self.rollback_data and 
            len(self.rollback_data) > 0 and
            self.mode in ["insert", "sync"]  # Solo para modos que insertan datos
        )
    
    def prepare_rollback_data(self, inserted_ids: List[int] = None, backup_data: Dict = None):
        """Preparar datos para rollback"""
        rollback_info = {
            "timestamp": datetime.now().isoformat(),
            "mode": self.mode,
            "target_table": self.target_table,
            "inserted_rows": self.inserted_rows,
            "updated_rows": self.updated_rows
        }
        
        if inserted_ids:
            rollback_info["inserted_ids"] = inserted_ids
        
        if backup_data:
            rollback_info["backup_data"] = backup_data
        
        self.rollback_data = rollback_info
        return self.save()
    
    def execute_rollback(self) -> Dict:
        """Ejecutar rollback de la carga"""
        if not self.can_rollback():
            return {"success": False, "message": "No se puede hacer rollback de esta carga"}
        
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            rollback_info = self.rollback_data
            
            if self.mode == "insert":
                # Para modo insert, eliminar los registros insertados
                if "inserted_ids" in rollback_info:
                    ids_str = ",".join(map(str, rollback_info["inserted_ids"]))
                    cursor.execute(f"DELETE FROM {self.target_table} WHERE id IN ({ids_str})")
                    deleted_count = cursor.rowcount
                else:
                    # Si no hay IDs específicos, usar timestamp
                    cursor.execute(
                        f"DELETE FROM {self.target_table} WHERE created_at >= %s",
                        (self.created_at,)
                    )
                    deleted_count = cursor.rowcount
                
                conn.commit()
                
                # Crear registro de rollback
                rollback_history = ETLLoadHistory(
                    session_id=f"rollback_{self.session_id}",
                    config_name=f"ROLLBACK_{self.config_name}",
                    source_file=f"ROLLBACK_{self.source_file}",
                    target_table=self.target_table,
                    mode="rollback",
                    total_rows=deleted_count,
                    inserted_rows=0,
                    updated_rows=0,
                    error_rows=0,
                    success_rate=100.0,
                    status="completed",
                    created_by=f"ROLLBACK_OF_{self.id}",
                    completed_at=datetime.now()
                )
                rollback_history.save()
                
                return {
                    "success": True,
                    "message": f"Rollback completado. Se eliminaron {deleted_count} registros.",
                    "deleted_count": deleted_count,
                    "rollback_history_id": rollback_history.id
                }
            
            elif self.mode == "sync":
                # Para modo sync, es más complejo - necesitaríamos restaurar datos originales
                return {
                    "success": False,
                    "message": "Rollback para modo 'sync' requiere implementación específica con backup de datos originales"
                }
            
        except Exception as e:
            conn.rollback()
            return {
                "success": False,
                "message": f"Error durante rollback: {str(e)}"
            }
        finally:
            cursor.close()
            conn.close()
    
    def get_summary(self) -> Dict:
        """Obtener resumen de la carga"""
        return {
            "id": self.id,
            "session_id": self.session_id,
            "config_name": self.config_name,
            "source_file": self.source_file,
            "target_table": self.target_table,
            "mode": self.mode,
            "status": self.status,
            "total_rows": self.total_rows,
            "inserted_rows": self.inserted_rows,
            "updated_rows": self.updated_rows,
            "error_rows": self.error_rows,
            "success_rate": self.success_rate,
            "execution_time": self.execution_time,
            "can_rollback": self.can_rollback(),
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "created_by": self.created_by
        }
