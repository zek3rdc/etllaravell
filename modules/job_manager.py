"""
Gestor de trabajos asíncronos para ETL
"""

import threading
from queue import Queue, PriorityQueue
from typing import Dict, List, Any, Optional, Callable
from datetime import datetime
import logging
import uuid
from models.base import BaseModel


class JobManager:
    """Gestor de trabajos asíncronos para ETL"""
    
    def __init__(self, max_workers: int = 5):
        self.max_workers = max_workers
        self.job_queue = PriorityQueue()
        self.active_jobs = {}
        self.workers = []
        self.logger = logging.getLogger(__name__)
        self._initialize_workers()
    
    def _initialize_workers(self):
        """Inicializar workers"""
        for _ in range(self.max_workers):
            worker = threading.Thread(target=self._worker_loop, daemon=True)
            worker.start()
            self.workers.append(worker)
    
    def _worker_loop(self):
        """Loop principal del worker"""
        while True:
            try:
                # Obtener siguiente trabajo
                priority, job_id, job_func, args, kwargs = self.job_queue.get()
                
                # Actualizar estado
                self._update_job_status(job_id, "processing")
                
                try:
                    # Ejecutar trabajo
                    result = job_func(*args, **kwargs)
                    self._update_job_status(job_id, "completed", result=result)
                except Exception as e:
                    self.logger.error(f"Error en trabajo {job_id}: {e}")
                    self._update_job_status(job_id, "failed", error=str(e))
                
                self.job_queue.task_done()
                
            except Exception as e:
                self.logger.error(f"Error en worker loop: {e}")
    
    def submit_job(self, func: Callable, *args, priority: int = 0, **kwargs) -> str:
        """Enviar un nuevo trabajo"""
        job_id = str(uuid.uuid4())
        
        # Registrar trabajo en base de datos
        self._create_job_record(job_id, priority, func.__name__, args, kwargs)
        
        # Agregar a la cola
        self.job_queue.put((priority, job_id, func, args, kwargs))
        
        return job_id
    
    def _create_job_record(self, job_id: str, priority: int, job_type: str, 
                          args: tuple, kwargs: dict):
        """Crear registro de trabajo en base de datos"""
        import json
        conn = BaseModel.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                INSERT INTO etl_job_queue (
                    job_id, job_type, status, priority, parameters
                ) VALUES (%s, %s, %s, %s, %s)
            """, (
                job_id, job_type, "pending", priority,
                json.dumps({
                    "args": args,
                    "kwargs": kwargs
                })
            ))
            conn.commit()
        except Exception as e:
            self.logger.error(f"Error creando registro de trabajo: {e}")
            conn.rollback()
        finally:
            cursor.close()
            conn.close()
    
    def _update_job_status(self, job_id: str, status: str, result: Any = None, 
                          error: str = None):
        """Actualizar estado de trabajo en base de datos"""
        conn = BaseModel.get_connection()
        cursor = conn.cursor()
        
        try:
            update_fields = {
                "status": status,
                "completed_at": datetime.now() if status in ["completed", "failed"] else None
            }
            
            if status == "processing":
                update_fields["started_at"] = datetime.now()
            
            if result is not None:
                update_fields["result"] = result
            
            if error is not None:
                update_fields["error_message"] = error
            
            # Construir query
            set_clause = ", ".join([f"{k} = %s" for k in update_fields.keys()])
            values = list(update_fields.values()) + [job_id]
            
            cursor.execute(f"""
                UPDATE etl_job_queue 
                SET {set_clause}
                WHERE job_id = %s
            """, values)
            
            conn.commit()
            
        except Exception as e:
            self.logger.error(f"Error actualizando estado de trabajo: {e}")
            conn.rollback()
        finally:
            cursor.close()
            conn.close()
    
    def get_job_status(self, job_id: str) -> Dict:
        """Obtener estado de un trabajo"""
        conn = BaseModel.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                SELECT job_id, job_type, status, priority, parameters,
                       progress, result, error_message, 
                       created_at, started_at, completed_at
                FROM etl_job_queue
                WHERE job_id = %s
            """, (job_id,))
            
            row = cursor.fetchone()
            if not row:
                return {"error": "Trabajo no encontrado"}
            
            return {
                "job_id": row[0],
                "job_type": row[1],
                "status": row[2],
                "priority": row[3],
                "parameters": row[4],
                "progress": row[5],
                "result": row[6],
                "error_message": row[7],
                "created_at": row[8].isoformat() if row[8] else None,
                "started_at": row[9].isoformat() if row[9] else None,
                "completed_at": row[10].isoformat() if row[10] else None
            }
            
        finally:
            cursor.close()
            conn.close()
    
    def update_job_progress(self, job_id: str, progress: int):
        """Actualizar progreso de un trabajo"""
        conn = BaseModel.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                UPDATE etl_job_queue
                SET progress = %s
                WHERE job_id = %s
            """, (progress, job_id))
            
            conn.commit()
            
        except Exception as e:
            self.logger.error(f"Error actualizando progreso: {e}")
            conn.rollback()
        finally:
            cursor.close()
            conn.close()
    
    def cancel_job(self, job_id: str) -> bool:
        """Cancelar un trabajo pendiente"""
        conn = BaseModel.get_connection()
        cursor = conn.cursor()
        
        try:
            # Solo se pueden cancelar trabajos pendientes
            cursor.execute("""
                UPDATE etl_job_queue
                SET status = 'cancelled',
                    completed_at = NOW(),
                    error_message = 'Cancelled by user'
                WHERE job_id = %s AND status = 'pending'
                RETURNING 1
            """, (job_id,))
            
            cancelled = cursor.fetchone() is not None
            conn.commit()
            
            return cancelled
            
        except Exception as e:
            self.logger.error(f"Error cancelando trabajo: {e}")
            conn.rollback()
            return False
        finally:
            cursor.close()
            conn.close()
    
    def get_queue_status(self) -> Dict:
        """Obtener estado de la cola de trabajos"""
        conn = BaseModel.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                SELECT status, COUNT(*) as count,
                       MIN(created_at) as oldest,
                       MAX(created_at) as newest
                FROM etl_job_queue
                GROUP BY status
            """)
            
            status = {}
            for row in cursor.fetchall():
                status[row[0]] = {
                    "count": row[1],
                    "oldest": row[2].isoformat() if row[2] else None,
                    "newest": row[3].isoformat() if row[3] else None
                }
            
            return {
                "queue_size": self.job_queue.qsize(),
                "active_workers": len([w for w in self.workers if w.is_alive()]),
                "max_workers": self.max_workers,
                "status": status
            }
            
        finally:
            cursor.close()
            conn.close()
    
    def cleanup_old_jobs(self, days: int = 7):
        """Limpiar trabajos antiguos"""
        conn = BaseModel.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                DELETE FROM etl_job_queue
                WHERE created_at < NOW() - INTERVAL '%s days'
                AND status IN ('completed', 'failed', 'cancelled')
            """, (days,))
            
            deleted = cursor.rowcount
            conn.commit()
            
            self.logger.info(f"Limpiados {deleted} trabajos antiguos")
            
        except Exception as e:
            self.logger.error(f"Error limpiando trabajos antiguos: {e}")
            conn.rollback()
        finally:
            cursor.close()
            conn.close()
    
    def shutdown(self, wait: bool = True):
        """Detener el gestor de trabajos"""
        if wait:
            self.job_queue.join()
        
        # Marcar trabajos pendientes como fallidos
        conn = BaseModel.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                UPDATE etl_job_queue
                SET status = 'failed',
                    completed_at = NOW(),
                    error_message = 'Job manager shutdown'
                WHERE status = 'pending'
            """)
            
            conn.commit()
            
        except Exception as e:
            self.logger.error(f"Error actualizando trabajos pendientes: {e}")
            conn.rollback()
        finally:
            cursor.close()
            conn.close()
