"""
Modelos para notificaciones ETL
"""

from datetime import datetime
from typing import Dict, List, Optional
from .base import BaseModel


class ETLNotificationConfig(BaseModel):
    """Modelo para configuraciones de notificación ETL"""
    
    table_name = "etl_notification_configs"
    
    def __init__(self, name: str = None, type: str = None, config: Dict = None,
                 events: Dict = None, is_active: bool = True, **kwargs):
        super().__init__(**kwargs)
        self.name = name
        self.type = type
        self.config = config or {}
        self.events = events or {}
        self.is_active = is_active
    
    @classmethod
    def find_by_name(cls, name: str):
        """Buscar configuración por nombre"""
        configs = cls.find_all("name = %s", (name,))
        return configs[0] if configs else None
    
    @classmethod
    def get_active_configs(cls):
        """Obtener configuraciones activas"""
        return cls.find_all("is_active = true")
    
    def activate(self):
        """Activar esta configuración"""
        self.is_active = True
        return self.save()
    
    def deactivate(self):
        """Desactivar esta configuración"""
        self.is_active = False
        return self.save()


class ETLNotificationLog(BaseModel):
    """Modelo para logs de notificaciones ETL"""
    
    table_name = "etl_notification_logs"
    
    def __init__(self, config_id: int = None, load_history_id: int = None,
                 event_type: str = None, status: str = None, message: str = None,
                 error_message: str = None, **kwargs):
        super().__init__(**kwargs)
        self.config_id = config_id
        self.load_history_id = load_history_id
        self.event_type = event_type
        self.status = status
        self.message = message
        self.error_message = error_message
    
    @classmethod
    def find_by_config(cls, config_id: int):
        """Buscar logs por configuración"""
        return cls.find_all("config_id = %s", (config_id,), "sent_at DESC")
    
    @classmethod
    def find_by_load_history(cls, load_history_id: int):
        """Buscar logs por historial de carga"""
        return cls.find_all("load_history_id = %s", (load_history_id,))
    
    @classmethod
    def get_failed_notifications(cls, hours: int = 24):
        """Obtener notificaciones fallidas en las últimas N horas"""
        return cls.find_all(
            "status = 'failed' AND sent_at >= NOW() - INTERVAL '%s hours'",
            (hours,),
            "sent_at DESC"
        )
