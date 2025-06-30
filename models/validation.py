"""
Modelo para validaciones de datos ETL
"""

from datetime import datetime
from typing import Dict, List, Optional
from .base import BaseModel


class ETLDataValidation(BaseModel):
    """Modelo para validaciones de datos ETL"""
    
    table_name = "etl_data_validations"
    
    def __init__(self, session_id: str = None, column_name: str = None, 
                 validation_type: str = None, validation_result: Dict = None,
                 severity: str = "info", **kwargs):
        super().__init__(**kwargs)
        self.session_id = session_id
        self.column_name = column_name
        self.validation_type = validation_type
        self.validation_result = validation_result or {}
        self.severity = severity
    
    @classmethod
    def find_by_session(cls, session_id: str):
        """Buscar validaciones por session_id"""
        return cls.find_all("session_id = %s", (session_id,))
    
    @classmethod
    def get_validation_summary(cls, session_id: str) -> Dict:
        """Obtener resumen de validaciones para una sesión"""
        validations = cls.find_by_session(session_id)
        
        summary = {
            "total": len(validations),
            "by_severity": {"error": 0, "warning": 0, "info": 0},
            "by_type": {},
            "by_column": {}
        }
        
        for validation in validations:
            # Contar por severidad
            summary["by_severity"][validation.severity] += 1
            
            # Contar por tipo
            if validation.validation_type not in summary["by_type"]:
                summary["by_type"][validation.validation_type] = 0
            summary["by_type"][validation.validation_type] += 1
            
            # Contar por columna
            if validation.column_name not in summary["by_column"]:
                summary["by_column"][validation.column_name] = {"error": 0, "warning": 0, "info": 0}
            summary["by_column"][validation.column_name][validation.severity] += 1
        
        return summary
