"""
Modelo para transformaciones personalizadas ETL
"""

from datetime import datetime
from typing import Dict, List, Optional
from .base import BaseModel


class ETLCustomTransformation(BaseModel):
    """Modelo para transformaciones personalizadas ETL"""
    
    table_name = "etl_custom_transformations"
    
    def __init__(self, name: str = None, description: str = None, 
                 python_code: str = None, parameters: Dict = None,
                 category: str = "custom", is_active: bool = True,
                 created_by: str = None, **kwargs):
        super().__init__(**kwargs)
        self.name = name
        self.description = description
        self.python_code = python_code
        self.parameters = parameters or {}
        self.category = category
        self.is_active = is_active
        self.created_by = created_by
    
    @classmethod
    def find_by_name(cls, name: str):
        """Buscar transformación por nombre"""
        transformations = cls.find_all("name = %s", (name,))
        return transformations[0] if transformations else None
    
    @classmethod
    def get_active_transformations(cls):
        """Obtener transformaciones activas"""
        return cls.find_all("is_active = true", (), "name")
    
    @classmethod
    def get_by_category(cls, category: str):
        """Obtener transformaciones por categoría"""
        return cls.find_all("category = %s AND is_active = true", (category,), "name")
    
    def activate(self):
        """Activar esta transformación"""
        self.is_active = True
        return self.save()
    
    def deactivate(self):
        """Desactivar esta transformación"""
        self.is_active = False
        return self.save()
    
    def validate_code(self) -> bool:
        """Validar el código Python"""
        try:
            compile(self.python_code, '<string>', 'exec')
            return True
        except SyntaxError:
            return False
    
    def get_summary(self) -> Dict:
        """Obtener resumen de la transformación"""
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "category": self.category,
            "is_active": self.is_active,
            "parameters": self.parameters,
            "created_by": self.created_by,
            "created_at": self.created_at.isoformat() if self.created_at else None
        }
