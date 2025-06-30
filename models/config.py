"""
Modelos para configuraciones ETL y sus versiones
"""

from datetime import datetime
from typing import Dict, List, Optional
from .base import BaseModel
import json


class ETLConfig(BaseModel):
    """Modelo para configuraciones ETL"""
    
    table_name = "etl_configs"
    
    def __init__(self, name: str = None, description: str = None, config_data: Dict = None, **kwargs):
        super().__init__(**kwargs)
        self.name = name
        self.description = description
        self.config_data = config_data or {}
    
    @classmethod
    def find_by_name(cls, name: str):
        """Buscar configuración por nombre"""
        configs = cls.find_all("name = %s", (name,))
        return configs[0] if configs else None
    
    def get_active_version(self):
        """Obtener la versión activa de esta configuración"""
        from .config import ETLConfigVersion
        versions = ETLConfigVersion.find_all(
            "config_id = %s AND is_active = true", 
            (self.id,)
        )
        return versions[0] if versions else None
    
    def get_all_versions(self):
        """Obtener todas las versiones de esta configuración"""
        from .config import ETLConfigVersion
        return ETLConfigVersion.find_all(
            "config_id = %s", 
            (self.id,), 
            "version DESC"
        )
    
    def create_version(self, version: str, config_data: Dict, created_by: str = None, make_active: bool = True):
        """Crear una nueva versión de la configuración"""
        from .config import ETLConfigVersion
        
        # Si se va a hacer activa, desactivar las demás
        if make_active:
            existing_versions = self.get_all_versions()
            for v in existing_versions:
                v.is_active = False
                v.save()
        
        # Verificar si ya existe la versión
        existing_version = ETLConfigVersion.find_by_config_and_version(self.id, version)
        if existing_version:
            # Actualizar versión existente
            existing_version.config_data = config_data
            existing_version.created_by = created_by
            existing_version.is_active = make_active
            return existing_version.save()
        
        # Crear nueva versión
        new_version = ETLConfigVersion(
            config_id=self.id,
            version=version,
            config_data=config_data,
            is_active=make_active,
            created_by=created_by
        )
        return new_version.save()


class ETLConfigVersion(BaseModel):
    """Modelo para versiones de configuraciones ETL"""
    
    table_name = "etl_config_versions"
    
    def __init__(self, config_id: int = None, version: str = None, config_data: Dict = None, 
                 is_active: bool = False, created_by: str = None, **kwargs):
        super().__init__(**kwargs)
        self.config_id = config_id
        self.version = version
        self.config_data = config_data or {}
        self.is_active = is_active
        self.created_by = created_by
    
    @classmethod
    def find_by_config_and_version(cls, config_id: int, version: str):
        """Buscar versión específica de una configuración"""
        versions = cls.find_all(
            "config_id = %s AND version = %s", 
            (config_id, version)
        )
        return versions[0] if versions else None
    
    @classmethod
    def get_latest_version_number(cls, config_id: int) -> str:
        """Obtener el número de la última versión"""
        versions = cls.find_all(
            "config_id = %s", 
            (config_id,), 
            "version DESC LIMIT 1"
        )
        if not versions:
            return "1.0"
        
        # Incrementar versión (simple: 1.0 -> 1.1 -> 1.2, etc.)
        current = versions[0].version
        try:
            major, minor = current.split('.')
            return f"{major}.{int(minor) + 1}"
        except:
            return "1.0"
    
    def get_config(self):
        """Obtener la configuración padre"""
        return ETLConfig.find_by_id(self.config_id)
    
    def activate(self):
        """Activar esta versión (desactivando las demás)"""
        # Desactivar otras versiones
        other_versions = self.find_all(
            "config_id = %s AND id != %s", 
            (self.config_id, self.id)
        )
        for version in other_versions:
            version.is_active = False
            version.save()
        
        # Activar esta versión
        self.is_active = True
        return self.save()
    
    def compare_with(self, other_version_id: int) -> Dict:
        """Comparar esta versión con otra"""
        other = self.find_by_id(other_version_id)
        if not other:
            return {"error": "Versión no encontrada"}
        
        def deep_diff(dict1, dict2, path=""):
            """Comparación profunda de diccionarios"""
            differences = []
            
            # Claves en dict1 pero no en dict2
            for key in dict1:
                current_path = f"{path}.{key}" if path else key
                if key not in dict2:
                    differences.append({
                        "type": "removed",
                        "path": current_path,
                        "old_value": dict1[key],
                        "new_value": None
                    })
                elif isinstance(dict1[key], dict) and isinstance(dict2[key], dict):
                    differences.extend(deep_diff(dict1[key], dict2[key], current_path))
                elif dict1[key] != dict2[key]:
                    differences.append({
                        "type": "modified",
                        "path": current_path,
                        "old_value": dict1[key],
                        "new_value": dict2[key]
                    })
            
            # Claves en dict2 pero no en dict1
            for key in dict2:
                current_path = f"{path}.{key}" if path else key
                if key not in dict1:
                    differences.append({
                        "type": "added",
                        "path": current_path,
                        "old_value": None,
                        "new_value": dict2[key]
                    })
            
            return differences
        
        return {
            "version_a": self.version,
            "version_b": other.version,
            "differences": deep_diff(self.config_data, other.config_data)
        }
