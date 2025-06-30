"""
Modelo base para todos los modelos ETL
"""

from datetime import datetime
from typing import Dict, Any, Optional
import psycopg2
from psycopg2.extras import RealDictCursor
import json
from config import DATABASE_CONFIG


class BaseModel:
    """Clase base para todos los modelos ETL"""
    
    table_name = None
    
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)
    
    @classmethod
    def get_connection(cls):
        """Obtener conexión a la base de datos"""
        return psycopg2.connect(**DATABASE_CONFIG)
    
    @classmethod
    def find_by_id(cls, id: int):
        """Buscar registro por ID"""
        conn = cls.get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        try:
            cursor.execute(f"SELECT * FROM {cls.table_name} WHERE id = %s", (id,))
            row = cursor.fetchone()
            return cls(**dict(row)) if row else None
        finally:
            cursor.close()
            conn.close()
    
    @classmethod
    def find_all(cls, where_clause: str = "", params: tuple = (), order_by: str = "id"):
        """Buscar todos los registros con filtros opcionales"""
        conn = cls.get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        try:
            query = f"SELECT * FROM {cls.table_name}"
            if where_clause:
                query += f" WHERE {where_clause}"
            query += f" ORDER BY {order_by}"
            
            cursor.execute(query, params)
            rows = cursor.fetchall()
            return [cls(**dict(row)) for row in rows]
        finally:
            cursor.close()
            conn.close()
    
    def save(self):
        """Guardar el modelo (insert o update)"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            if hasattr(self, 'id') and self.id:
                # Update
                self._update(cursor)
            else:
                # Insert
                self._insert(cursor)
            
            conn.commit()
            return self
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cursor.close()
            conn.close()
    
    def delete(self):
        """Eliminar el registro"""
        if not hasattr(self, 'id') or not self.id:
            raise ValueError("No se puede eliminar un registro sin ID")
        
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute(f"DELETE FROM {self.table_name} WHERE id = %s", (self.id,))
            conn.commit()
            return True
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cursor.close()
            conn.close()
    
    def _insert(self, cursor):
        """Insertar nuevo registro"""
        # Obtener atributos del objeto excluyendo 'id'
        attrs = {k: v for k, v in self.__dict__.items() if k != 'id'}
        
        # Convertir objetos complejos a JSON
        for key, value in attrs.items():
            if isinstance(value, (dict, list)):
                attrs[key] = json.dumps(value)
        
        columns = list(attrs.keys())
        values = list(attrs.values())
        placeholders = ', '.join(['%s'] * len(values))
        columns_str = ', '.join(columns)
        
        query = f"INSERT INTO {self.table_name} ({columns_str}) VALUES ({placeholders}) RETURNING id"
        cursor.execute(query, values)
        
        # Obtener el ID generado
        self.id = cursor.fetchone()[0]
    
    def _update(self, cursor):
        """Actualizar registro existente"""
        # Obtener atributos del objeto excluyendo 'id'
        attrs = {k: v for k, v in self.__dict__.items() if k != 'id'}
        
        # Convertir objetos complejos a JSON
        for key, value in attrs.items():
            if isinstance(value, (dict, list)):
                attrs[key] = json.dumps(value)
        
        set_clause = ', '.join([f"{k} = %s" for k in attrs.keys()])
        values = list(attrs.values()) + [self.id]
        
        query = f"UPDATE {self.table_name} SET {set_clause} WHERE id = %s"
        cursor.execute(query, values)
    
    def to_dict(self):
        """Convertir el modelo a diccionario"""
        result = {}
        for key, value in self.__dict__.items():
            if isinstance(value, datetime):
                result[key] = value.isoformat()
            elif isinstance(value, (dict, list)):
                result[key] = value
            else:
                result[key] = value
        return result
