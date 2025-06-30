"""
Módulo de transformaciones de datos para ETL
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Callable
from datetime import datetime
import re
import ast
import sys
from io import StringIO
import logging


class DataTransformer:
    """Transformador de datos con soporte para transformaciones personalizadas"""
    
    def __init__(self, session_id: str):
        self.session_id = session_id
        self.custom_transformations = {}
        self.transformation_history = []
        self.logger = logging.getLogger(__name__)
    
    def apply_transformations(self, df: pd.DataFrame, transformations: Dict) -> pd.DataFrame:
        """Aplicar todas las transformaciones al DataFrame"""
        df_transformed = df.copy()
        
        for column, transform_config in transformations.items():
            if column not in df_transformed.columns:
                self.logger.warning(f"Columna '{column}' no encontrada en el DataFrame")
                continue
            
            try:
                df_transformed[column] = self._apply_single_transformation(
                    df_transformed[column], 
                    transform_config
                )
                
                # Registrar transformación aplicada
                self.transformation_history.append({
                    "column": column,
                    "transformation": transform_config,
                    "timestamp": datetime.now().isoformat(),
                    "status": "success"
                })
                
            except Exception as e:
                self.logger.error(f"Error transformando columna '{column}': {e}")
                self.transformation_history.append({
                    "column": column,
                    "transformation": transform_config,
                    "timestamp": datetime.now().isoformat(),
                    "status": "error",
                    "error": str(e)
                })
        
        return df_transformed
    
    def get_custom_transformation_by_name(self, name: str):
        """Obtener transformación personalizada por nombre"""
        from models.transformation import ETLCustomTransformation
        transformations = ETLCustomTransformation.find_all("name = %s", (name,))
        return transformations[0] if transformations else None
    
    def _apply_single_transformation(self, series: pd.Series, transform_config: Dict) -> pd.Series:
        """Aplicar una transformación específica a una serie"""
        transform_type = transform_config.get('type')
        options = transform_config.get('options', {})
        
        if transform_type == 'date':
            return self._transform_date_column(series, options)
        elif transform_type == 'number':
            return self._transform_number_column(series, options)
        elif transform_type == 'text':
            return self._transform_text_column(series, options)
        elif transform_type == 'replace':
            return self._transform_replace_column(series, options)
        elif transform_type == 'custom':
            return self._transform_custom_column(series, options)
        elif transform_type == 'conditional':
            return self._transform_conditional_column(series, options)
        elif transform_type == 'regex':
            return self._transform_regex_column(series, options)
        elif transform_type == 'mathematical':
            return self._transform_mathematical_column(series, options)
        else:
            self.logger.warning(f"Tipo de transformación desconocido: {transform_type}")
            return series
    
    def _transform_date_column(self, series: pd.Series, options: Dict) -> pd.Series:
        """Transformar columna de fechas"""
        date_format_from = options.get('date_format_from', 'auto')
        date_format_to = options.get('date_format_to', '%Y-%m-%d')
        handle_errors = options.get('handle_errors', 'coerce')  # coerce, raise, ignore
        
        try:
            if date_format_from == 'auto':
                # Intentar detectar formato automáticamente
                parsed_dates = pd.to_datetime(series, infer_datetime_format=True, errors=handle_errors)
            else:
                parsed_dates = pd.to_datetime(series, format=date_format_from, errors=handle_errors)
            
            # Formatear según el formato de salida
            if date_format_to == 'timestamp':
                return parsed_dates.astype('int64') // 10**9  # Unix timestamp
            elif date_format_to == 'iso':
                return parsed_dates.dt.strftime('%Y-%m-%dT%H:%M:%S')
            else:
                return parsed_dates.dt.strftime(date_format_to)
                
        except Exception as e:
            self.logger.error(f"Error en transformación de fecha: {e}")
            return series
    
    def _transform_number_column(self, series: pd.Series, options: Dict) -> pd.Series:
        """Transformar columna numérica"""
        decimal_separator = options.get('decimal_separator', '.')
        thousands_separator = options.get('thousands_separator', '')
        round_decimals = options.get('round_decimals', None)
        fill_na = options.get('fill_na', None)
        
        # Limpiar la serie
        cleaned_series = series.astype(str)
        
        # Reemplazar separadores
        if thousands_separator:
            cleaned_series = cleaned_series.str.replace(thousands_separator, '')
        
        if decimal_separator != '.':
            cleaned_series = cleaned_series.str.replace(decimal_separator, '.')
        
        # Convertir a numérico
        numeric_series = pd.to_numeric(cleaned_series, errors='coerce')
        
        # Rellenar valores nulos si se especifica
        if fill_na is not None:
            numeric_series = numeric_series.fillna(fill_na)
        
        # Redondear si se especifica
        if round_decimals is not None:
            numeric_series = numeric_series.round(round_decimals)
        
        return numeric_series
    
    def _transform_text_column(self, series: pd.Series, options: Dict) -> pd.Series:
        """Transformar columna de texto"""
        text_transform = options.get('text_transform', 'none')
        remove_accents = options.get('remove_accents', False)
        remove_extra_spaces = options.get('remove_extra_spaces', True)
        
        text_series = series.astype(str)
        
        # Aplicar transformación de caso
        if text_transform == 'upper':
            text_series = text_series.str.upper()
        elif text_transform == 'lower':
            text_series = text_series.str.lower()
        elif text_transform == 'title':
            text_series = text_series.str.title()
        elif text_transform == 'capitalize':
            text_series = text_series.str.capitalize()
        elif text_transform == 'trim':
            text_series = text_series.str.strip()
        
        # Remover acentos si se especifica
        if remove_accents:
            text_series = self._remove_accents(text_series)
        
        # Remover espacios extra
        if remove_extra_spaces:
            text_series = text_series.str.replace(r'\s+', ' ', regex=True).str.strip()
        
        return text_series
    
    def _transform_replace_column(self, series: pd.Series, options: Dict) -> pd.Series:
        """Reemplazar valores en columna"""
        replace_from = options.get('replace_from', '')
        replace_to = options.get('replace_to', '')
        use_regex = options.get('use_regex', False)
        case_sensitive = options.get('case_sensitive', True)
        
        if not replace_from:
            return series
        
        text_series = series.astype(str)
        
        if use_regex:
            flags = 0 if case_sensitive else re.IGNORECASE
            return text_series.str.replace(replace_from, replace_to, regex=True, flags=flags)
        else:
            if case_sensitive:
                return text_series.str.replace(replace_from, replace_to)
            else:
                # Para reemplazo sin regex y sin case sensitive
                pattern = re.escape(replace_from)
                return text_series.str.replace(pattern, replace_to, regex=True, flags=re.IGNORECASE)
    
    def _transform_custom_column(self, series: pd.Series, options: Dict) -> pd.Series:
        """Aplicar transformación personalizada con código Python"""
        custom_code = options.get('code', '')
        function_name = options.get('function_name', 'transform')
        parameters = options.get('parameters', {})
        
        if not custom_code:
            self.logger.warning("No se proporcionó código personalizado")
            return series
        
        try:
            # Crear un entorno seguro para ejecutar el código
            safe_globals = {
                '__builtins__': {
                    'len': len,
                    'str': str,
                    'int': int,
                    'float': float,
                    'bool': bool,
                    'list': list,
                    'dict': dict,
                    'tuple': tuple,
                    'set': set,
                    'min': min,
                    'max': max,
                    'sum': sum,
                    'abs': abs,
                    'round': round,
                    'range': range,
                    'enumerate': enumerate,
                    'zip': zip,
                    'map': map,
                    'filter': filter,
                    'sorted': sorted,
                    'reversed': reversed
                },
                'pd': pd,
                'np': np,
                're': re,
                'datetime': datetime,
                'parameters': parameters
            }
            
            # Ejecutar el código personalizado
            exec(custom_code, safe_globals)
            
            # Obtener la función de transformación
            if function_name in safe_globals:
                transform_function = safe_globals[function_name]
                
                # Aplicar la función a cada valor de la serie
                if callable(transform_function):
                    return series.apply(transform_function)
                else:
                    self.logger.error(f"'{function_name}' no es una función")
                    return series
            else:
                self.logger.error(f"Función '{function_name}' no encontrada en el código personalizado")
                return series
                
        except Exception as e:
            self.logger.error(f"Error ejecutando código personalizado: {e}")
            return series
    
    def _transform_conditional_column(self, series: pd.Series, options: Dict) -> pd.Series:
        """Aplicar transformación condicional"""
        conditions = options.get('conditions', [])
        default_value = options.get('default_value', None)
        
        result = series.copy()
        
        for condition in conditions:
            condition_expr = condition.get('condition', '')
            value = condition.get('value', '')
            
            try:
                # Evaluar condición de forma segura
                mask = self._evaluate_condition(series, condition_expr)
                result.loc[mask] = value
            except Exception as e:
                self.logger.error(f"Error en condición '{condition_expr}': {e}")
        
        # Aplicar valor por defecto si se especifica
        if default_value is not None:
            result = result.fillna(default_value)
        
        return result
    
    def _transform_regex_column(self, series: pd.Series, options: Dict) -> pd.Series:
        """Aplicar transformación con expresiones regulares"""
        pattern = options.get('pattern', '')
        replacement = options.get('replacement', '')
        extract_group = options.get('extract_group', None)
        flags = options.get('flags', 0)
        
        if not pattern:
            return series
        
        text_series = series.astype(str)
        
        try:
            if extract_group is not None:
                # Extraer grupo específico
                return text_series.str.extract(pattern, flags=flags)[extract_group]
            else:
                # Reemplazar patrón
                return text_series.str.replace(pattern, replacement, regex=True, flags=flags)
        except Exception as e:
            self.logger.error(f"Error en transformación regex: {e}")
            return series
    
    def _transform_mathematical_column(self, series: pd.Series, options: Dict) -> pd.Series:
        """Aplicar transformación matemática"""
        operation = options.get('operation', 'none')
        operand = options.get('operand', 0)
        
        try:
            numeric_series = pd.to_numeric(series, errors='coerce')
            
            if operation == 'add':
                return numeric_series + operand
            elif operation == 'subtract':
                return numeric_series - operand
            elif operation == 'multiply':
                return numeric_series * operand
            elif operation == 'divide':
                return numeric_series / operand if operand != 0 else numeric_series
            elif operation == 'power':
                return numeric_series ** operand
            elif operation == 'sqrt':
                return np.sqrt(numeric_series)
            elif operation == 'log':
                return np.log(numeric_series)
            elif operation == 'log10':
                return np.log10(numeric_series)
            elif operation == 'abs':
                return np.abs(numeric_series)
            elif operation == 'round':
                return np.round(numeric_series, int(operand))
            else:
                self.logger.warning(f"Operación matemática desconocida: {operation}")
                return series
                
        except Exception as e:
            self.logger.error(f"Error en transformación matemática: {e}")
            return series
    
    def _remove_accents(self, series: pd.Series) -> pd.Series:
        """Remover acentos de texto"""
        accent_map = {
            'á': 'a', 'à': 'a', 'ä': 'a', 'â': 'a', 'ā': 'a', 'ã': 'a',
            'é': 'e', 'è': 'e', 'ë': 'e', 'ê': 'e', 'ē': 'e',
            'í': 'i', 'ì': 'i', 'ï': 'i', 'î': 'i', 'ī': 'i',
            'ó': 'o', 'ò': 'o', 'ö': 'o', 'ô': 'o', 'ō': 'o', 'õ': 'o',
            'ú': 'u', 'ù': 'u', 'ü': 'u', 'û': 'u', 'ū': 'u',
            'ñ': 'n', 'ç': 'c',
            'Á': 'A', 'À': 'A', 'Ä': 'A', 'Â': 'A', 'Ā': 'A', 'Ã': 'A',
            'É': 'E', 'È': 'E', 'Ë': 'E', 'Ê': 'E', 'Ē': 'E',
            'Í': 'I', 'Ì': 'I', 'Ï': 'I', 'Î': 'I', 'Ī': 'I',
            'Ó': 'O', 'Ò': 'O', 'Ö': 'O', 'Ô': 'O', 'Ō': 'O', 'Õ': 'O',
            'Ú': 'U', 'Ù': 'U', 'Ü': 'U', 'Û': 'U', 'Ū': 'U',
            'Ñ': 'N', 'Ç': 'C'
        }
        
        result = series.copy()
        for accented, unaccented in accent_map.items():
            result = result.str.replace(accented, unaccented)
        
        return result
    
    def _evaluate_condition(self, series: pd.Series, condition_expr: str) -> pd.Series:
        """Evaluar condición de forma segura"""
        # Reemplazar 'value' con la serie actual en la expresión
        safe_condition = condition_expr.replace('value', 'series')
        
        # Entorno seguro para evaluación
        safe_locals = {
            'series': series,
            'pd': pd,
            'np': np,
            'len': len,
            'str': str,
            'int': int,
            'float': float,
            'bool': bool
        }
        
        try:
            return eval(safe_condition, {"__builtins__": {}}, safe_locals)
        except Exception as e:
            self.logger.error(f"Error evaluando condición '{condition_expr}': {e}")
            return pd.Series([False] * len(series), index=series.index)
    
    def load_custom_transformation(self, name: str, code: str, description: str = "", parameters: Dict = None):
        """Cargar una transformación personalizada"""
        from models.transformation import ETLCustomTransformation
        
        # Validar el código antes de guardarlo
        if self._validate_custom_code(code):
            transformation = ETLCustomTransformation(
                name=name,
                description=description,
                python_code=code,
                parameters=parameters or {},
                created_by=f"session_{self.session_id}"
            )
            transformation.save()
            
            # Cargar en memoria para uso inmediato
            self.custom_transformations[name] = {
                'python_code': code,
                'description': description,
                'parameters': parameters or {}
            }
            
            return True
        else:
            return False
    
    def _validate_custom_code(self, code: str) -> bool:
        """Validar código personalizado por seguridad"""
        # Lista de palabras/módulos prohibidos por seguridad
        forbidden_keywords = [
            'import os', 'import sys', 'import subprocess', 'import socket',
            'import urllib', 'import requests', 'import http', 'import ftplib',
            'import smtplib', 'import telnetlib', 'import pickle', 'import marshal',
            'import ctypes', 'import threading', 'import multiprocessing',
            'exec(', 'eval(', '__import__', 'open(', 'file(', 'input(',
            'raw_input(', 'compile(', 'globals(', 'locals(', 'vars(',
            'dir(', 'hasattr(', 'getattr(', 'setattr(', 'delattr('
        ]
        
        code_lower = code.lower()
        for keyword in forbidden_keywords:
            if keyword in code_lower:
                self.logger.error(f"Código contiene palabra prohibida: {keyword}")
                return False
        
        # Intentar compilar el código para verificar sintaxis
        try:
            compile(code, '<string>', 'exec')
            return True
        except SyntaxError as e:
            self.logger.error(f"Error de sintaxis en código personalizado: {e}")
            return False
    
    def get_transformation_history(self) -> List[Dict]:
        """Obtener historial de transformaciones aplicadas"""
        return self.transformation_history
    
    def get_available_transformations(self) -> Dict:
        """Obtener lista de transformaciones disponibles"""
        return {
            "built_in": {
                "date": {
                    "description": "Transformaciones de fecha y hora",
                    "options": ["date_format_from", "date_format_to", "handle_errors"]
                },
                "number": {
                    "description": "Transformaciones numéricas",
                    "options": ["decimal_separator", "thousands_separator", "round_decimals", "fill_na"]
                },
                "text": {
                    "description": "Transformaciones de texto",
                    "options": ["text_transform", "remove_accents", "remove_extra_spaces"]
                },
                "replace": {
                    "description": "Reemplazo de valores",
                    "options": ["replace_from", "replace_to", "use_regex", "case_sensitive"]
                },
                "conditional": {
                    "description": "Transformaciones condicionales",
                    "options": ["conditions", "default_value"]
                },
                "regex": {
                    "description": "Transformaciones con expresiones regulares",
                    "options": ["pattern", "replacement", "extract_group", "flags"]
                },
                "mathematical": {
                    "description": "Operaciones matemáticas",
                    "options": ["operation", "operand"]
                }
            },
            "custom": self.custom_transformations
        }
