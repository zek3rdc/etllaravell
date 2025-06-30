"""
Módulo de validación de datos para ETL
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional
from datetime import datetime
import re
from models.base import BaseModel


class DataValidator:
    """Validador de calidad de datos"""
    
    def __init__(self, session_id: str):
        self.session_id = session_id
        self.validations = []
    
    def validate_dataframe(self, df: pd.DataFrame, column_mapping: Dict = None) -> Dict:
        """Validar un DataFrame completo"""
        validation_results = {
            "session_id": self.session_id,
            "total_rows": len(df),
            "total_columns": len(df.columns),
            "validations": [],
            "summary": {
                "errors": 0,
                "warnings": 0,
                "info": 0
            },
            "recommendations": []
        }
        
        # Validaciones por columna
        for column in df.columns:
            column_validations = self._validate_column(df, column)
            validation_results["validations"].extend(column_validations)
        
        # Validaciones generales del DataFrame
        general_validations = self._validate_general(df)
        validation_results["validations"].extend(general_validations)
        
        # Contar severidades
        for validation in validation_results["validations"]:
            severity = validation.get("severity", "info")
            if severity in validation_results["summary"]:
                validation_results["summary"][severity] += 1
        
        # Generar recomendaciones
        validation_results["recommendations"] = self._generate_recommendations(validation_results["validations"])
        
        # Guardar validaciones en base de datos
        self._save_validations(validation_results["validations"])
        
        return validation_results
    
    def _validate_column(self, df: pd.DataFrame, column: str) -> List[Dict]:
        """Validar una columna específica"""
        validations = []
        series = df[column]
        
        # Validación de valores nulos
        null_count = series.isnull().sum()
        null_percentage = (null_count / len(series)) * 100
        
        validations.append({
            "column_name": column,
            "validation_type": "null_count",
            "validation_result": {
                "null_count": int(null_count),
                "null_percentage": round(null_percentage, 2),
                "total_rows": len(series)
            },
            "severity": "error" if null_percentage > 50 else "warning" if null_percentage > 10 else "info",
            "message": f"Columna '{column}' tiene {null_count} valores nulos ({null_percentage:.1f}%)"
        })
        
        # Validación de valores únicos/duplicados
        unique_count = series.nunique()
        duplicate_count = len(series) - unique_count
        duplicate_percentage = (duplicate_count / len(series)) * 100
        
        validations.append({
            "column_name": column,
            "validation_type": "duplicate_count",
            "validation_result": {
                "unique_count": int(unique_count),
                "duplicate_count": int(duplicate_count),
                "duplicate_percentage": round(duplicate_percentage, 2),
                "total_rows": len(series)
            },
            "severity": "warning" if duplicate_percentage > 80 else "info",
            "message": f"Columna '{column}' tiene {unique_count} valores únicos, {duplicate_count} duplicados"
        })
        
        # Validación de tipo de datos
        inferred_type = self._infer_data_type(series)
        validations.append({
            "column_name": column,
            "validation_type": "data_type",
            "validation_result": {
                "inferred_type": inferred_type,
                "pandas_dtype": str(series.dtype),
                "sample_values": series.dropna().head(5).tolist()
            },
            "severity": "info",
            "message": f"Columna '{column}' parece ser de tipo '{inferred_type}'"
        })
        
        # Validaciones específicas por tipo
        if inferred_type == "numeric":
            validations.extend(self._validate_numeric_column(series, column))
        elif inferred_type == "date":
            validations.extend(self._validate_date_column(series, column))
        elif inferred_type == "text":
            validations.extend(self._validate_text_column(series, column))
        elif inferred_type == "email":
            validations.extend(self._validate_email_column(series, column))
        
        return validations
    
    def _validate_general(self, df: pd.DataFrame) -> List[Dict]:
        """Validaciones generales del DataFrame"""
        validations = []
        
        # Validación de filas completamente vacías
        empty_rows = df.isnull().all(axis=1).sum()
        if empty_rows > 0:
            validations.append({
                "column_name": "ALL",
                "validation_type": "empty_rows",
                "validation_result": {
                    "empty_rows": int(empty_rows),
                    "percentage": round((empty_rows / len(df)) * 100, 2)
                },
                "severity": "warning",
                "message": f"Se encontraron {empty_rows} filas completamente vacías"
            })
        
        # Validación de filas duplicadas
        duplicate_rows = df.duplicated().sum()
        if duplicate_rows > 0:
            validations.append({
                "column_name": "ALL",
                "validation_type": "duplicate_rows",
                "validation_result": {
                    "duplicate_rows": int(duplicate_rows),
                    "percentage": round((duplicate_rows / len(df)) * 100, 2)
                },
                "severity": "warning",
                "message": f"Se encontraron {duplicate_rows} filas completamente duplicadas"
            })
        
        # Validación de consistencia de columnas
        column_consistency = self._check_column_consistency(df)
        if column_consistency:
            validations.append(column_consistency)
        
        return validations
    
    def _validate_numeric_column(self, series: pd.Series, column: str) -> List[Dict]:
        """Validaciones específicas para columnas numéricas"""
        validations = []
        
        # Convertir a numérico para análisis
        numeric_series = pd.to_numeric(series, errors='coerce')
        conversion_errors = numeric_series.isnull().sum() - series.isnull().sum()
        
        if conversion_errors > 0:
            validations.append({
                "column_name": column,
                "validation_type": "numeric_conversion_errors",
                "validation_result": {
                    "conversion_errors": int(conversion_errors),
                    "percentage": round((conversion_errors / len(series)) * 100, 2)
                },
                "severity": "error",
                "message": f"Columna '{column}' tiene {conversion_errors} valores que no se pueden convertir a número"
            })
        
        # Estadísticas numéricas
        if not numeric_series.dropna().empty:
            stats = {
                "min": float(numeric_series.min()),
                "max": float(numeric_series.max()),
                "mean": float(numeric_series.mean()),
                "median": float(numeric_series.median()),
                "std": float(numeric_series.std())
            }
            
            # Detectar outliers
            Q1 = numeric_series.quantile(0.25)
            Q3 = numeric_series.quantile(0.75)
            IQR = Q3 - Q1
            outliers = numeric_series[(numeric_series < (Q1 - 1.5 * IQR)) | (numeric_series > (Q3 + 1.5 * IQR))]
            
            validations.append({
                "column_name": column,
                "validation_type": "numeric_statistics",
                "validation_result": {
                    "statistics": stats,
                    "outliers_count": len(outliers),
                    "outliers_percentage": round((len(outliers) / len(numeric_series.dropna())) * 100, 2)
                },
                "severity": "warning" if len(outliers) > len(numeric_series) * 0.1 else "info",
                "message": f"Columna '{column}': {len(outliers)} valores atípicos detectados"
            })
        
        return validations
    
    def _validate_date_column(self, series: pd.Series, column: str) -> List[Dict]:
        """Validaciones específicas para columnas de fecha"""
        validations = []
        
        # Intentar convertir a fecha
        date_series = pd.to_datetime(series, errors='coerce', infer_datetime_format=True)
        conversion_errors = date_series.isnull().sum() - series.isnull().sum()
        
        if conversion_errors > 0:
            validations.append({
                "column_name": column,
                "validation_type": "date_conversion_errors",
                "validation_result": {
                    "conversion_errors": int(conversion_errors),
                    "percentage": round((conversion_errors / len(series)) * 100, 2)
                },
                "severity": "error",
                "message": f"Columna '{column}' tiene {conversion_errors} valores que no se pueden convertir a fecha"
            })
        
        # Rango de fechas
        if not date_series.dropna().empty:
            min_date = date_series.min()
            max_date = date_series.max()
            
            # Detectar fechas futuras
            future_dates = date_series[date_series > datetime.now()].count()
            
            # Detectar fechas muy antiguas (antes de 1900)
            old_dates = date_series[date_series < datetime(1900, 1, 1)].count()
            
            validations.append({
                "column_name": column,
                "validation_type": "date_range",
                "validation_result": {
                    "min_date": min_date.isoformat() if pd.notna(min_date) else None,
                    "max_date": max_date.isoformat() if pd.notna(max_date) else None,
                    "future_dates": int(future_dates),
                    "old_dates": int(old_dates)
                },
                "severity": "warning" if future_dates > 0 or old_dates > 0 else "info",
                "message": f"Columna '{column}': rango de {min_date.date()} a {max_date.date()}"
            })
        
        return validations
    
    def _validate_text_column(self, series: pd.Series, column: str) -> List[Dict]:
        """Validaciones específicas para columnas de texto"""
        validations = []
        
        # Longitud de texto
        text_lengths = series.astype(str).str.len()
        
        validations.append({
            "column_name": column,
            "validation_type": "text_length",
            "validation_result": {
                "min_length": int(text_lengths.min()),
                "max_length": int(text_lengths.max()),
                "avg_length": round(text_lengths.mean(), 2),
                "empty_strings": int((series == "").sum())
            },
            "severity": "info",
            "message": f"Columna '{column}': longitud promedio {text_lengths.mean():.1f} caracteres"
        })
        
        # Patrones comunes
        patterns = {
            "only_numbers": r'^\d+$',
            "contains_special_chars": r'[!@#$%^&*(),.?":{}|<>]',
            "all_uppercase": r'^[A-Z\s]+$',
            "all_lowercase": r'^[a-z\s]+$'
        }
        
        pattern_results = {}
        for pattern_name, pattern in patterns.items():
            matches = series.astype(str).str.contains(pattern, regex=True, na=False).sum()
            pattern_results[pattern_name] = int(matches)
        
        validations.append({
            "column_name": column,
            "validation_type": "text_patterns",
            "validation_result": pattern_results,
            "severity": "info",
            "message": f"Columna '{column}': análisis de patrones de texto completado"
        })
        
        return validations
    
    def _validate_email_column(self, series: pd.Series, column: str) -> List[Dict]:
        """Validaciones específicas para columnas de email"""
        validations = []
        
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        valid_emails = series.astype(str).str.contains(email_pattern, regex=True, na=False).sum()
        invalid_emails = len(series.dropna()) - valid_emails
        
        validations.append({
            "column_name": column,
            "validation_type": "email_format",
            "validation_result": {
                "valid_emails": int(valid_emails),
                "invalid_emails": int(invalid_emails),
                "validity_percentage": round((valid_emails / len(series.dropna())) * 100, 2) if len(series.dropna()) > 0 else 0
            },
            "severity": "error" if invalid_emails > valid_emails else "warning" if invalid_emails > 0 else "info",
            "message": f"Columna '{column}': {valid_emails} emails válidos, {invalid_emails} inválidos"
        })
        
        return validations
    
    def _infer_data_type(self, series: pd.Series) -> str:
        """Inferir el tipo de datos de una serie"""
        # Eliminar valores nulos para análisis
        clean_series = series.dropna()
        
        if clean_series.empty:
            return "empty"
        
        # Convertir a string para análisis de patrones
        str_series = clean_series.astype(str)
        
        # Patrones para diferentes tipos
        patterns = {
            "email": r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
            "date": r'^\d{1,2}[/-]\d{1,2}[/-]\d{2,4}$|^\d{4}[/-]\d{1,2}[/-]\d{1,2}$',
            "numeric": r'^-?\d+\.?\d*$',
            "phone": r'^[\+]?[1-9][\d]{0,15}$',
            "url": r'^https?://',
            "boolean": r'^(true|false|yes|no|si|no|1|0)$'
        }
        
        # Verificar patrones
        for data_type, pattern in patterns.items():
            matches = str_series.str.contains(pattern, regex=True, case=False).sum()
            if matches / len(str_series) > 0.8:  # 80% de coincidencia
                return data_type
        
        # Intentar conversión numérica
        try:
            pd.to_numeric(clean_series)
            return "numeric"
        except:
            pass
        
        # Intentar conversión de fecha
        try:
            pd.to_datetime(clean_series, infer_datetime_format=True)
            return "date"
        except:
            pass
        
        return "text"
    
    def _check_column_consistency(self, df: pd.DataFrame) -> Optional[Dict]:
        """Verificar consistencia entre columnas relacionadas"""
        # Esta función puede expandirse para verificar relaciones específicas
        # Por ejemplo, verificar que fecha_inicio < fecha_fin
        
        # Buscar columnas que podrían estar relacionadas
        date_columns = []
        for col in df.columns:
            if any(keyword in col.lower() for keyword in ['fecha', 'date', 'inicio', 'fin', 'start', 'end']):
                date_columns.append(col)
        
        if len(date_columns) >= 2:
            return {
                "column_name": "MULTIPLE",
                "validation_type": "column_consistency",
                "validation_result": {
                    "related_columns": date_columns,
                    "suggestion": "Verificar relaciones entre columnas de fecha"
                },
                "severity": "info",
                "message": f"Se detectaron columnas relacionadas: {', '.join(date_columns)}"
            }
        
        return None
    
    def _generate_recommendations(self, validations: List[Dict]) -> List[str]:
        """Generar recomendaciones basadas en las validaciones"""
        recommendations = []
        
        # Contar tipos de problemas
        high_null_columns = []
        conversion_error_columns = []
        outlier_columns = []
        
        for validation in validations:
            if validation["validation_type"] == "null_count" and validation["severity"] in ["error", "warning"]:
                high_null_columns.append(validation["column_name"])
            elif "conversion_errors" in validation["validation_type"] and validation["severity"] == "error":
                conversion_error_columns.append(validation["column_name"])
            elif validation["validation_type"] == "numeric_statistics" and validation["validation_result"].get("outliers_count", 0) > 0:
                outlier_columns.append(validation["column_name"])
        
        # Generar recomendaciones específicas
        if high_null_columns:
            recommendations.append(f"Considere limpiar o imputar valores nulos en: {', '.join(high_null_columns)}")
        
        if conversion_error_columns:
            recommendations.append(f"Revise y corrija errores de formato en: {', '.join(conversion_error_columns)}")
        
        if outlier_columns:
            recommendations.append(f"Analice valores atípicos en: {', '.join(outlier_columns)}")
        
        # Recomendaciones generales
        total_errors = sum(1 for v in validations if v["severity"] == "error")
        total_warnings = sum(1 for v in validations if v["severity"] == "warning")
        
        if total_errors > 0:
            recommendations.append(f"Se encontraron {total_errors} errores críticos que deben corregirse antes de procesar")
        
        if total_warnings > 5:
            recommendations.append("Considere revisar las advertencias antes de continuar con el procesamiento")
        
        if not recommendations:
            recommendations.append("Los datos parecen estar en buenas condiciones para el procesamiento")
        
        return recommendations
    
    def _save_validations(self, validations: List[Dict]):
        """Guardar validaciones en la base de datos"""
        from models.validation import ETLDataValidation
        
        for validation in validations:
            validation_record = ETLDataValidation(
                session_id=self.session_id,
                column_name=validation["column_name"],
                validation_type=validation["validation_type"],
                validation_result=validation["validation_result"],
                severity=validation["severity"]
            )
            validation_record.save()
    
    @classmethod
    def get_validation_history(cls, session_id: str) -> List[Dict]:
        """Obtener historial de validaciones para una sesión"""
        from models.validation import ETLDataValidation
        
        validations = ETLDataValidation.find_all("session_id = %s", (session_id,))
        return [v.to_dict() for v in validations]
