"""
Módulo de notificaciones para ETL
"""

import requests
import json
from typing import Dict, List, Any, Optional
from datetime import datetime
import logging
from models.notification import ETLNotificationConfig, ETLNotificationLog

# Importaciones opcionales para email
try:
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MimeMultipart
    EMAIL_AVAILABLE = True
except ImportError:
    EMAIL_AVAILABLE = False


class NotificationManager:
    """Gestor de notificaciones para eventos ETL"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.notification_configs = self._load_active_configs()
    
    def _load_active_configs(self) -> List[ETLNotificationConfig]:
        """Cargar configuraciones de notificación activas"""
        return ETLNotificationConfig.find_all("is_active = true")
    
    def send_notifications(self, event_type: str, context: Dict):
        """Enviar notificaciones para un evento específico"""
        for config in self.notification_configs:
            if self._should_notify(config, event_type):
                try:
                    success = self._send_notification(config, event_type, context)
                    self._log_notification(config.id, context.get('load_history_id'), event_type, 
                                         'sent' if success else 'failed', context)
                except Exception as e:
                    self.logger.error(f"Error enviando notificación {config.name}: {e}")
                    self._log_notification(config.id, context.get('load_history_id'), event_type, 
                                         'failed', context, str(e))
    
    def _should_notify(self, config: ETLNotificationConfig, event_type: str) -> bool:
        """Verificar si se debe enviar notificación para este evento"""
        events = config.events
        
        # Verificar si el evento está en la lista de eventos configurados
        if event_type in events.get('types', []):
            return True
        
        # Verificar condiciones específicas
        conditions = events.get('conditions', {})
        
        # Por ejemplo, solo notificar si hay errores
        if event_type == 'load_completed' and conditions.get('only_on_errors', False):
            return context.get('error_count', 0) > 0
        
        # Solo notificar si la tasa de éxito es baja
        if event_type == 'load_completed' and 'min_success_rate' in conditions:
            success_rate = context.get('success_rate', 100)
            return success_rate < conditions['min_success_rate']
        
        return False
    
    def _send_notification(self, config: ETLNotificationConfig, event_type: str, context: Dict) -> bool:
        """Enviar notificación según el tipo configurado"""
        notification_type = config.type
        
        if notification_type == 'email':
            return self._send_email_notification(config, event_type, context)
        elif notification_type == 'slack':
            return self._send_slack_notification(config, event_type, context)
        elif notification_type == 'telegram':
            return self._send_telegram_notification(config, event_type, context)
        elif notification_type == 'webhook':
            return self._send_webhook_notification(config, event_type, context)
        else:
            self.logger.error(f"Tipo de notificación no soportado: {notification_type}")
            return False
    
    def _send_email_notification(self, config: ETLNotificationConfig, event_type: str, context: Dict) -> bool:
        """Enviar notificación por email"""
        if not EMAIL_AVAILABLE:
            self.logger.warning("Funcionalidad de email no disponible - módulos de email no importados")
            return False
            
        try:
            email_config = config.config
            
            # Configurar servidor SMTP
            server = smtplib.SMTP(email_config['smtp_server'], email_config.get('smtp_port', 587))
            server.starttls()
            server.login(email_config['username'], email_config['password'])
            
            # Crear mensaje
            msg = MimeMultipart()
            msg['From'] = email_config['from_email']
            msg['To'] = ', '.join(email_config['to_emails'])
            msg['Subject'] = self._generate_email_subject(event_type, context)
            
            # Cuerpo del mensaje
            body = self._generate_email_body(event_type, context)
            msg.attach(MIMEText(body, 'html'))
            
            # Enviar
            server.send_message(msg)
            server.quit()
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error enviando email: {e}")
            return False
    
    def _send_slack_notification(self, config: ETLNotificationConfig, event_type: str, context: Dict) -> bool:
        """Enviar notificación a Slack"""
        try:
            slack_config = config.config
            webhook_url = slack_config['webhook_url']
            
            # Crear mensaje para Slack
            message = self._generate_slack_message(event_type, context)
            
            payload = {
                "text": message['text'],
                "attachments": message.get('attachments', [])
            }
            
            response = requests.post(webhook_url, json=payload, timeout=10)
            return response.status_code == 200
            
        except Exception as e:
            self.logger.error(f"Error enviando mensaje a Slack: {e}")
            return False
    
    def _send_telegram_notification(self, config: ETLNotificationConfig, event_type: str, context: Dict) -> bool:
        """Enviar notificación a Telegram"""
        try:
            telegram_config = config.config
            bot_token = telegram_config['bot_token']
            chat_id = telegram_config['chat_id']
            
            # Crear mensaje
            message = self._generate_telegram_message(event_type, context)
            
            url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
            payload = {
                "chat_id": chat_id,
                "text": message,
                "parse_mode": "HTML"
            }
            
            response = requests.post(url, json=payload, timeout=10)
            return response.status_code == 200
            
        except Exception as e:
            self.logger.error(f"Error enviando mensaje a Telegram: {e}")
            return False
    
    def _send_webhook_notification(self, config: ETLNotificationConfig, event_type: str, context: Dict) -> bool:
        """Enviar notificación via webhook"""
        try:
            webhook_config = config.config
            url = webhook_config['url']
            method = webhook_config.get('method', 'POST').upper()
            headers = webhook_config.get('headers', {})
            
            # Preparar payload
            payload = {
                "event_type": event_type,
                "timestamp": datetime.now().isoformat(),
                "context": context
            }
            
            if method == 'POST':
                response = requests.post(url, json=payload, headers=headers, timeout=10)
            elif method == 'PUT':
                response = requests.put(url, json=payload, headers=headers, timeout=10)
            else:
                response = requests.get(url, params=payload, headers=headers, timeout=10)
            
            return response.status_code in [200, 201, 202]
            
        except Exception as e:
            self.logger.error(f"Error enviando webhook: {e}")
            return False
    
    def _generate_email_subject(self, event_type: str, context: Dict) -> str:
        """Generar asunto del email"""
        subjects = {
            'load_started': f"ETL: Iniciado procesamiento - {context.get('table', 'N/A')}",
            'load_completed': f"ETL: Completado - {context.get('table', 'N/A')} ({context.get('success_rate', 0)}% éxito)",
            'load_failed': f"ETL: Error - {context.get('table', 'N/A')}",
            'validation_warning': f"ETL: Advertencias de validación - {context.get('table', 'N/A')}",
            'validation_error': f"ETL: Errores de validación - {context.get('table', 'N/A')}"
        }
        
        return subjects.get(event_type, f"ETL: Evento {event_type}")
    
    def _generate_email_body(self, event_type: str, context: Dict) -> str:
        """Generar cuerpo del email en HTML"""
        base_template = """
        <html>
        <body>
            <h2>Notificación ETL</h2>
            <p><strong>Evento:</strong> {event_type}</p>
            <p><strong>Fecha:</strong> {timestamp}</p>
            <p><strong>Tabla:</strong> {table}</p>
            {content}
            <hr>
            <p><small>Sistema ETL - Generado automáticamente</small></p>
        </body>
        </html>
        """
        
        content = ""
        
        if event_type == 'load_completed':
            content = f"""
            <h3>Resumen del Procesamiento</h3>
            <ul>
                <li><strong>Total de filas:</strong> {context.get('total_rows', 0)}</li>
                <li><strong>Filas procesadas:</strong> {context.get('processed_rows', 0)}</li>
                <li><strong>Errores:</strong> {context.get('error_rows', 0)}</li>
                <li><strong>Tasa de éxito:</strong> {context.get('success_rate', 0)}%</li>
                <li><strong>Tiempo de ejecución:</strong> {context.get('execution_time', 0)} segundos</li>
            </ul>
            """
        elif event_type == 'load_failed':
            content = f"""
            <h3>Error en el Procesamiento</h3>
            <p><strong>Mensaje de error:</strong></p>
            <pre>{context.get('error_message', 'Error desconocido')}</pre>
            """
        elif event_type in ['validation_warning', 'validation_error']:
            content = f"""
            <h3>Problemas de Validación</h3>
            <p><strong>Errores:</strong> {context.get('error_count', 0)}</p>
            <p><strong>Advertencias:</strong> {context.get('warning_count', 0)}</p>
            """
        
        return base_template.format(
            event_type=event_type.replace('_', ' ').title(),
            timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            table=context.get('table', 'N/A'),
            content=content
        )
    
    def _generate_slack_message(self, event_type: str, context: Dict) -> Dict:
        """Generar mensaje para Slack"""
        color_map = {
            'load_started': '#36a64f',  # Verde
            'load_completed': '#36a64f' if context.get('success_rate', 0) > 90 else '#ff9500',  # Verde o naranja
            'load_failed': '#ff0000',  # Rojo
            'validation_warning': '#ff9500',  # Naranja
            'validation_error': '#ff0000'  # Rojo
        }
        
        title_map = {
            'load_started': '🚀 ETL Iniciado',
            'load_completed': '✅ ETL Completado',
            'load_failed': '❌ ETL Fallido',
            'validation_warning': '⚠️ Advertencias de Validación',
            'validation_error': '🚫 Errores de Validación'
        }
        
        fields = []
        
        if event_type == 'load_completed':
            fields = [
                {"title": "Total de filas", "value": str(context.get('total_rows', 0)), "short": True},
                {"title": "Procesadas", "value": str(context.get('processed_rows', 0)), "short": True},
                {"title": "Errores", "value": str(context.get('error_rows', 0)), "short": True},
                {"title": "Tasa de éxito", "value": f"{context.get('success_rate', 0)}%", "short": True}
            ]
        
        attachment = {
            "color": color_map.get(event_type, '#36a64f'),
            "title": title_map.get(event_type, event_type),
            "fields": [
                {"title": "Tabla", "value": context.get('table', 'N/A'), "short": True},
                {"title": "Timestamp", "value": datetime.now().strftime('%Y-%m-%d %H:%M:%S'), "short": True}
            ] + fields
        }
        
        return {
            "text": f"Notificación ETL: {title_map.get(event_type, event_type)}",
            "attachments": [attachment]
        }
    
    def _generate_telegram_message(self, event_type: str, context: Dict) -> str:
        """Generar mensaje para Telegram"""
        emoji_map = {
            'load_started': '🚀',
            'load_completed': '✅',
            'load_failed': '❌',
            'validation_warning': '⚠️',
            'validation_error': '🚫'
        }
        
        emoji = emoji_map.get(event_type, '📊')
        title = event_type.replace('_', ' ').title()
        
        message = f"<b>{emoji} ETL: {title}</b>\n\n"
        message += f"<b>Tabla:</b> {context.get('table', 'N/A')}\n"
        message += f"<b>Timestamp:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        
        if event_type == 'load_completed':
            message += f"\n<b>📊 Resumen:</b>\n"
            message += f"• Total: {context.get('total_rows', 0)} filas\n"
            message += f"• Procesadas: {context.get('processed_rows', 0)}\n"
            message += f"• Errores: {context.get('error_rows', 0)}\n"
            message += f"• Éxito: {context.get('success_rate', 0)}%\n"
            message += f"• Tiempo: {context.get('execution_time', 0)}s"
        elif event_type == 'load_failed':
            message += f"\n<b>❌ Error:</b>\n"
            message += f"<code>{context.get('error_message', 'Error desconocido')}</code>"
        
        return message
    
    def _log_notification(self, config_id: int, load_history_id: Optional[int], 
                         event_type: str, status: str, context: Dict, error_message: str = None):
        """Registrar log de notificación"""
        try:
            log = ETLNotificationLog(
                config_id=config_id,
                load_history_id=load_history_id,
                event_type=event_type,
                status=status,
                message=json.dumps(context),
                error_message=error_message
            )
            log.save()
        except Exception as e:
            self.logger.error(f"Error guardando log de notificación: {e}")
    
    @classmethod
    def create_notification_config(cls, name: str, notification_type: str, 
                                 config: Dict, events: Dict) -> ETLNotificationConfig:
        """Crear nueva configuración de notificación"""
        notification_config = ETLNotificationConfig(
            name=name,
            type=notification_type,
            config=config,
            events=events
        )
        return notification_config.save()
    
    @classmethod
    def get_notification_templates(cls) -> Dict:
        """Obtener plantillas de configuración para diferentes tipos de notificación"""
        return {
            "email": {
                "config": {
                    "smtp_server": "smtp.gmail.com",
                    "smtp_port": 587,
                    "username": "tu_email@gmail.com",
                    "password": "tu_contraseña",
                    "from_email": "etl@tuempresa.com",
                    "to_emails": ["admin@tuempresa.com"]
                },
                "events": {
                    "types": ["load_completed", "load_failed"],
                    "conditions": {
                        "only_on_errors": False,
                        "min_success_rate": 95
                    }
                }
            },
            "slack": {
                "config": {
                    "webhook_url": "https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK"
                },
                "events": {
                    "types": ["load_completed", "load_failed", "validation_error"],
                    "conditions": {
                        "only_on_errors": True
                    }
                }
            },
            "telegram": {
                "config": {
                    "bot_token": "YOUR_BOT_TOKEN",
                    "chat_id": "YOUR_CHAT_ID"
                },
                "events": {
                    "types": ["load_failed", "validation_error"],
                    "conditions": {}
                }
            },
            "webhook": {
                "config": {
                    "url": "https://tu-api.com/webhook/etl",
                    "method": "POST",
                    "headers": {
                        "Authorization": "Bearer YOUR_TOKEN",
                        "Content-Type": "application/json"
                    }
                },
                "events": {
                    "types": ["load_started", "load_completed", "load_failed"],
                    "conditions": {}
                }
            }
        }
