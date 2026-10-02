# Correos descriptivos

Los correos muestran nombres de equipo, UG y central, códigos de OT y nombres de materiales, bodegas y usuarios. Las referencias técnicas siguen identificando alertas y controlando envíos internamente. Los UUID no se presentan como etiquetas ni como referencias visibles.

## Familias revisadas

| Correo                     | Disparador                                                                                                          | Destinatarios existentes                                       |
| -------------------------- | ------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------- |
| Alertas operativas         | Mantenimiento por horómetro/tiempo, programación, reporte diario, lubricante, combustible, inventario y ciclo de OT | Resolución de destinatarios por alerta y alcance               |
| Recordatorio de horómetros | Diario a las 08:00, Guayaquil                                                                                       | Supervisores                                                   |
| Resumen de stock           | Diario a las 06:00; nuevas caídas bajo mínimo                                                                       | Personal con acceso a sucursales/bodegas correspondientes      |
| Stock tras carga masiva    | Finalización de importación                                                                                         | Destinatarios de inventario según alcance                      |
| Stock tras movimiento      | Movimiento que deja stock bajo mínimo                                                                               | Destinatarios de inventario según alcance                      |
| Reserva de materiales      | Reserva de una OT, agrupada durante 45 segundos                                                                     | Bodega, administradores y súper administradores según alcance  |
| Salida de materiales       | Aviso manual desde la OT                                                                                            | Solicitantes, administradores y súper administradores          |
| OT en revisión             | Paso de una OT de cebado a revisión                                                                                 | Supervisores                                                   |
| Consumos de OT             | Registro de consumos agrupados                                                                                      | Destinatarios resueltos para la OT                             |
| Solicitud a matriz         | Solicitud manual desde reservas de bodega                                                                           | Administradores, súper administradores y gerente general       |
| Incidente técnico          | Reporte automático de incidente                                                                                     | Administrador configurado para soporte                         |
| Bienvenida                 | Alta de usuario, servicio de seguridad                                                                              | Nuevo usuario; nombre, usuario de acceso y perfil descriptivos |

Esta revisión conserva los disparadores, restricciones de tipo de mantenimiento, destinatarios, alcance e idempotencia existentes. La reprogramación mensual continúa siendo un registro informativo sin correo automático. El servicio de notificaciones registra avisos internos; no contiene un transporte SMTP adicional.

## Presentación y datos históricos

- `HOROMETRO:<uuid>:38855` se muestra como `Mantenimiento a las 38855.00 h`.
- Origen `SYSTEM`: `Sistema automático`; tipo `HOROMETRO_PROXIMO`: `Mantenimiento próximo por horómetro`.
- Las alertas antiguas de OT recuperan código y título desde la orden. Las referencias de programación, cronograma, reporte, análisis, combustible e inventario tienen descripciones específicas.
- Reservas, entregas, consumos y avisos de stock consultan catálogos históricos para obtener nombres de registros dados de baja. Esto no habilita esos registros para nuevas operaciones.
- Si el registro ya no existe, se presenta `Material sin registro`, `Bodega sin registro` o `Equipo no disponible`.
- Todos los diez puntos SMTP de mantenimiento usan un control final para UUID en asunto, texto y nodos visibles de HTML. Los enlaces y metadatos de entrega permanecen operativos.
- Bienvenida obtiene el nombre y perfil desde el usuario y su rol en seguridad; no utiliza los IDs como contenido.

## Costos y validación

Los correos de consumos respetan el perfil individual del destinatario: solo Administrador, Súper Administrador y Gerente General pueden ver costos. El correo de revisión retira el costo del aceite para Supervisores.

Las pruebas capturan `sendMail` sin conexión SMTP. Cubren el caso de la imagen, todas las referencias, las familias de mantenimiento, catálogos históricos, datos faltantes, metadatos/enlaces, incidentes y seis perfiles de costos. Las vistas previas con catálogos reales se generan en una transacción de solo lectura, sin eventos ni envíos externos.

No se requiere una migración de datos. Un rollback del commit revierte la presentación de los correos; no modifica inventario, órdenes ni históricos de alertas.
