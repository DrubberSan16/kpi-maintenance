import type { SendMailOptions } from 'nodemailer';

const UUID = '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}';
const opaqueId = new RegExp(`${UUID}|\\b[0-9a-f]{32}\\b`, 'i');
const technicalReference = new RegExp(
  `\\b[A-Z][A-Z_]*(?::[A-Z_]+)*:(?:${UUID}|[0-9a-f]{32})(?::[^\\s<>,;|]+)*`,
  'gi',
);

/** Business codes (OT-A00163, BOD-001, etc.) remain useful to the recipient. */
export function emailLabel(fallback: string, ...values: unknown[]): string {
  for (const value of values) {
    const label = String(value ?? '').trim();
    if (label && !opaqueId.test(label) && !/^\d+$/.test(label)) return label;
  }
  return fallback;
}

export function emailDisplayText(value: string): string {
  return value
    .replace(technicalReference, 'Referencia no disponible')
    .replace(
      new RegExp(`${UUID}|\\b[0-9a-f]{32}\\b`, 'gi'),
      'Referencia no disponible',
    );
}

export function emailHours(value: unknown): string {
  if (
    value == null ||
    String(value).trim() === '' ||
    !Number.isFinite(Number(value))
  )
    return 'No disponible';
  return `${Number(value).toFixed(2)} h`;
}

/** Final guard for old payloads/free text; links and delivery metadata stay intact. */
export function readableEmail(options: SendMailOptions): SendMailOptions {
  return {
    ...options,
    subject:
      typeof options.subject === 'string'
        ? emailDisplayText(options.subject)
        : options.subject,
    text:
      typeof options.text === 'string'
        ? emailDisplayText(options.text)
        : options.text,
    html:
      typeof options.html === 'string'
        ? options.html
            .split(/(<[^>]*>)/g)
            .map((part) =>
              part.startsWith('<') ? part : emailDisplayText(part),
            )
            .join('')
        : options.html,
  };
}

const labels: Record<string, string> = {
  SYSTEM: 'Sistema automático',
  WORK_ORDER: 'Orden de trabajo',
  ORDEN_TRABAJO: 'Orden de trabajo',
  PROGRAMACION: 'Programación de mantenimiento',
  PROGRAMACION_MENSUAL: 'Programación mensual',
  CRONOGRAMA_SEMANAL: 'Cronograma semanal',
  EQUIPO_SERVICIO_TIEMPO: 'Servicio por tiempo',
  REPORTE_DIARIO: 'Reporte diario',
  ANALISIS_LUBRICANTE: 'Análisis de lubricante',
  INVENTARIO_RESUMEN: 'Resumen de inventario',
  STOCK_BODEGA: 'Stock de bodega',
  MANTENIMIENTO: 'Mantenimiento',
  OPERACION: 'Operación',
  LUBRICANTE: 'Lubricante',
  INVENTARIO: 'Inventario',
  COMBUSTIBLE: 'Combustible',
  HOROMETRO: 'Horómetro',
  ABIERTA: 'Abierta',
  EN_PROCESO: 'En proceso',
  CERRADA: 'Cerrada',
  PLANNED: 'Planificada',
  OPEN: 'Abierta',
  IN_PROGRESS: 'En proceso',
  IN_REVIEW: 'En revisión',
  CLOSED: 'Cerrada',
  CANCELLED: 'Anulada',
  HOROMETRO_PROXIMO: 'Mantenimiento próximo por horómetro',
  HOROMETRO_VENCIDO: 'Mantenimiento vencido por horómetro',
  MANTENIMIENTO_PROXIMO: 'Mantenimiento próximo',
  MANTENIMIENTO_VENCIDO: 'Mantenimiento vencido',
  PROGRAMACION_VENCIDA: 'Programación vencida',
  PROGRAMACION_REPROGRAMADA: 'Programación reprogramada',
  REPORTE_DIARIO_PROXIMO: 'Mantenimiento próximo del reporte diario',
  REPORTE_DIARIO_VENCIDO: 'Mantenimiento vencido del reporte diario',
  LUBRICANTE_CRITICO: 'Condición crítica del lubricante',
  LUBRICANTE_ALERTA: 'Alerta de lubricante',
  COMBUSTIBLE_BAJO: 'Combustible bajo el mínimo',
  COMBUSTIBLE_PROXIMO_MINIMO: 'Combustible próximo al mínimo',
  SIN_STOCK: 'Sin stock disponible',
  STOCK_BAJO_BODEGA: 'Stock bajo el mínimo en bodega',
  SERVICIO_EQUIPO_TIEMPO: 'Mantenimiento del equipo por tiempo',
  EVENTO_A_REALIZAR: 'Actividad programada pendiente',
  ORDEN_TRABAJO_AUTOGENERADA: 'Orden de trabajo generada automáticamente',
  ORDEN_TRABAJO_GENERADA: 'Orden de trabajo generada',
  ORDEN_TRABAJO_CREADA: 'Orden de trabajo creada',
  ORDEN_TRABAJO_CONSUMOS: 'Consumos registrados en la orden',
  ORDEN_TRABAJO_FINALIZADA: 'Orden de trabajo finalizada',
  ORDEN_TRABAJO_BLOQUEADA: 'Orden de trabajo bloqueada',
  ORDEN_TRABAJO_DESBLOQUEADA: 'Orden de trabajo desbloqueada',
  ORDEN_TRABAJO_EN_REVISION: 'Orden de trabajo en revisión',
};

export function emailEnumLabel(value: unknown): string {
  const raw = emailLabel('No disponible', value);
  const key = raw.toUpperCase();
  if (labels[key]) return labels[key];
  const words = raw.replace(/_/g, ' ').toLocaleLowerCase('es');
  return words.charAt(0).toUpperCase() + words.slice(1);
}
