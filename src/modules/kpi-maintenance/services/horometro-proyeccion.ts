/**
 * Regla única del "último mantenimiento" por horómetro.
 *
 * La comparten la proyección del Dashboard administrativo (puntos 5, 6 y 7) y
 * las alertas por horómetro: si una cambia, la otra tiene que cambiar con ella.
 *
 * Solo reinicia el ciclo un servicio del plan: OT cerrada, no anulada, de una
 * plantilla MPG por horas ("MPG 500 HORAS MTU", "MPG 250 HRS - CAT-CUMMINS",
 * "MPG 1000 HRS - CAT"). Cuenta aunque la OT se haya registrado como
 * correctiva (pasa con varias MPG 250 de Cummins): el servicio se hizo igual.
 * Los cebados se hacen casi a diario y los preventivos menores (filtro racor)
 * cada pocos días: contarlos dejaba a toda la flota con la frecuencia completa
 * por delante y el semáforo nunca salía del verde. Decisión del usuario del
 * 2026-10-15.
 */

/** Valores de `status` o `approval_action` que marcan una OT anulada. */
const ESTADOS_ANULADOS = `'ANULADA', 'ANULADO', 'CANCELLED', 'CANCELED', 'CANCELADA', 'CANCELADO', 'VOID', 'VOIDED'`;

/**
 * Subconsulta correlacionada con el equipo `e`: sus servicios MPG, del más
 * reciente al más antiguo, como arreglo JSON. Va sin barras invertidas a
 * propósito (clases POSIX en la expresión regular): dentro de una plantilla de
 * JavaScript `\s` se convierte en `s` sin avisar.
 *
 * Una OT anulada conserva `status_workflow = CLOSED`; la anulación está en
 * `status` y en `valor_json` (`approval_action`, `annulment`), igual que en
 * `isWorkOrderAnnulled`.
 */
export const SERVICIOS_MPG_SQL = `
  (
    SELECT COALESCE(json_agg(s ORDER BY s.cerrada_en DESC NULLS LAST), '[]'::json)
    FROM (
      SELECT
        wo.code AS work_order_code,
        COALESCE(wo.hora_fin, wo.closed_at) AS cerrada_en,
        p.nombre AS plan_nombre,
        COALESCE(
          NULLIF(wo.valor_json ->> 'horometro_actual', ''),
          NULLIF(wo.valor_json ->> 'horometro_inicio_ejecucion', '')
        ) AS horometro
      FROM kpi_process.tb_work_order wo
      INNER JOIN kpi_maintenance.tb_plan_mantenimiento p ON p.id = wo.plan_id
      WHERE wo.equipment_id = e.id
        AND COALESCE(wo.is_deleted, false) = false
        AND UPPER(COALESCE(wo.status_workflow, '')) IN ('CLOSED', 'CERRADA', 'CERRADO')
        AND UPPER(COALESCE(wo.maintenance_kind, '')) NOT IN ('CEBADO', 'PROYECTO')
        AND UPPER(COALESCE(wo.status, '')) NOT IN (${ESTADOS_ANULADOS})
        AND UPPER(COALESCE(wo.valor_json ->> 'approval_action', '')) NOT IN (${ESTADOS_ANULADOS})
        AND COALESCE(jsonb_typeof(wo.valor_json::jsonb -> 'annulment'), '') <> 'object'
        AND p.nombre ~* '^[[:space:]]*MPG[[:space:]-]*[0-9]+[[:space:]]*H'
    ) s
  )`;

export type ServicioMpg = {
  work_order_code?: string | null;
  cerrada_en?: string | null;
  plan_nombre?: string | null;
  horometro?: string | number | null;
};

export type ReferenciaMantenimiento = {
  /** Horómetro desde el que se cuenta la frecuencia. */
  base: number;
  /** El último servicio dejó una lectura utilizable. */
  fiable: boolean;
  /** Por qué la base es un supuesto; `null` cuando es fiable. */
  motivo: string | null;
  /** Servicio del que sale la base; `null` si se asumió el ciclo. */
  servicio: ServicioMpg | null;
};

function lecturaDe(servicio: ServicioMpg | null | undefined): number | null {
  const raw = servicio?.horometro;
  if (raw === null || raw === undefined || String(raw).trim() === '') return null;
  const value = Number(raw);
  return Number.isFinite(value) ? value : null;
}

/**
 * Elige la base del ciclo entre los servicios MPG del equipo (ya ordenados del
 * más reciente al más antiguo).
 *
 * El objetivo nunca puede salir igual al horómetro actual. Antes, sin una
 * lectura fiable se tomaba "actual menos la frecuencia" y el objetivo quedaba
 * exactamente en el horómetro actual, con 0 h restantes. Ahora:
 *  1. el último servicio, si su lectura es mayor que cero y no supera al actual;
 *  2. si no, el servicio anterior más reciente que sí la tenga (base real, pero
 *     marcada para que alguien corrija la OT sin lectura);
 *  3. si ninguno sirve, el ciclo en múltiplos de la frecuencia (52.169 h con
 *     500 h de frecuencia proyecta a 52.500 h), también marcado.
 */
export function resolverReferenciaMantenimiento(
  servicios: unknown,
  horometroActual: number,
  frecuencia: number,
): ReferenciaMantenimiento {
  const lista = (Array.isArray(servicios) ? servicios : []) as ServicioMpg[];
  const utilizable = (servicio: ServicioMpg) => {
    const lectura = lecturaDe(servicio);
    return lectura !== null && lectura > 0 && lectura <= horometroActual;
  };

  const ultimo = lista[0] ?? null;
  if (ultimo && utilizable(ultimo)) {
    return { base: lecturaDe(ultimo) as number, fiable: true, motivo: null, servicio: ultimo };
  }

  const codigo = ultimo?.work_order_code ?? 'sin código';
  const lecturaUltimo = lecturaDe(ultimo);
  const problema = !ultimo
    ? 'Sin servicio MPG cerrado'
    : lecturaUltimo === null
      ? `El último servicio (${codigo}) no registró horómetro`
      : lecturaUltimo <= 0
        ? `El último servicio (${codigo}) quedó con horómetro en cero`
        : `El último servicio (${codigo}) tiene un horómetro mayor al actual`;

  const anterior = lista.find(utilizable);
  if (anterior) {
    return {
      base: lecturaDe(anterior) as number,
      fiable: false,
      motivo: `${problema}; se proyecta desde ${anterior.work_order_code ?? 'el servicio anterior'}`,
      servicio: anterior,
    };
  }

  return {
    base: Math.floor(horometroActual / frecuencia) * frecuencia,
    fiable: false,
    motivo: `${problema}; se asume el ciclo en múltiplos de ${frecuencia} h`,
    servicio: null,
  };
}

/** Fecha del calendario de Guayaquil (`YYYY-MM-DD`) de un instante. */
const FECHA_GUAYAQUIL = new Intl.DateTimeFormat('en-CA', {
  timeZone: 'America/Guayaquil',
  year: 'numeric',
  month: '2-digit',
  day: '2-digit',
});

export function fechaGuayaquil(instante: Date): string {
  return FECHA_GUAYAQUIL.format(instante);
}

/**
 * Fecha en que el horómetro llega al objetivo al ritmo de operación del
 * equipo. Con horas negativas (vencido) cae en el pasado: es cuándo debió
 * hacerse. Sin ritmo medible no hay fecha.
 */
export function fechaProximoMantenimiento(
  horasRestantes: number,
  ritmoHorasDia: number | null,
  ahora: Date,
): string | null {
  if (ritmoHorasDia === null || !Number.isFinite(ritmoHorasDia) || ritmoHorasDia <= 0) return null;
  return fechaGuayaquil(new Date(ahora.getTime() + (horasRestantes / ritmoHorasDia) * 86400000));
}
