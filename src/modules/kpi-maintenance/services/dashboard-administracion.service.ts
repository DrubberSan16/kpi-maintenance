import { Injectable, Logger } from '@nestjs/common';
import { InjectDataSource } from '@nestjs/typeorm';
import { DataSource } from 'typeorm';
import { workOrderWorkedHours } from './work-order-hours';
import {
  SERVICIOS_MPG_SQL,
  fechaProximoMantenimiento,
  resolverReferenciaMantenimiento,
} from './horometro-proyeccion';
import { aceiteEnGalonesSql } from './material-cost-category';

/**
 * Dashboard Administración: KPI de mantenimiento.
 *
 * Se implementa aparte del servicio principal a propósito: son consultas de
 * agregación que no comparten estado con el CRUD y meterlas en un archivo de
 * 30.000 líneas lo haría irrevisable.
 *
 * Todas las consultas van en SQL porque son agregaciones sobre tres esquemas
 * (`kpi_maintenance`, `kpi_process`, `kpi_inventory`) y resolverlas en memoria
 * obligaría a traerse tablas enteras.
 *
 * Convenciones que atraviesan el módulo:
 *  - Las horas de una OT salen de `hora_inicio`/`hora_fin`, no de la duración
 *    del flujo ni del valor tecleado en `horas_a_realizar`.
 *  - La disponibilidad sale del historial de funcionamiento del equipo, que es
 *    la fuente que registra realmente cuándo estuvo parado.
 *  - El semáforo del horómetro usa los márgenes configurables de cada equipo.
 */
/**
 * Lectura de horometro normalizada: entero o `null`.
 *
 * El horometro cuenta horas enteras; los decimales que llegan son relleno de la
 * columna `numeric(18, 2)` en que se guarda, no precision.
 */
function normalizeHorometro(value: unknown): number | null {
  if (value === null || value === undefined || String(value).trim() === '') {
    return null;
  }
  const num = Number(value);
  return Number.isFinite(num) ? Math.round(num) : null;
}

/** Cómo se lee la unidad guardada de una frecuencia por calendario. */
const UNIDADES_CALENDARIO: Record<string, string> = {
  DIAS: 'días',
  SEMANAS: 'semanas',
  MESES: 'meses',
  ANIOS: 'años',
};

@Injectable()
export class DashboardAdministracionService {
  private readonly logger = new Logger(DashboardAdministracionService.name);

  /** Umbrales de galones para el semáforo de cebado (punto 3). */
  private readonly CEBADO_VERDE_MAX = 5;
  private readonly CEBADO_AMARILLO_MAX = 10;

  /**
   * Una fila por OT, también cuando no consumió aceite. Misma fuente para
   * gráficos y exportes de los dos tableros, Gerencia y Administración.
   *
   * La sucursal activa llega en la cabecera `x-sucursal-id`. Sin filtrarla, el
   * PDF de una central (TPTA) traía también las OT de la otra (CPT). La OT se
   * ubica por la foto de ubicación que guardó al crearse y, si no la tiene, por
   * la ubicación actual del equipo, igual que el listado de OT.
   */
  async getControlCebadoAceite(
    query: { desde?: string; hasta?: string; equipo_id?: string; solo_cebado?: boolean },
    sucursalId?: string | null,
  ) {
    const period = this.resolvePeriod();
    // Son días de Guayaquil sobre columnas timestamp sin zona: no convertirlos a UTC.
    const desde = query.desde ? `${query.desde.slice(0, 10)} 00:00:00` : period.desde;
    const hasta = query.hasta ? `${query.hasta.slice(0, 10)} 23:59:59.999999` : period.hasta;
    const sucursal = this.equipoParam(sucursalId);
    const filas = await this.dataSource.query(`
      SELECT wo.id AS work_order_id, wo.code AS work_order_code, wo.title AS work_order_title,
        wo.maintenance_kind, wo.status_workflow AS work_order_status,
        wo.started_at, wo.closed_at, wo.valor_json,
        wo.scheduled_start AS fecha_programada,
        COALESCE(wo.hora_inicio, wo.started_at, wo.created_at) AS fecha_referencia,
        wo.equipment_id, e.codigo AS equipment_code, COALESCE(e.nombre, e.nombre_real) AS equipment_name,
        m.nombre AS equipment_brand_name, e.es_reserva,
        CASE WHEN wo.equipment_location_snapshot IS NOT NULL THEN (wo.equipment_location_snapshot->>'location_id')::uuid ELSE e.location_id END AS location_id,
        CASE WHEN wo.equipment_location_snapshot IS NOT NULL THEN CONCAT_WS(' - ', wo.equipment_location_snapshot->>'location_code', wo.equipment_location_snapshot->>'location_name', wo.equipment_location_snapshot->>'sucursal_name') ELSE CONCAT_WS(' - ', l.codigo, l.nombre) END AS central,
        NULLIF(wo.valor_json->>'horometro_anterior', '')::numeric AS horometro_inicial,
        NULLIF(wo.valor_json->>'horometro_actual', '')::numeric AS horometro_final,
        COALESCE(oil.cantidad, 0)::numeric AS cantidad, COALESCE(oil.subtotal, 0)::numeric AS subtotal,
        COALESCE(oil.producto, 'Sin consumo de aceite') AS producto,
        COALESCE(hist.events, '[]'::jsonb) AS history
      FROM kpi_process.tb_work_order wo
      LEFT JOIN kpi_maintenance.tb_equipo e ON e.id = wo.equipment_id
      LEFT JOIN kpi_inventory.tb_marca m ON m.id = e.marca_id
      LEFT JOIN kpi_maintenance.tb_location l ON l.id = e.location_id AND NOT COALESCE(l.is_deleted, false)
      LEFT JOIN LATERAL (
        SELECT SUM(cr.cantidad) AS cantidad,
          SUM(cr.cantidad * COALESCE(issued.unit_cost, cr.costo_unitario, 0)) AS subtotal,
          string_agg(DISTINCT CONCAT_WS(' - ', p.codigo, p.nombre), ', ') AS producto
        FROM kpi_maintenance.tb_consumo_repuesto cr
        JOIN kpi_inventory.tb_producto p ON p.id = cr.producto_id AND ${aceiteEnGalonesSql('p')}
        LEFT JOIN LATERAL (
          SELECT SUM(d.cantidad * d.costo_unitario) / NULLIF(SUM(d.cantidad), 0) AS unit_cost
          FROM kpi_inventory.tb_entrega_material em
          JOIN kpi_inventory.tb_entrega_material_det d ON d.entrega_id = em.id
          WHERE em.work_order_id = wo.id AND NOT COALESCE(em.is_deleted, false)
            AND d.producto_id = cr.producto_id AND d.bodega_id = cr.bodega_id
        ) issued ON true
        WHERE cr.work_order_id = wo.id AND NOT COALESCE(cr.is_deleted, false)
      ) oil ON true
      LEFT JOIN LATERAL (
        SELECT jsonb_agg(jsonb_build_object('changed_at', h.changed_at, 'from_status', h.from_status,
          'to_status', h.to_status) ORDER BY h.changed_at, h.id) AS events
        FROM kpi_process.tb_work_order_status_history h WHERE h.work_order_id = wo.id
      ) hist ON true
      WHERE NOT COALESCE(wo.is_deleted, false)
        AND UPPER(COALESCE(wo.status, '')) NOT IN ('ANULADA', 'ANULADO', 'CANCELLED', 'CANCELED', 'VOID', 'VOIDED')
        AND UPPER(COALESCE(wo.status_workflow, '')) NOT IN ('ANULADA', 'ANULADO', 'CANCELLED', 'CANCELED', 'VOID', 'VOIDED')
        AND UPPER(COALESCE(wo.valor_json->>'approval_action', '')) NOT IN ('ANULADA', 'ANULADO', 'CANCELADA', 'CANCELADO')
        AND COALESCE(wo.valor_json->'annulment', 'null'::jsonb) = 'null'::jsonb
        AND COALESCE(wo.hora_inicio, wo.started_at, wo.created_at) BETWEEN $1::timestamp AND $2::timestamp
        AND ($3::uuid IS NULL OR wo.equipment_id = $3::uuid)
        AND (NOT $4::boolean OR UPPER(wo.maintenance_kind) = 'CEBADO')
        AND ($5::uuid IS NULL OR COALESCE(
          NULLIF(wo.equipment_location_snapshot->>'sucursal_id', '')::uuid, l.sucursal_id) = $5::uuid)
      ORDER BY fecha_referencia, wo.code
    `, [desde, hasta, this.equipoParam(query.equipo_id), query.solo_cebado === true, sucursal]);
    const equipos = await this.dataSource.query(`
      SELECT e.id, e.codigo, COALESCE(e.nombre, e.nombre_real) AS nombre, e.es_reserva,
        e.location_id, CONCAT_WS(' - ', l.codigo, l.nombre) AS central
      FROM kpi_maintenance.tb_equipo e
      LEFT JOIN kpi_maintenance.tb_equipo_tipo t ON t.id = e.equipo_tipo_id
      LEFT JOIN kpi_maintenance.tb_location l ON l.id = e.location_id AND NOT COALESCE(l.is_deleted, false)
      WHERE NOT COALESCE(e.is_deleted, false) AND ($1::uuid IS NULL OR l.sucursal_id = $1::uuid) AND (
        UPPER(COALESCE(t.nombre, '') || ' ' || COALESCE(t.codigo, '')) SIMILAR TO '%(GENERACI|GENERADOR)%'
        OR EXISTS (SELECT 1 FROM kpi_process.tb_work_order wo WHERE wo.equipment_id = e.id
          AND NOT COALESCE(wo.is_deleted, false)
          AND (wo.started_at IS NOT NULL OR wo.status_workflow IN ('IN_PROGRESS', 'REVIEW', 'CLOSED'))
          AND UPPER(COALESCE(wo.status, '')) NOT LIKE '%ANUL%'
          AND UPPER(COALESCE(wo.valor_json->>'approval_action', '')) NOT IN ('ANULADA', 'ANULADO', 'CANCELADA', 'CANCELADO')
          AND COALESCE(wo.valor_json->'annulment', 'null'::jsonb) = 'null'::jsonb)
      ) ORDER BY e.codigo, e.nombre, e.id
    `, [sucursal]);
    const inventario = await this.getInventarioAceite(desde, hasta, sucursal);
    return this.wrap({ filas: filas.map(({ history, valor_json, ...row }) => ({
      ...row, cantidad: Number(row.cantidad), subtotal: Number(row.subtotal),
      horas_trabajadas: workOrderWorkedHours({ ...row, valor_json, status_workflow: row.work_order_status }, history),
      semaforo: this.resolveCebadoSemaforo(Number(row.cantidad)),
    })), equipos, inventario }, 'Control de cebado y aceite generado');
  }

  /**
   * Movimiento del aceite en las bodegas de la sucursal durante el período.
   *
   * Igual que el resumen del Kardex, la existencia se cuenta hacia atrás desde
   * el stock real de cada bodega, que es el dato autoritativo: existencia al
   * cierre = stock de hoy - entradas posteriores + salidas posteriores, y la
   * inicial resta los movimientos del período. Así la tabla siempre cierra:
   * inicial + ingresos - salidas = final.
   *
   * Los valores salen del costo de cada asiento del kardex (FIFO), y el stock
   * de hoy se valoriza con el costo promedio de las capas vigentes de la bodega.
   *
   * Las transferencias se separan de las compras: entre bodegas de la misma
   * sucursal salen y entran a la vez, y sumarlas como compra inflaría el
   * ingreso. Una entrada ligada a una OT es material devuelto.
   */
  private async getInventarioAceite(desde: string, hasta: string, sucursal: string | null) {
    const [rows, stock] = await Promise.all([
      this.dataSource.query(`
        SELECT
          CASE
            WHEN k.fecha > $2::timestamp THEN 'POSTERIOR'
            WHEN tr.es_transferencia AND k.entrada_cantidad > 0 THEN 'TRANSFERENCIAS_RECIBIDAS'
            WHEN tr.es_transferencia THEN 'TRANSFERENCIAS_ENVIADAS'
            WHEN UPPER(COALESCE(k.tipo_movimiento, '')) LIKE 'AJUSTE%' THEN 'AJUSTES'
            WHEN k.entrada_cantidad > 0 AND m.work_order_id IS NOT NULL THEN 'DEVOLUCIONES'
            WHEN k.entrada_cantidad > 0 THEN 'COMPRAS'
            ELSE 'SALIDAS'
          END AS concepto,
          SUM(k.entrada_cantidad)::numeric AS entradas,
          SUM(k.salida_cantidad)::numeric AS salidas,
          SUM(CASE WHEN k.entrada_cantidad > 0 THEN k.costo_total ELSE 0 END)::numeric AS costo_entradas,
          SUM(CASE WHEN k.entrada_cantidad > 0 THEN 0 ELSE k.costo_total END)::numeric AS costo_salidas
        FROM kpi_inventory.tb_kardex k
        JOIN kpi_inventory.tb_producto p ON p.id = k.producto_id AND ${aceiteEnGalonesSql('p')}
        JOIN kpi_inventory.tb_bodega b ON b.id = k.bodega_id
        LEFT JOIN kpi_inventory.tb_movimiento_inventario m ON m.id = k.movimiento_id
        LEFT JOIN LATERAL (
          SELECT true AS es_transferencia FROM kpi_inventory.tb_transferencia_bodega t
          WHERE k.movimiento_id IS NOT NULL
            AND (t.movimiento_salida_id = k.movimiento_id OR t.movimiento_ingreso_id = k.movimiento_id)
          LIMIT 1
        ) tr ON true
        WHERE NOT COALESCE(k.is_deleted, false)
          AND k.fecha >= $1::timestamp
          AND ($3::uuid IS NULL OR b.sucursal_id = $3::uuid)
        GROUP BY 1
      `, [desde, hasta, sucursal]),
      this.dataSource.query(`
        SELECT COALESCE(SUM(sb.stock_actual), 0)::numeric AS cantidad,
          COALESCE(SUM(sb.stock_actual * COALESCE(sb.costo_promedio_bodega, 0)), 0)::numeric AS costo
        FROM kpi_inventory.tb_stock_bodega sb
        JOIN kpi_inventory.tb_producto p ON p.id = sb.producto_id AND ${aceiteEnGalonesSql('p')}
        JOIN kpi_inventory.tb_bodega b ON b.id = sb.bodega_id AND NOT COALESCE(b.is_deleted, false)
        WHERE NOT COALESCE(sb.is_deleted, false)
          AND ($1::uuid IS NULL OR b.sucursal_id = $1::uuid)
      `, [sucursal]),
    ]);
    const round = (value: number) => Number(value.toFixed(4));
    const byConcept = new Map<string, any>(rows.map((row: any) => [row.concepto, row]));
    const flow = (concepto: string, sentido: 'entrada' | 'salida') => {
      const row = byConcept.get(concepto);
      return {
        cantidad: round(Number(sentido === 'entrada' ? row?.entradas : row?.salidas) || 0),
        costo: round(Number(sentido === 'entrada' ? row?.costo_entradas : row?.costo_salidas) || 0),
      };
    };
    // Un ajuste puede sumar o restar: se informa neto.
    const ajusteRow = byConcept.get('AJUSTES');
    const ajustes = {
      cantidad: round((Number(ajusteRow?.entradas) || 0) - (Number(ajusteRow?.salidas) || 0)),
      costo: round((Number(ajusteRow?.costo_entradas) || 0) - (Number(ajusteRow?.costo_salidas) || 0)),
    };
    const compras = flow('COMPRAS', 'entrada');
    const devoluciones = flow('DEVOLUCIONES', 'entrada');
    const transferenciasRecibidas = flow('TRANSFERENCIAS_RECIBIDAS', 'entrada');
    const salidas = flow('SALIDAS', 'salida');
    const transferenciasEnviadas = flow('TRANSFERENCIAS_ENVIADAS', 'salida');
    const posterior = byConcept.get('POSTERIOR');
    const saldoActual = { cantidad: round(Number(stock[0]?.cantidad) || 0), costo: round(Number(stock[0]?.costo) || 0) };
    const existenciaFinal = {
      cantidad: round(saldoActual.cantidad - (Number(posterior?.entradas) || 0) + (Number(posterior?.salidas) || 0)),
      costo: round(saldoActual.costo - (Number(posterior?.costo_entradas) || 0) + (Number(posterior?.costo_salidas) || 0)),
    };
    const neto = (field: 'cantidad' | 'costo') =>
      compras[field] + devoluciones[field] + transferenciasRecibidas[field] + ajustes[field]
      - salidas[field] - transferenciasEnviadas[field];
    return {
      existencia_inicial: {
        cantidad: round(existenciaFinal.cantidad - neto('cantidad')),
        costo: round(existenciaFinal.costo - neto('costo')),
      },
      compras,
      transferencias_recibidas: transferenciasRecibidas,
      devoluciones,
      ajustes,
      salidas,
      transferencias_enviadas: transferenciasEnviadas,
      existencia_final: existenciaFinal,
      saldo_actual: saldoActual,
    };
  }

  constructor(@InjectDataSource() private readonly dataSource: DataSource) {}

  private wrap(data: unknown, message = 'OK', meta?: unknown) {
    return { data, meta, message };
  }

  /**
   * Filtro opcional por equipo. Se resuelve como parametro $3 y la condicion se
   * escribe siempre igual: `($3::uuid IS NULL OR <col> = $3::uuid)`. Asi la
   * misma consulta sirve filtrada y sin filtrar, sin concatenar SQL.
   */
  private equipoParam(equipoId?: string | null) {
    const value = String(equipoId ?? '').trim();
    return value ? value : null;
  }

  private resolvePeriod(desde?: string, hasta?: string) {
    const end = hasta ? new Date(`${hasta}T23:59:59`) : new Date();
    const start = desde
      ? new Date(`${desde}T00:00:00`)
      : new Date(end.getTime() - 30 * 24 * 3600 * 1000);
    return {
      desde: start.toISOString().slice(0, 19).replace('T', ' '),
      hasta: end.toISOString().slice(0, 19).replace('T', ' '),
    };
  }

  /**
   * 1. Disponibilidad por equipo.
   *
   * Reconstruye la línea de tiempo de `tb_equipo_funcionamiento_historial`
   * dentro del período. Cada fila dice qué estado terminó (`estado_anterior`) y
   * cuándo (`changed_at`), así que el tramo va del cambio previo al actual. El
   * tramo final se cierra con el estado vigente del equipo.
   *
   * Los tramos se recortan al período para que un paro que viene de antes no
   * infle las horas del rango consultado.
   */
  private async getDisponibilidad(desde: string, hasta: string, equipoId: string | null) {
    return this.dataSource.query(
      `
      WITH tramos AS (
        SELECT
          h.equipo_id,
          h.estado_anterior AS estado,
          GREATEST(
            COALESCE(h.estado_anterior_desde, $1::timestamp),
            $1::timestamp
          ) AS inicio,
          LEAST(h.changed_at, $2::timestamp) AS fin
        FROM kpi_maintenance.tb_equipo_funcionamiento_historial h
        WHERE h.estado_anterior IS NOT NULL
          AND h.changed_at > $1::timestamp
          AND COALESCE(h.estado_anterior_desde, h.changed_at) < $2::timestamp
      ),
      ultimo_cambio AS (
        SELECT DISTINCT ON (equipo_id) equipo_id, estado_nuevo, changed_at
        FROM kpi_maintenance.tb_equipo_funcionamiento_historial
        WHERE changed_at <= $2::timestamp
        ORDER BY equipo_id, changed_at DESC
      ),
      tramo_abierto AS (
        -- Tramo vigente: desde el último cambio hasta el fin del período.
        SELECT
          u.equipo_id,
          u.estado_nuevo AS estado,
          GREATEST(u.changed_at, $1::timestamp) AS inicio,
          $2::timestamp AS fin
        FROM ultimo_cambio u
        WHERE u.changed_at < $2::timestamp
      ),
      todos AS (
        SELECT * FROM tramos UNION ALL SELECT * FROM tramo_abierto
      ),
      agregado AS (
        SELECT
          equipo_id,
          SUM(
            CASE WHEN estado = 'PARADO'
              THEN GREATEST(EXTRACT(EPOCH FROM (fin - inicio)), 0) ELSE 0 END
          ) AS seg_parado,
          SUM(
            CASE WHEN estado = 'FUNCIONAMIENTO'
              THEN GREATEST(EXTRACT(EPOCH FROM (fin - inicio)), 0) ELSE 0 END
          ) AS seg_funcionando
        FROM todos
        WHERE fin > inicio
        GROUP BY equipo_id
      )
      SELECT
        e.id AS equipo_id,
        e.codigo AS equipo_codigo,
        COALESCE(e.nombre, e.nombre_real) AS equipo_nombre,
        e.nombre_real AS equipo_descripcion,
        e.estado_funcionamiento,
        e.es_reserva,
        ROUND((COALESCE(a.seg_funcionando, 0) / 3600.0)::numeric, 2) AS horas_disponibles,
        ROUND((COALESCE(a.seg_parado, 0) / 3600.0)::numeric, 2) AS horas_fuera_servicio,
        CASE
          WHEN COALESCE(a.seg_funcionando, 0) + COALESCE(a.seg_parado, 0) = 0 THEN NULL
          ELSE ROUND(
            (COALESCE(a.seg_funcionando, 0) * 100.0
              / (COALESCE(a.seg_funcionando, 0) + COALESCE(a.seg_parado, 0)))::numeric, 2)
        END AS porcentaje_disponibilidad
      FROM kpi_maintenance.tb_equipo e
      LEFT JOIN agregado a ON a.equipo_id = e.id
      WHERE COALESCE(e.is_deleted, false) = false
        AND UPPER(COALESCE(e.status, 'ACTIVE')) = 'ACTIVE'
        AND ($3::uuid IS NULL OR e.id = $3::uuid)
      ORDER BY porcentaje_disponibilidad ASC NULLS LAST, e.codigo
      `,
      [desde, hasta, equipoId],
    );
  }

  /**
   * 2. Correctivos por equipo y reincidencia de fallas.
   *
   * La reincidencia se agrupa por equipo y compartimiento: sin catálogo de
   * fallas cargado, el componente intervenido es el mejor indicador de que la
   * misma avería se repite.
   */
  private async getCorrectivos(desde: string, hasta: string, equipoId: string | null) {
    const porEquipo = await this.dataSource.query(
      `
      SELECT
        e.id AS equipo_id,
        e.codigo AS equipo_codigo,
        COALESCE(e.nombre, e.nombre_real) AS equipo_nombre,
        e.nombre_real AS equipo_descripcion,
        COUNT(*) AS total_correctivos,
        COUNT(*) FILTER (WHERE wo.hora_inicio IS NOT NULL AND wo.hora_fin IS NOT NULL) AS con_horas,
        ROUND(COALESCE(SUM(
          EXTRACT(EPOCH FROM (wo.hora_fin - wo.hora_inicio)) / 3600.0
        ), 0)::numeric, 2) AS horas_intervencion,
        MAX(COALESCE(wo.hora_inicio, wo.created_at))::date AS ultima_intervencion
      FROM kpi_process.tb_work_order wo
      INNER JOIN kpi_maintenance.tb_equipo e ON e.id = wo.equipment_id
      WHERE COALESCE(wo.is_deleted, false) = false
        AND UPPER(COALESCE(wo.maintenance_kind, '')) = 'CORRECTIVO'
        AND COALESCE(wo.hora_inicio, wo.created_at) BETWEEN $1::timestamp AND $2::timestamp
        AND ($3::uuid IS NULL OR wo.equipment_id = $3::uuid)
      GROUP BY e.id, e.codigo, equipo_nombre, equipo_descripcion
      ORDER BY total_correctivos DESC, e.codigo
      `,
      [desde, hasta, equipoId],
    );

    const reincidencias = await this.dataSource.query(
      `
      SELECT
        e.id AS equipo_id,
        e.codigo AS equipo_codigo,
        COALESCE(e.nombre, e.nombre_real) AS equipo_nombre,
        e.nombre_real AS equipo_descripcion,
        wo.equipo_componente_id,
        COALESCE(c.nombre_oficial, c.nombre, 'Sin compartimiento') AS componente,
        COUNT(*) AS veces,
        MAX(COALESCE(wo.hora_inicio, wo.created_at))::date AS ultima_vez
      FROM kpi_process.tb_work_order wo
      INNER JOIN kpi_maintenance.tb_equipo e ON e.id = wo.equipment_id
      LEFT JOIN kpi_maintenance.tb_equipo_componente c ON c.id = wo.equipo_componente_id
      WHERE COALESCE(wo.is_deleted, false) = false
        AND UPPER(COALESCE(wo.maintenance_kind, '')) = 'CORRECTIVO'
        AND COALESCE(wo.hora_inicio, wo.created_at) BETWEEN $1::timestamp AND $2::timestamp
        AND ($3::uuid IS NULL OR wo.equipment_id = $3::uuid)
      GROUP BY e.id, e.codigo, equipo_nombre, equipo_descripcion, wo.equipo_componente_id, componente
      HAVING COUNT(*) > 1
      ORDER BY veces DESC, e.codigo
      `,
      [desde, hasta, equipoId],
    );

    return { por_equipo: porEquipo, reincidencias };
  }

  /**
   * 3. Cebado y consumo de aceite por equipo.
   *
   * Los galones salen del consumo de productos marcados como aceite en las OT
   * de cebado. El semáforo es por acumulado del período; además se devuelve el
   * acumulado de los últimos 7 y 30 días para leer la tendencia.
   */
  private async getCebado(desde: string, hasta: string, equipoId: string | null) {
    const filas = await this.dataSource.query(
      `
      WITH por_orden AS (
        -- El semaforo mide lo que gasto cada orden, asi que primero se totaliza
        -- por OT y solo despues se agrega por equipo. Sumar todas las lineas del
        -- periodo y semaforizar ese total mediria otra cosa.
        SELECT
          wo.id AS work_order_id,
          wo.equipment_id,
          wo.equipment_location_snapshot - 'captured_at' - 'basis' AS equipment_location_snapshot,
          COALESCE(wo.hora_inicio, wo.created_at) AS momento,
          NULLIF(TRIM(wo.valor_json ->> 'horometro_anterior'), '')::numeric AS horometro_anterior,
          NULLIF(TRIM(wo.valor_json ->> 'horometro_actual'), '')::numeric AS horometro_actual,
          SUM(cr.cantidad) AS galones,
          SUM(cr.cantidad * COALESCE(issued.costo_unitario, cr.costo_unitario, 0)) AS costo
        FROM kpi_process.tb_work_order wo
        INNER JOIN kpi_maintenance.tb_consumo_repuesto cr
          ON cr.work_order_id = wo.id AND COALESCE(cr.is_deleted, false) = false
        INNER JOIN kpi_inventory.tb_producto p
          ON p.id = cr.producto_id AND COALESCE(p.es_aceite, false) = true
        LEFT JOIN LATERAL (
          SELECT SUM(d.cantidad * d.costo_unitario) / NULLIF(SUM(d.cantidad), 0) AS costo_unitario
          FROM kpi_inventory.tb_entrega_material em
          INNER JOIN kpi_inventory.tb_entrega_material_det d ON d.entrega_id = em.id
          WHERE em.work_order_id = wo.id AND COALESCE(em.is_deleted, false) = false
            AND d.producto_id = cr.producto_id AND d.bodega_id = cr.bodega_id
        ) issued ON true
        WHERE COALESCE(wo.is_deleted, false) = false
          AND UPPER(COALESCE(wo.maintenance_kind, '')) = 'CEBADO'
          AND UPPER(COALESCE(wo.valor_json ->> 'approval_action', '')) NOT IN ('ANULADA', 'ANULADO', 'CANCELADA', 'CANCELADO')
          AND UPPER(COALESCE(wo.status_workflow, '')) NOT IN ('ANULADA', 'CANCELLED', 'VOID')
          AND COALESCE(wo.hora_inicio, wo.created_at) BETWEEN $1::timestamp AND $2::timestamp
          AND ($3::uuid IS NULL OR wo.equipment_id = $3::uuid)
        GROUP BY wo.id, wo.equipment_id, momento
      )
      SELECT
        e.id AS equipo_id,
        e.codigo AS equipo_codigo,
        COALESCE(e.nombre, e.nombre_real) AS equipo_nombre,
        e.nombre_real AS equipo_descripcion,
        CASE WHEN o.equipment_location_snapshot IS NOT NULL THEN (o.equipment_location_snapshot->>'location_id')::uuid ELSE l.id END AS location_id,
        CASE WHEN o.equipment_location_snapshot IS NOT NULL THEN CONCAT_WS(' - ', o.equipment_location_snapshot->>'location_code', o.equipment_location_snapshot->>'location_name', o.equipment_location_snapshot->>'sucursal_name') ELSE CONCAT_WS(' - ', l.codigo, l.nombre) END AS central,
        ROUND(COALESCE(SUM(o.galones), 0)::numeric, 2) AS galones_periodo,
        ROUND(COALESCE(SUM(o.galones) FILTER (
          WHERE o.momento >= $2::timestamp - INTERVAL '7 days'
        ), 0)::numeric, 2) AS galones_semana,
        ROUND(COALESCE(SUM(o.galones) FILTER (
          WHERE o.momento >= $2::timestamp - INTERVAL '30 days'
        ), 0)::numeric, 2) AS galones_mes,
        COUNT(o.work_order_id) AS ots_cebado,
        COUNT(*) FILTER (WHERE o.galones >= 10) AS ots_criticas,
        COUNT(*) FILTER (WHERE o.galones > 5 AND o.galones < 10) AS ots_seguimiento,
        ROUND(COALESCE(MAX(o.galones), 0)::numeric, 2) AS galones_max_orden,
        ROUND(COALESCE(SUM(o.costo), 0)::numeric, 2) AS costo_aceite,
        -- Con que horometro llego la maquina al primer cebado del periodo y con
        -- cual quedo tras el ultimo. Se descartan los nulos: hay ordenes que se
        -- cerraron sin anotarlo y tomar ese hueco daria un recorrido falso.
        (array_agg(o.horometro_anterior ORDER BY o.momento ASC)
           FILTER (WHERE o.horometro_anterior IS NOT NULL))[1] AS horometro_inicial,
        (array_agg(o.horometro_actual ORDER BY o.momento DESC)
           FILTER (WHERE o.horometro_actual IS NOT NULL))[1] AS horometro_final
      FROM por_orden o
      INNER JOIN kpi_maintenance.tb_equipo e ON e.id = o.equipment_id
      LEFT JOIN kpi_maintenance.tb_location l ON l.id = e.location_id AND COALESCE(l.is_deleted, false) = false
      GROUP BY e.id, e.codigo, equipo_nombre, equipo_descripcion, l.id, o.equipment_location_snapshot
      ORDER BY l.nombre NULLS LAST, NULLIF(substring(e.nombre from '(?i)UG[ -]*([0-9]+)'), '')::integer NULLS LAST, e.nombre
      `,
      [desde, hasta, equipoId],
    );

    // El resumen no lleva semaforo ni tendencia propios: ambos son por orden y
    // viven en el detalle. Aqui se informa cuantas ordenes del equipo cayeron en
    // cada nivel, que es el agregado honesto de una medida por OT.
    return filas.map((row: any) => ({
      ...row,
      galones_periodo: Number(row.galones_periodo ?? 0),
      galones_semana: Number(row.galones_semana ?? 0),
      galones_mes: Number(row.galones_mes ?? 0),
      galones_max_orden: Number(row.galones_max_orden ?? 0),
      ots_cebado: Number(row.ots_cebado ?? 0),
      ots_criticas: Number(row.ots_criticas ?? 0),
      ots_seguimiento: Number(row.ots_seguimiento ?? 0),
      // Contador de horas: entero. La columna es numeric(18,2) por herencia y
      // devolvia "15286.00", que la pantalla pintaba tal cual.
      horometro_inicial: normalizeHorometro(row.horometro_inicial),
      horometro_final: normalizeHorometro(row.horometro_final),
    }));
  }

  /**
   * Tendencia de una orden frente a la anterior del mismo equipo.
   *
   * Se usa un margen del 10% para no marcar como cambio lo que es ruido de
   * medicion: repostar 5.0 y luego 5.2 galones no es una tendencia al alza.
   */
  private resolveTendenciaOrden(actual: number, anterior: number | null) {
    if (anterior == null || anterior <= 0) return 'SIN_REFERENCIA';
    if (actual > anterior * 1.1) return 'AL_ALZA';
    if (actual < anterior * 0.9) return 'A_LA_BAJA';
    return 'ESTABLE';
  }

  /** Semaforización de galones del punto 3. */
  private resolveCebadoSemaforo(galones: number) {
    if (galones >= this.CEBADO_AMARILLO_MAX) {
      return { nivel: 'ROJO', etiqueta: 'Consumo crítico', detalle: '10 galones o más en la orden' };
    }
    if (galones > this.CEBADO_VERDE_MAX) {
      return { nivel: 'AMARILLO', etiqueta: 'Seguimiento', detalle: 'Entre 5 y 10 galones en la orden' };
    }
    return { nivel: 'VERDE', etiqueta: 'Normal', detalle: 'Hasta 5 galones en la orden' };
  }

  /** 4. Consumo de repuestos por equipo: cantidad, costo y OT implicadas. */
  private async getRepuestos(desde: string, hasta: string, equipoId: string | null) {
    return this.dataSource.query(
      `
      SELECT
        e.id AS equipo_id,
        e.codigo AS equipo_codigo,
        COALESCE(e.nombre, e.nombre_real) AS equipo_nombre,
        e.nombre_real AS equipo_descripcion,
        COUNT(DISTINCT wo.id) AS ots,
        COUNT(cr.id) AS lineas,
        ROUND(COALESCE(SUM(cr.cantidad), 0)::numeric, 2) AS cantidad,
        ROUND(COALESCE(SUM(cr.subtotal), 0)::numeric, 2) AS costo
      FROM kpi_maintenance.tb_consumo_repuesto cr
      INNER JOIN kpi_process.tb_work_order wo
        ON wo.id = cr.work_order_id AND COALESCE(wo.is_deleted, false) = false
      INNER JOIN kpi_maintenance.tb_equipo e ON e.id = wo.equipment_id
      WHERE COALESCE(cr.is_deleted, false) = false
        AND COALESCE(wo.hora_inicio, wo.created_at) BETWEEN $1::timestamp AND $2::timestamp
        AND ($3::uuid IS NULL OR wo.equipment_id = $3::uuid)
      GROUP BY e.id, e.codigo, equipo_nombre, equipo_descripcion
      ORDER BY costo DESC, e.codigo
      `,
      [desde, hasta, equipoId],
    );
  }

  /**
   * 5, 6 y 7. Frecuencia por horómetro, semáforo anticipado y proyección.
   *
   * El último mantenimiento es el último servicio MPG por horas del equipo
   * (`resolverReferenciaMantenimiento`, la misma regla de las alertas). El
   * objetivo es ese horómetro más la frecuencia: con cuánto horómetro debe
   * hacerse, o debió hacerse, el siguiente. Las restantes son objetivo menos
   * actual y quedan negativas cuando el equipo ya lo pasó.
   *
   * Cuando la base es un supuesto, la fila viaja con `referencia_estimada` y el
   * motivo, para no dejar el equipo fuera de la proyección sin que nadie lo note.
   *
   * La fecha del próximo mantenimiento convierte las restantes en días al ritmo
   * de operación del equipo: horas en FUNCIONAMIENTO de los últimos 30 días según
   * su historial (la misma línea de tiempo que Disponibilidad), llevadas a horas
   * por día.
   *
   * Los umbrales salen de los márgenes configurables de cada unidad, de modo
   * que MTU 500 h, Cummins 350 h y Caterpillar 250 h se resuelven solas. Una
   * frecuencia por calendario (días, semanas, años) no se proyecta por horómetro.
   */
  private async getProyeccion(equipoId: string | null) {
    const ahora = new Date();
    // Se pasan como Date, igual que se guardan los timestamp del historial, para
    // que la ventana y los tramos se lean en la misma zona horaria.
    const ventanaDesde = new Date(ahora.getTime() - 30 * 24 * 3600 * 1000);
    const filas = await this.dataSource.query(
      `
      WITH tramos AS (
        SELECT
          h.equipo_id,
          h.estado_anterior AS estado,
          GREATEST(COALESCE(h.estado_anterior_desde, $2::timestamp), $2::timestamp) AS inicio,
          LEAST(h.changed_at, $3::timestamp) AS fin
        FROM kpi_maintenance.tb_equipo_funcionamiento_historial h
        WHERE h.estado_anterior IS NOT NULL
          AND h.changed_at > $2::timestamp
          AND COALESCE(h.estado_anterior_desde, h.changed_at) < $3::timestamp
      ),
      ultimo_cambio AS (
        SELECT DISTINCT ON (equipo_id) equipo_id, estado_nuevo, changed_at
        FROM kpi_maintenance.tb_equipo_funcionamiento_historial
        WHERE changed_at <= $3::timestamp
        ORDER BY equipo_id, changed_at DESC
      ),
      tramo_abierto AS (
        SELECT
          u.equipo_id,
          u.estado_nuevo AS estado,
          GREATEST(u.changed_at, $2::timestamp) AS inicio,
          $3::timestamp AS fin
        FROM ultimo_cambio u
        WHERE u.changed_at < $3::timestamp
      ),
      ritmo AS (
        SELECT
          equipo_id,
          SUM(CASE WHEN estado = 'FUNCIONAMIENTO' THEN EXTRACT(EPOCH FROM (fin - inicio)) ELSE 0 END) AS seg_funcionando,
          SUM(EXTRACT(EPOCH FROM (fin - inicio))) AS seg_cubiertos
        FROM (SELECT * FROM tramos UNION ALL SELECT * FROM tramo_abierto) t
        WHERE fin > inicio
        GROUP BY equipo_id
      )
      SELECT
        e.id AS equipo_id,
        e.codigo AS equipo_codigo,
        COALESCE(e.nombre, e.nombre_real) AS equipo_nombre,
        e.nombre_real AS equipo_descripcion,
        m.nombre AS marca,
        e.horometro_actual::numeric AS horometro_actual,
        e.estado_funcionamiento,
        e.es_reserva,
        e.horometro_operativo_desde,
        e.intervalo_mantenimiento_valor::numeric AS frecuencia,
        e.intervalo_mantenimiento_unidad AS frecuencia_unidad,
        e.margen_anticipacion_pct::numeric AS margen_anticipacion_pct,
        e.margen_tolerancia_pct::numeric AS margen_tolerancia_pct,
        ROUND((r.seg_funcionando * 24.0 / NULLIF(r.seg_cubiertos, 0))::numeric, 2) AS ritmo_horas_dia,
        ${SERVICIOS_MPG_SQL} AS servicios_mpg
      FROM kpi_maintenance.tb_equipo e
      LEFT JOIN kpi_inventory.tb_marca m ON m.id = e.marca_id
      LEFT JOIN ritmo r ON r.equipo_id = e.id
      WHERE COALESCE(e.is_deleted, false) = false
        AND UPPER(COALESCE(e.status, 'ACTIVE')) = 'ACTIVE'
        AND ($1::uuid IS NULL OR e.id = $1::uuid)
      ORDER BY e.codigo
      `,
      [equipoId, ventanaDesde, ahora],
    );

    return filas
      .map((row: any) => {
        const { servicios_mpg: servicios, ...datos } = row;
        const frecuencia = Number(row.frecuencia ?? 0);
        const unidad = String(row.frecuencia_unidad ?? '').trim().toUpperCase();
        // La columna guarda la lectura del último cambio; mientras el equipo
        // funciona el contador sigue corriendo, igual que en el Control operativo.
        const horometroBase = Number(row.horometro_actual ?? 0);
        const horometroActual = this.horometroOperativo(row, ahora);
        // Sin frecuencia configurada no hay proyeccion posible.
        if (!Number.isFinite(frecuencia) || frecuencia <= 0) {
          return {
            ...datos,
            aplica: false,
            motivo_sin_proyeccion: 'Sin frecuencia configurada',
            semaforo: null,
          };
        }
        // La frecuencia en días la vigila el aviso de servicios por calendario.
        if (unidad && !unidad.startsWith('HORA')) {
          return {
            ...datos,
            aplica: false,
            motivo_sin_proyeccion: `Frecuencia por calendario (${frecuencia} ${UNIDADES_CALENDARIO[unidad] ?? unidad.toLowerCase()})`,
            semaforo: null,
          };
        }

        const referencia = resolverReferenciaMantenimiento(servicios, horometroActual, frecuencia);
        const base = referencia.base;
        const objetivo = Number((base + frecuencia).toFixed(2));
        const restantes = Math.round(objetivo - horometroActual);
        const ritmo = row.ritmo_horas_dia == null ? null : Number(row.ritmo_horas_dia);

        // Se clasifica con los mismos umbrales que viajan a la pantalla, que
        // recalcula el semáforo en vivo: 1500 h al 4,6 % es 69 h, no 68,999...
        const anticipacion = Number(((frecuencia * Number(row.margen_anticipacion_pct ?? 10)) / 100).toFixed(2));
        const tolerancia = Number(((frecuencia * Number(row.margen_tolerancia_pct ?? 0)) / 100).toFixed(2));

        let semaforo: { nivel: string; etiqueta: string };
        if (restantes < -tolerancia) {
          semaforo = { nivel: 'ROJO', etiqueta: 'Superó la tolerancia' };
        } else if (restantes <= anticipacion) {
          semaforo = { nivel: 'AMARILLO', etiqueta: 'Próximo a mantenimiento' };
        } else {
          semaforo = { nivel: 'VERDE', etiqueta: 'Dentro de frecuencia' };
        }

        return {
          ...datos,
          horometro_operativo_base: horometroBase,
          horometro_actual: Math.round(horometroActual),
          aplica: true,
          referencia_estimada: !referencia.fiable,
          motivo_referencia_estimada: referencia.motivo,
          horometro_ultimo_mantenimiento: Math.round(base),
          fecha_ultimo_mantenimiento: referencia.servicio?.cerrada_en
            ? String(referencia.servicio.cerrada_en).slice(0, 10)
            : null,
          ultima_ot: referencia.servicio?.work_order_code ?? null,
          ultimo_plan: referencia.servicio?.plan_nombre ?? null,
          horometro_proximo_mantenimiento: Math.round(objetivo),
          // Sin redondear: la pantalla recalcula las restantes con el contador
          // en vivo y debe partir del mismo objetivo que el semáforo de aquí.
          horometro_objetivo: objetivo,
          horas_restantes: restantes,
          horas_excedidas: restantes < 0 ? Math.abs(restantes) : 0,
          ritmo_horas_dia: ritmo,
          fecha_proximo_mantenimiento: fechaProximoMantenimiento(restantes, ritmo, ahora),
          umbral_amarillo: anticipacion,
          umbral_rojo: tolerancia,
          semaforo,
        };
      })
      .sort((a: any, b: any) => {
        const orden: Record<string, number> = { ROJO: 0, AMARILLO: 1, VERDE: 2 };
        const va = orden[a.semaforo?.nivel] ?? 3;
        const vb = orden[b.semaforo?.nivel] ?? 3;
        if (va !== vb) return va - vb;
        return Number(a.horas_restantes ?? 0) - Number(b.horas_restantes ?? 0);
      });
  }

  /**
   * Lectura guardada más el tiempo en FUNCIONAMIENTO desde
   * `horometro_operativo_desde`: la misma cuenta que `operationalHorometer` en
   * KpiMaintenanceService y que el Control operativo del Dashboard.
   */
  private horometroOperativo(
    row: { horometro_actual?: unknown; estado_funcionamiento?: unknown; horometro_operativo_desde?: unknown },
    ahora: Date,
  ) {
    const base = Number(row.horometro_actual ?? 0);
    const lectura = Number.isFinite(base) ? base : 0;
    if (row.estado_funcionamiento !== 'FUNCIONAMIENTO' || !row.horometro_operativo_desde) return lectura;
    const desde = new Date(row.horometro_operativo_desde as string | Date).getTime();
    if (Number.isNaN(desde)) return lectura;
    return Number((lectura + Math.max(0, ahora.getTime() - desde) / 3600000).toFixed(6));
  }

  /**
   * 8. MTBF y MTTR.
   *
   * MTTR: media de `hora_fin - hora_inicio` de las intervenciones correctivas
   * cerradas, es decir cuánto se tarda en reparar.
   *
   * MTBF: media del tiempo entre el fin de una correctiva y el inicio de la
   * siguiente del mismo equipo, es decir cuánto aguanta entre averías. Necesita
   * al menos dos correctivas para poder medirse.
   */
  private async getConfiabilidad(desde: string, hasta: string, equipoId: string | null) {
    return this.dataSource.query(
      `
      WITH correctivas AS (
        SELECT
          wo.equipment_id,
          wo.hora_inicio,
          wo.hora_fin,
          LAG(wo.hora_fin) OVER (
            PARTITION BY wo.equipment_id ORDER BY wo.hora_inicio
          ) AS fin_anterior
        FROM kpi_process.tb_work_order wo
        WHERE COALESCE(wo.is_deleted, false) = false
          AND UPPER(COALESCE(wo.maintenance_kind, '')) = 'CORRECTIVO'
          AND wo.hora_inicio IS NOT NULL
          AND wo.hora_fin IS NOT NULL
          AND wo.hora_inicio BETWEEN $1::timestamp AND $2::timestamp
          AND ($3::uuid IS NULL OR wo.equipment_id = $3::uuid)
      )
      SELECT
        e.id AS equipo_id,
        e.codigo AS equipo_codigo,
        COALESCE(e.nombre, e.nombre_real) AS equipo_nombre,
        e.nombre_real AS equipo_descripcion,
        COUNT(*) AS intervenciones,
        ROUND(AVG(
          EXTRACT(EPOCH FROM (c.hora_fin - c.hora_inicio)) / 3600.0
        )::numeric, 2) AS mttr_horas,
        ROUND(AVG(
          EXTRACT(EPOCH FROM (c.hora_inicio - c.fin_anterior)) / 3600.0
        ) FILTER (WHERE c.fin_anterior IS NOT NULL)::numeric, 2) AS mtbf_horas,
        COUNT(*) FILTER (WHERE c.fin_anterior IS NOT NULL) AS intervalos_medidos
      FROM correctivas c
      INNER JOIN kpi_maintenance.tb_equipo e ON e.id = c.equipment_id
      GROUP BY e.id, e.codigo, equipo_nombre, equipo_descripcion
      ORDER BY mtbf_horas ASC NULLS LAST, e.codigo
      `,
      [desde, hasta, equipoId],
    );
  }

  /**
   * Serie temporal de consumo de aceite en cebado, por equipo.
   *
   * `granularidad` decide el bucket con `date_trunc`. Se devuelve una fila por
   * equipo y bucket; el frontend arma las series. Se agrupa en SQL y no en
   * memoria porque el rango puede abarcar un ano entero.
   */
  async getCebadoSeries(query: {
    desde?: string;
    hasta?: string;
    equipo_id?: string;
    granularidad?: string;
  }) {
    const { desde, hasta } = this.resolvePeriod(query.desde, query.hasta);
    const equipoId = this.equipoParam(query.equipo_id);
    const bucket = this.resolveBucket(query.granularidad);

    const filas = await this.dataSource.query(
      `
      SELECT
        e.id AS equipo_id,
        e.codigo AS equipo_codigo,
        COALESCE(e.nombre, e.nombre_real) AS equipo_nombre,
        e.nombre_real AS equipo_descripcion,
        date_trunc($4, COALESCE(wo.hora_inicio, wo.created_at))::date AS periodo,
        ROUND(COALESCE(SUM(cr.cantidad), 0)::numeric, 2) AS galones,
        COUNT(DISTINCT wo.id) AS cebados
      FROM kpi_process.tb_work_order wo
      INNER JOIN kpi_maintenance.tb_equipo e ON e.id = wo.equipment_id
      INNER JOIN kpi_maintenance.tb_consumo_repuesto cr
        ON cr.work_order_id = wo.id AND COALESCE(cr.is_deleted, false) = false
      INNER JOIN kpi_inventory.tb_producto p
        ON p.id = cr.producto_id AND COALESCE(p.es_aceite, false) = true
      WHERE COALESCE(wo.is_deleted, false) = false
        AND UPPER(COALESCE(wo.maintenance_kind, '')) = 'CEBADO'
        AND COALESCE(wo.hora_inicio, wo.created_at) BETWEEN $1::timestamp AND $2::timestamp
        AND ($3::uuid IS NULL OR wo.equipment_id = $3::uuid)
      GROUP BY e.id, e.codigo, equipo_nombre, equipo_descripcion, periodo
      ORDER BY periodo, e.codigo
      `,
      [desde, hasta, equipoId, bucket],
    );

    return this.wrap(
      { periodo: { desde, hasta }, granularidad: bucket, filas },
      'Serie de consumo de cebado generada',
    );
  }

  /** Traduce la granularidad publica al argumento de `date_trunc`. */
  private resolveBucket(valor?: string) {
    const v = String(valor ?? '').trim().toLowerCase();
    if (v === 'semana' || v === 'week') return 'week';
    if (v === 'anio' || v === 'ano' || v === 'year') return 'year';
    return 'month';
  }

  /**
   * Detalle que sustenta un bloque del resumen.
   *
   * Devuelve los registros concretos que produjeron la cifra, para que se pueda
   * ver de donde sale cada numero en lugar de tener que fiarse del agregado.
   *
   * Todos los bloques se ordenan de la intervencion mas reciente a la mas
   * antigua, y el codigo de orden desempata para que el resultado sea estable
   * entre llamadas. El grafico del detalle respeta ese mismo orden: si la tabla
   * y el grafico se leyeran al reves uno del otro, comparar seria enganoso.
   */
  async getDetalle(query: {
    bloque?: string;
    equipo_id?: string;
    desde?: string;
    hasta?: string;
  }) {
    const { desde, hasta } = this.resolvePeriod(query.desde, query.hasta);
    const equipoId = this.equipoParam(query.equipo_id);
    const bloque = String(query.bloque ?? '').trim().toLowerCase();

    if (bloque === 'aceite') {
      const filas = await this.dataSource.query(
        `
        SELECT wo.id AS work_order_id, wo.code AS orden, wo.equipment_id,
          wo.scheduled_start AS fecha_programada,
          COALESCE(wo.hora_inicio, wo.created_at)::date AS fecha,
          wo.maintenance_kind AS tipo,
          COALESCE(e.nombre, e.nombre_real) AS equipo_nombre,
          CASE WHEN wo.equipment_location_snapshot IS NOT NULL THEN (wo.equipment_location_snapshot->>'location_id')::uuid ELSE e.location_id END AS location_id,
        CASE WHEN wo.equipment_location_snapshot IS NOT NULL THEN CONCAT_WS(' - ', wo.equipment_location_snapshot->>'location_code', wo.equipment_location_snapshot->>'location_name', wo.equipment_location_snapshot->>'sucursal_name') ELSE CONCAT_WS(' - ', l.codigo, l.nombre) END AS central,
          ROUND(SUM(cr.cantidad)::numeric, 2) AS galones,
          ROUND(SUM(cr.cantidad * COALESCE(issued.costo_unitario, cr.costo_unitario, 0))::numeric, 2) AS costo
        FROM kpi_process.tb_work_order wo
        INNER JOIN kpi_maintenance.tb_consumo_repuesto cr
          ON cr.work_order_id = wo.id AND COALESCE(cr.is_deleted, false) = false
        INNER JOIN kpi_inventory.tb_producto p
          ON p.id = cr.producto_id AND COALESCE(p.es_aceite, false) = true
        LEFT JOIN kpi_maintenance.tb_equipo e ON e.id = wo.equipment_id
        LEFT JOIN kpi_maintenance.tb_location l ON l.id = e.location_id
          AND COALESCE(l.is_deleted, false) = false
        LEFT JOIN LATERAL (
          SELECT SUM(d.cantidad * d.costo_unitario) / NULLIF(SUM(d.cantidad), 0) AS costo_unitario
          FROM kpi_inventory.tb_entrega_material em
          INNER JOIN kpi_inventory.tb_entrega_material_det d ON d.entrega_id = em.id
          WHERE em.work_order_id = wo.id AND COALESCE(em.is_deleted, false) = false
            AND d.producto_id = cr.producto_id AND d.bodega_id = cr.bodega_id
        ) issued ON true
        WHERE COALESCE(wo.is_deleted, false) = false
          AND UPPER(COALESCE(wo.valor_json ->> 'approval_action', '')) NOT IN ('ANULADA', 'ANULADO', 'CANCELADA', 'CANCELADO')
          AND UPPER(COALESCE(wo.status_workflow, '')) NOT IN ('ANULADA', 'ANULADO', 'CANCELLED', 'CANCELED', 'VOID', 'VOIDED')
          AND COALESCE(wo.hora_inicio, wo.created_at) BETWEEN $1::timestamp AND $2::timestamp
          AND ($3::uuid IS NULL OR wo.equipment_id = $3::uuid)
        GROUP BY wo.id, e.id, l.id
        ORDER BY fecha, wo.code
        `,
        [desde, hasta, equipoId],
      );
      return this.wrap({ bloque, filas }, 'Detalle de consumo de aceite');
    }

    if (bloque === 'cebado') {
      // Una fila por orden, no por linea de consumo: el semaforo mide lo que
      // gasto la OT completa, y una orden puede tener varias lineas de aceite.
      const filas = await this.dataSource.query(
        `
        SELECT
          wo.id AS work_order_id,
          wo.code AS orden,
          wo.title AS titulo,
          wo.equipment_id,
          wo.started_at, wo.closed_at,
          (SELECT jsonb_agg(jsonb_build_object('changed_at', h.changed_at, 'from_status', h.from_status,
            'to_status', h.to_status) ORDER BY h.changed_at, h.id)
            FROM kpi_process.tb_work_order_status_history h WHERE h.work_order_id = wo.id) AS execution_history,
          COALESCE(wo.hora_inicio, wo.created_at)::date AS fecha,
          COALESCE(wo.hora_inicio, wo.created_at) AS momento,
          -- El horometro con el que llego la maquina y el que se anoto en el
          -- cebado. Sin los dos no se sabe cuantas horas corrio entre uno y
          -- otro, que es lo que explica si el consumo fue alto o normal.
          NULLIF(TRIM(wo.valor_json ->> 'horometro_anterior'), '')::numeric AS horometro_anterior,
          NULLIF(TRIM(wo.valor_json ->> 'horometro_actual'), '')::numeric AS horometro_actual,
          string_agg(DISTINCT
            TRIM(CONCAT_WS(' - ', NULLIF(TRIM(p.codigo), ''), NULLIF(TRIM(p.nombre), ''))
               || COALESCE(' (' || NULLIF(TRIM(p.descripcion), '') || ')', ''))
          , ', ') AS producto,
          ROUND(SUM(cr.cantidad)::numeric, 2) AS galones,
          ROUND(SUM(COALESCE(cr.subtotal, 0))::numeric, 2) AS costo,
          COUNT(cr.id) AS lineas
        FROM kpi_process.tb_work_order wo
        INNER JOIN kpi_maintenance.tb_consumo_repuesto cr
          ON cr.work_order_id = wo.id AND COALESCE(cr.is_deleted, false) = false
        INNER JOIN kpi_inventory.tb_producto p
          ON p.id = cr.producto_id AND COALESCE(p.es_aceite, false) = true
        WHERE COALESCE(wo.is_deleted, false) = false
          AND UPPER(COALESCE(wo.maintenance_kind, '')) = 'CEBADO'
          AND COALESCE(wo.hora_inicio, wo.created_at) BETWEEN $1::timestamp AND $2::timestamp
          AND ($3::uuid IS NULL OR wo.equipment_id = $3::uuid)
        GROUP BY wo.id, wo.code, wo.title, wo.equipment_id, fecha, momento,
                 horometro_anterior, horometro_actual
        ORDER BY momento DESC, wo.code DESC
        `,
        [desde, hasta, equipoId],
      );

      // La tendencia compara cada orden con la anterior del mismo equipo. Las
      // filas llegan de la mas reciente a la mas antigua, asi que la "anterior"
      // es la siguiente posicion dentro del grupo.
      const porEquipo = new Map<string, any[]>();
      for (const fila of filas) {
        const clave = String(fila.equipment_id ?? '');
        if (!porEquipo.has(clave)) porEquipo.set(clave, []);
        porEquipo.get(clave)!.push(fila);
      }
      for (const grupo of porEquipo.values()) {
        grupo.forEach((fila, indice) => {
          const actual = Number(fila.galones ?? 0);
          const anterior = grupo[indice + 1]
            ? Number(grupo[indice + 1].galones ?? 0)
            : null;
          fila.galones_orden_anterior = anterior;
          fila.horas_trabajadas = workOrderWorkedHours(fila, fila.execution_history || []);
          delete fila.execution_history;
          fila.horometro_anterior = normalizeHorometro(fila.horometro_anterior);
          fila.horometro_actual = normalizeHorometro(fila.horometro_actual);
          fila.tendencia = this.resolveTendenciaOrden(actual, anterior);
          fila.semaforo = this.resolveCebadoSemaforo(actual);
        });
      }

      return this.wrap({ bloque: 'cebado', filas }, 'Detalle de cebado');
    }

    if (bloque === 'repuestos') {
      const filas = await this.dataSource.query(
        `
        SELECT
          -- El identificador viaja para que el tablero pueda abrir la orden
          -- sin tener que buscarla por codigo.
          wo.id AS work_order_id,
          wo.code AS orden,
          COALESCE(wo.hora_inicio, wo.created_at)::date AS fecha,
          TRIM(CONCAT_WS(' - ', NULLIF(TRIM(p.codigo), ''), NULLIF(TRIM(p.nombre), ''))
               || COALESCE(' (' || NULLIF(TRIM(p.descripcion), '') || ')', '')) AS producto,
          ROUND(cr.cantidad::numeric, 2) AS cantidad,
          ROUND(COALESCE(cr.costo_unitario, 0)::numeric, 2) AS costo_unitario,
          ROUND(COALESCE(cr.subtotal, 0)::numeric, 2) AS costo
        FROM kpi_maintenance.tb_consumo_repuesto cr
        INNER JOIN kpi_process.tb_work_order wo
          ON wo.id = cr.work_order_id AND COALESCE(wo.is_deleted, false) = false
        INNER JOIN kpi_inventory.tb_producto p ON p.id = cr.producto_id
        WHERE COALESCE(cr.is_deleted, false) = false
          AND COALESCE(wo.hora_inicio, wo.created_at) BETWEEN $1::timestamp AND $2::timestamp
          AND ($3::uuid IS NULL OR wo.equipment_id = $3::uuid)
        ORDER BY fecha DESC, wo.code DESC, costo DESC
        `,
        [desde, hasta, equipoId],
      );
      return this.wrap({ bloque: 'repuestos', filas }, 'Detalle de repuestos');
    }

    if (bloque === 'correctivos') {
      const filas = await this.dataSource.query(
        `
        SELECT
          wo.id AS work_order_id,
          wo.code AS orden,
          wo.title AS titulo,
          COALESCE(c.nombre_oficial, c.nombre, 'Sin compartimiento') AS componente,
          COALESCE(wo.hora_inicio, wo.created_at)::date AS fecha,
          wo.hora_inicio,
          wo.hora_fin,
          ROUND(
            (EXTRACT(EPOCH FROM (wo.hora_fin - wo.hora_inicio)) / 3600.0)::numeric, 2
          ) AS horas,
          wo.status_workflow AS estado
        FROM kpi_process.tb_work_order wo
        LEFT JOIN kpi_maintenance.tb_equipo_componente c ON c.id = wo.equipo_componente_id
        WHERE COALESCE(wo.is_deleted, false) = false
          AND UPPER(COALESCE(wo.maintenance_kind, '')) = 'CORRECTIVO'
          AND COALESCE(wo.hora_inicio, wo.created_at) BETWEEN $1::timestamp AND $2::timestamp
          AND ($3::uuid IS NULL OR wo.equipment_id = $3::uuid)
        ORDER BY fecha DESC, wo.code DESC
        `,
        [desde, hasta, equipoId],
      );
      return this.wrap({ bloque: 'correctivos', filas }, 'Detalle de correctivos');
    }

    if (bloque === 'disponibilidad') {
      const filas = await this.dataSource.query(
        `
        SELECT
          h.estado_anterior,
          h.estado_nuevo,
          h.estado_anterior_desde AS desde,
          h.changed_at AS hasta,
          ROUND((COALESCE(h.duracion_estado_anterior_segundos, 0) / 3600.0)::numeric, 2) AS horas,
          h.changed_by
        FROM kpi_maintenance.tb_equipo_funcionamiento_historial h
        WHERE h.changed_at BETWEEN $1::timestamp AND $2::timestamp
          AND ($3::uuid IS NULL OR h.equipo_id = $3::uuid)
        ORDER BY h.changed_at DESC
        `,
        [desde, hasta, equipoId],
      );
      return this.wrap({ bloque: 'disponibilidad', filas }, 'Detalle de disponibilidad');
    }

    return this.wrap({ bloque, filas: [] }, 'Bloque sin detalle disponible');
  }

  /** Resumen liviano para reutilizar solo el bloque de cebado. */
  async getCebadoSummary(query: {
    desde?: string;
    hasta?: string;
    equipo_id?: string;
  }) {
    const { desde, hasta } = this.resolvePeriod(query.desde, query.hasta);
    const equipoId = this.equipoParam(query.equipo_id);
    const cebado = await this.getCebado(desde, hasta, equipoId);
    return this.wrap(
      {
        periodo: { desde, hasta, equipo_id: equipoId },
        cebado,
      },
      'Control de cebado generado',
    );
  }

  /** Punto de entrada: devuelve los ocho bloques del dashboard. */
  async getDashboard(query: {
    desde?: string;
    hasta?: string;
    equipo_id?: string;
  }) {
    const { desde, hasta } = this.resolvePeriod(query.desde, query.hasta);
    const equipoId = this.equipoParam(query.equipo_id);

    const [disponibilidad, correctivos, cebado, repuestos, proyeccion, confiabilidad] =
      await Promise.all([
        this.getDisponibilidad(desde, hasta, equipoId),
        this.getCorrectivos(desde, hasta, equipoId),
        this.getCebado(desde, hasta, equipoId),
        this.getRepuestos(desde, hasta, equipoId),
        this.getProyeccion(equipoId),
        this.getConfiabilidad(desde, hasta, equipoId),
      ]);

    return this.wrap(
      {
        periodo: { desde, hasta, equipo_id: equipoId },
        disponibilidad,
        correctivos,
        cebado,
        repuestos,
        proyeccion,
        confiabilidad,
        resumen: this.buildResumen({
          disponibilidad,
          correctivos,
          cebado,
          repuestos,
          proyeccion,
          confiabilidad,
        }),
      },
      'Dashboard de administración generado',
    );
  }

  /**
   * Solo la proyección (una consulta), para que el tablero abierto resincronice
   * el horómetro en vivo cada pocos segundos sin recalcular los demás bloques.
   */
  async getProyeccionVigente(query: { equipo_id?: string }) {
    const proyeccion = await this.getProyeccion(this.equipoParam(query.equipo_id));
    return this.wrap(
      { proyeccion, ...this.contarMantenimientos(proyeccion) },
      'Proyección por horómetro generada',
    );
  }

  private contarMantenimientos(proyeccion: any[]) {
    return {
      mantenimientos_vencidos: proyeccion.filter(
        (row) => row.semaforo?.nivel === 'ROJO',
      ).length,
      mantenimientos_proximos: proyeccion.filter(
        (row) => row.semaforo?.nivel === 'AMARILLO',
      ).length,
    };
  }

  /** Cabecera de indicadores agregados para la parte superior de la pantalla. */
  private buildResumen(input: {
    disponibilidad: any[];
    correctivos: { por_equipo: any[]; reincidencias: any[] };
    cebado: any[];
    repuestos: any[];
    proyeccion: any[];
    confiabilidad: any[];
  }) {
    const conDatos = input.disponibilidad.filter(
      (row) => row.porcentaje_disponibilidad != null,
    );
    const disponibilidadMedia = conDatos.length
      ? Number(
          (
            conDatos.reduce(
              (sum, row) => sum + Number(row.porcentaje_disponibilidad),
              0,
            ) / conDatos.length
          ).toFixed(2),
        )
      : null;

    const promedio = (rows: any[], campo: string) => {
      const valores = rows
        .map((row) => Number(row[campo]))
        .filter((value) => Number.isFinite(value));
      if (!valores.length) return null;
      return Number(
        (valores.reduce((a, b) => a + b, 0) / valores.length).toFixed(2),
      );
    };

    return {
      disponibilidad_media: disponibilidadMedia,
      equipos_evaluados: input.disponibilidad.length,
      total_correctivos: input.correctivos.por_equipo.reduce(
        (sum, row) => sum + Number(row.total_correctivos ?? 0),
        0,
      ),
      equipos_con_reincidencia: input.correctivos.reincidencias.length,
      galones_cebado: Number(
        input.cebado
          .reduce((sum, row) => sum + Number(row.galones_periodo ?? 0), 0)
          .toFixed(2),
      ),
      ordenes_cebado_criticas: input.cebado.reduce(
        (sum, row) => sum + Number(row.ots_criticas ?? 0),
        0,
      ),
      ordenes_cebado_seguimiento: input.cebado.reduce(
        (sum, row) => sum + Number(row.ots_seguimiento ?? 0),
        0,
      ),
      costo_repuestos: Number(
        input.repuestos
          .reduce((sum, row) => sum + Number(row.costo ?? 0), 0)
          .toFixed(2),
      ),
      ...this.contarMantenimientos(input.proyeccion),
      mttr_medio: promedio(input.confiabilidad, 'mttr_horas'),
      mtbf_medio: promedio(input.confiabilidad, 'mtbf_horas'),
    };
  }
}
