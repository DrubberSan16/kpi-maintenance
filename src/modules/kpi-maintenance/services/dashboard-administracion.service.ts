import { Injectable, Logger } from '@nestjs/common';
import { InjectDataSource } from '@nestjs/typeorm';
import { DataSource } from 'typeorm';

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
@Injectable()
export class DashboardAdministracionService {
  private readonly logger = new Logger(DashboardAdministracionService.name);

  /** Umbrales de galones para el semáforo de cebado (punto 3). */
  private readonly CEBADO_VERDE_MAX = 5;
  private readonly CEBADO_AMARILLO_MAX = 10;

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
          COALESCE(wo.hora_inicio, wo.created_at) AS momento,
          SUM(cr.cantidad) AS galones,
          SUM(COALESCE(cr.subtotal, 0)) AS costo
        FROM kpi_process.tb_work_order wo
        INNER JOIN kpi_maintenance.tb_consumo_repuesto cr
          ON cr.work_order_id = wo.id AND COALESCE(cr.is_deleted, false) = false
        INNER JOIN kpi_inventory.tb_producto p
          ON p.id = cr.producto_id AND COALESCE(p.es_aceite, false) = true
        WHERE COALESCE(wo.is_deleted, false) = false
          AND UPPER(COALESCE(wo.maintenance_kind, '')) = 'CEBADO'
          AND COALESCE(wo.hora_inicio, wo.created_at) BETWEEN $1::timestamp AND $2::timestamp
          AND ($3::uuid IS NULL OR wo.equipment_id = $3::uuid)
        GROUP BY wo.id, wo.equipment_id, momento
      )
      SELECT
        e.id AS equipo_id,
        e.codigo AS equipo_codigo,
        COALESCE(e.nombre, e.nombre_real) AS equipo_nombre,
        e.nombre_real AS equipo_descripcion,
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
        ROUND(COALESCE(SUM(o.costo), 0)::numeric, 2) AS costo_aceite
      FROM por_orden o
      INNER JOIN kpi_maintenance.tb_equipo e ON e.id = o.equipment_id
      GROUP BY e.id, e.codigo, equipo_nombre, equipo_descripcion
      ORDER BY ots_criticas DESC, galones_periodo DESC, e.codigo
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
      return { nivel: 'ROJO', etiqueta: 'Consumo anormal', detalle: '10 galones o más' };
    }
    if (galones > this.CEBADO_VERDE_MAX) {
      return { nivel: 'AMARILLO', etiqueta: 'Seguimiento', detalle: 'Entre 5 y 10 galones' };
    }
    return { nivel: 'VERDE', etiqueta: 'Normal', detalle: 'Hasta 5 galones' };
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
   * El horómetro del último mantenimiento se toma de la última OT preventiva o
   * de cebado cerrada del equipo; si no hay ninguna, se cae al horómetro actual
   * menos la frecuencia, para no dejar el equipo fuera de la proyección.
   *
   * Los umbrales salen de los márgenes configurables de cada unidad, de modo
   * que MTU 500 h, Cummins 350 h y Caterpillar 250 h se resuelven solas.
   */
  private async getProyeccion(equipoId: string | null) {
    const filas = await this.dataSource.query(
      `
      WITH ultimo_mant AS (
        SELECT DISTINCT ON (wo.equipment_id)
          wo.equipment_id,
          wo.id AS work_order_id,
          wo.code AS work_order_code,
          COALESCE(wo.hora_fin, wo.closed_at)::date AS fecha,
          NULLIF((wo.valor_json ->> 'horometro_actual'), '')::numeric AS horometro
        FROM kpi_process.tb_work_order wo
        WHERE COALESCE(wo.is_deleted, false) = false
          AND UPPER(COALESCE(wo.status_workflow, '')) IN ('CLOSED', 'CERRADA', 'CERRADO')
          AND UPPER(COALESCE(wo.maintenance_kind, '')) IN ('PREVENTIVO', 'CEBADO')
        ORDER BY wo.equipment_id, COALESCE(wo.hora_fin, wo.closed_at) DESC
      )
      SELECT
        e.id AS equipo_id,
        e.codigo AS equipo_codigo,
        COALESCE(e.nombre, e.nombre_real) AS equipo_nombre,
        e.nombre_real AS equipo_descripcion,
        m.nombre AS marca,
        e.horometro_actual::numeric AS horometro_actual,
        e.intervalo_mantenimiento_valor::numeric AS frecuencia,
        e.intervalo_mantenimiento_unidad AS frecuencia_unidad,
        e.margen_anticipacion_pct::numeric AS margen_anticipacion_pct,
        e.margen_tolerancia_pct::numeric AS margen_tolerancia_pct,
        u.horometro AS horometro_ultimo_mantenimiento,
        u.fecha AS fecha_ultimo_mantenimiento,
        u.work_order_code AS ultima_ot
      FROM kpi_maintenance.tb_equipo e
      LEFT JOIN kpi_inventory.tb_marca m ON m.id = e.marca_id
      LEFT JOIN ultimo_mant u ON u.equipment_id = e.id
      WHERE COALESCE(e.is_deleted, false) = false
        AND UPPER(COALESCE(e.status, 'ACTIVE')) = 'ACTIVE'
        AND ($1::uuid IS NULL OR e.id = $1::uuid)
      ORDER BY e.codigo
      `,
      [equipoId],
    );

    return filas
      .map((row: any) => {
        const frecuencia = Number(row.frecuencia ?? 0);
        const horometroActual = Number(row.horometro_actual ?? 0);
        // Sin frecuencia configurada no hay proyeccion posible.
        if (!Number.isFinite(frecuencia) || frecuencia <= 0) {
          return { ...row, aplica: false, semaforo: null };
        }
        const base = Number(
          row.horometro_ultimo_mantenimiento ?? horometroActual - frecuencia,
        );
        const objetivo = Number((base + frecuencia).toFixed(2));
        const restantes = Number((objetivo - horometroActual).toFixed(2));

        const anticipacion = (frecuencia * Number(row.margen_anticipacion_pct ?? 10)) / 100;
        const tolerancia = (frecuencia * Number(row.margen_tolerancia_pct ?? 0)) / 100;

        let semaforo: { nivel: string; etiqueta: string };
        if (restantes < -tolerancia) {
          semaforo = { nivel: 'ROJO', etiqueta: 'Superó la tolerancia' };
        } else if (restantes <= anticipacion) {
          semaforo = { nivel: 'AMARILLO', etiqueta: 'Próximo a mantenimiento' };
        } else {
          semaforo = { nivel: 'VERDE', etiqueta: 'Dentro de frecuencia' };
        }

        return {
          ...row,
          aplica: true,
          horometro_ultimo_mantenimiento: Number(base.toFixed(2)),
          horometro_proximo_mantenimiento: objetivo,
          horas_restantes: restantes,
          horas_excedidas: restantes < 0 ? Math.abs(restantes) : 0,
          umbral_amarillo: Number(anticipacion.toFixed(2)),
          umbral_rojo: Number(tolerancia.toFixed(2)),
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
          COALESCE(wo.hora_inicio, wo.created_at)::date AS fecha,
          COALESCE(wo.hora_inicio, wo.created_at) AS momento,
          string_agg(DISTINCT p.nombre, ', ') AS producto,
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
        GROUP BY wo.id, wo.code, wo.title, wo.equipment_id, fecha, momento
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
          wo.code AS orden,
          COALESCE(wo.hora_inicio, wo.created_at)::date AS fecha,
          p.nombre AS producto,
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
      mantenimientos_vencidos: input.proyeccion.filter(
        (row) => row.semaforo?.nivel === 'ROJO',
      ).length,
      mantenimientos_proximos: input.proyeccion.filter(
        (row) => row.semaforo?.nivel === 'AMARILLO',
      ).length,
      mttr_medio: promedio(input.confiabilidad, 'mttr_horas'),
      mtbf_medio: promedio(input.confiabilidad, 'mtbf_horas'),
    };
  }
}
