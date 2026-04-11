import { Column, Entity, PrimaryGeneratedColumn } from 'typeorm';

@Entity({ schema: 'kpi_maintenance', name: 'tb_equipo' })
export class EquipoEntity {
  @PrimaryGeneratedColumn('uuid') id: string;
  @Column() codigo: string;
  @Column() nombre: string;
  @Column({ type: 'text', nullable: true }) nombre_real?: string | null;
  @Column({ type: 'text', nullable: true }) modelo?: string | null;
  @Column({ type: 'text', nullable: true }) codigo_lubricante?: string | null;
  @Column() equipo_tipo_id: string;
  @Column({ type: 'uuid', nullable: true }) location_id?: string | null;
  @Column({ default: 'MEDIA' }) criticidad: string;
  @Column({ default: 'OPERATIVO' }) estado_operativo: string;
  @Column('numeric', { precision: 18, scale: 2, default: 0 })
  horometro_actual: number;
  @Column({ type: 'timestamp without time zone', nullable: true })
  fecha_ultima_lectura?: Date | null;
  @Column({ default: 'ACTIVE' }) status: string;
  @Column({ type: 'timestamp without time zone', default: () => 'now()' })
  created_at: Date;
  @Column({ type: 'timestamp without time zone', default: () => 'now()' })
  updated_at: Date;
  @Column({ type: 'text', nullable: true }) created_by?: string | null;
  @Column({ type: 'text', nullable: true }) updated_by?: string | null;
  @Column({ default: false }) is_deleted: boolean;
  @Column({ type: 'timestamp without time zone', nullable: true })
  deleted_at?: Date | null;
  @Column({ type: 'text', nullable: true }) deleted_by?: string | null;
  @Column() marca_id: string;
}

@Entity({ schema: 'kpi_maintenance', name: 'tb_equipo_componente' })
export class EquipoComponenteEntity {
  @PrimaryGeneratedColumn('uuid') id: string;
  @Column({ type: 'uuid' }) equipo_id: string;
  @Column({ type: 'uuid', nullable: true }) parent_id?: string | null;
  @Column({ type: 'text', nullable: true }) codigo?: string | null;
  @Column() nombre: string;
  @Column({ type: 'text', nullable: true }) nombre_oficial?: string | null;
  @Column({ type: 'text', nullable: true }) categoria?: string | null;
  @Column({ type: 'integer', default: 1 }) orden: number;
  @Column({ type: 'text', nullable: true }) descripcion?: string | null;
  @Column({ default: 'ACTIVE' }) status: string;
  @Column({ default: false }) is_deleted: boolean;
}

@Entity({ schema: 'kpi_inventory', name: 'tb_marca' })
export class MarcaEntity {
  @PrimaryGeneratedColumn('uuid') id: string;
  @Column({ type: 'text' }) nombre: string;
  @Column({ default: 'ACTIVE' }) status: string;
  @Column({ default: false }) is_deleted: boolean;
}

@Entity({ schema: 'kpi_maintenance', name: 'tb_location' })
export class LocationEntity {
  @PrimaryGeneratedColumn('uuid') id: string;
  @Column({ type: 'text' }) codigo: string;
  @Column({ type: 'text' }) nombre: string;
  @Column({ type: 'uuid', nullable: true }) sucursal_id?: string | null;
  @Column({ type: 'text', nullable: true }) descripcion?: string | null;
  @Column({ default: 'ACTIVE' }) status: string;
  @Column({ default: false }) is_deleted: boolean;
  @Column({ type: 'timestamp without time zone', nullable: true })
  deleted_at?: Date | null;
}

@Entity({ schema: 'kpi_maintenance', name: 'tb_equipo_tipo' })
export class EquipoTipoEntity {
  @PrimaryGeneratedColumn('uuid') id: string;
  @Column() codigo: string;
  @Column() nombre: string;
  @Column({ type: 'text', nullable: true }) descripcion: string;
  @Column({ default: 'ACTIVE' }) status: string;
  @Column({ type: 'timestamp without time zone', default: () => 'now()' })
  created_at: Date;
  @Column({ type: 'timestamp without time zone', default: () => 'now()' })
  updated_at: Date;
  @Column({ type: 'text', nullable: true }) created_by?: string | null;
  @Column({ type: 'text', nullable: true }) updated_by?: string | null;
  @Column({ default: false }) is_deleted: boolean;
  @Column({ type: 'timestamp without time zone', nullable: true })
  deleted_at?: Date | null;
  @Column({ type: 'text', nullable: true }) deleted_by?: string | null;
}

@Entity({ schema: 'kpi_maintenance', name: 'tb_bitacora_diaria' })
export class BitacoraDiariaEntity {
  @PrimaryGeneratedColumn('uuid') id: string;
  @Column() equipo_id: string;
  @Column({ type: 'date' }) fecha: string;
  @Column('numeric', { precision: 18, scale: 2 }) horometro: number;
  @Column({ type: 'uuid', nullable: true }) estado_id?: string | null;
  @Column({ type: 'text', nullable: true }) observaciones?: string | null;
  @Column({ type: 'text', nullable: true }) registrado_por?: string | null;
  @Column({ default: 'ACTIVE' }) status: string;
  @Column({ type: 'timestamp without time zone', default: () => 'now()' })
  created_at: Date;
  @Column({ type: 'timestamp without time zone', default: () => 'now()' })
  updated_at: Date;
  @Column({ type: 'text', nullable: true }) created_by?: string | null;
  @Column({ type: 'text', nullable: true }) updated_by?: string | null;
  @Column({ default: false }) is_deleted: boolean;
}

@Entity({ schema: 'kpi_maintenance', name: 'tb_estado_equipo_catalogo' })
export class EstadoEquipoCatalogoEntity {
  @PrimaryGeneratedColumn('uuid') id: string;
  @Column() codigo: string;
  @Column({ type: 'text', nullable: true }) descripcion?: string | null;
  @Column({ default: 'ACTIVE' }) status: string;
  @Column({ default: false }) is_deleted: boolean;
}

@Entity({ schema: 'kpi_maintenance', name: 'tb_estado_equipo' })
export class EstadoEquipoEntity {
  @PrimaryGeneratedColumn('uuid') id: string;
  @Column({ type: 'uuid' }) equipo_id: string;
  @Column({ type: 'uuid' }) estado_id: string;
  @Column({ type: 'timestamp without time zone', default: () => 'now()' })
  fecha_inicio: Date;
  @Column({ type: 'timestamp without time zone', nullable: true })
  fecha_fin?: Date | null;
  @Column({ type: 'text', nullable: true }) motivo?: string | null;
  @Column({ default: 'ACTIVE' }) status: string;
  @Column({ default: false }) is_deleted: boolean;
}

@Entity({ schema: 'kpi_maintenance', name: 'tb_evento_equipo' })
export class EventoEquipoEntity {
  @PrimaryGeneratedColumn('uuid') id: string;
  @Column({ type: 'uuid' }) equipo_id: string;
  @Column({ type: 'uuid', nullable: true }) work_order_id?: string | null;
  @Column() tipo_evento: string;
  @Column({ type: 'timestamp without time zone', default: () => 'now()' })
  fecha_inicio: Date;
  @Column({ type: 'timestamp without time zone', nullable: true })
  fecha_fin?: Date | null;
  @Column({ default: 3 }) severidad: number;
  @Column({ type: 'text', nullable: true }) descripcion?: string | null;
  @Column({ default: 'ACTIVE' }) status: string;
  @Column({ default: false }) is_deleted: boolean;
}

@Entity({ schema: 'kpi_maintenance', name: 'tb_falla_catalogo' })
export class FallaCatalogoEntity {
  @PrimaryGeneratedColumn('uuid') id: string;
  @Column({ type: 'text', nullable: true }) codigo?: string | null;
  @Column({ type: 'text' }) sintoma: string;
  @Column({ type: 'text', nullable: true }) causa?: string | null;
  @Column({ type: 'text', nullable: true }) accion_recomendada?: string | null;
  @Column({ type: 'uuid', nullable: true }) equipo_tipo_id?: string | null;
  @Column({ default: 'MEDIA' }) severidad: string;
  @Column({ default: 'ACTIVE' }) status: string;
  @Column({ default: false }) is_deleted: boolean;
}

@Entity({ schema: 'kpi_maintenance', name: 'tb_lectura_equipo' })
export class LecturaEquipoEntity {
  @PrimaryGeneratedColumn('uuid') id: string;
  @Column({ type: 'uuid' }) equipo_id: string;
  @Column({ type: 'timestamp without time zone', default: () => 'now()' })
  fecha: Date;
  @Column() tipo: string;
  @Column('numeric', { precision: 18, scale: 4, nullable: true }) valor?:
    | number
    | null;
  @Column({ type: 'text', nullable: true }) valor_texto?: string | null;
  @Column({ type: 'text', nullable: true }) unidad?: string | null;
  @Column({ type: 'uuid', nullable: true }) tomado_por?: string | null;
  @Column({ type: 'text', nullable: true }) observacion?: string | null;
  @Column({ default: 'ACTIVE' }) status: string;
  @Column({ default: false }) is_deleted: boolean;
}

@Entity({ schema: 'kpi_maintenance', name: 'tb_lubricacion_punto' })
export class LubricacionPuntoEntity {
  @PrimaryGeneratedColumn('uuid') id: string;
  @Column({ type: 'uuid' }) equipo_id: string;
  @Column({ type: 'uuid', nullable: true }) componente_id?: string | null;
  @Column() nombre: string;
  @Column({ type: 'text', nullable: true }) lubricante?: string | null;
  @Column('numeric', { precision: 18, scale: 4, nullable: true })
  cantidad_recomendada?: number | null;
  @Column({ type: 'text', nullable: true }) unidad?: string | null;
  @Column({ default: 'HORAS' }) frecuencia_tipo: string;
  @Column({ type: 'integer', default: 0 }) frecuencia_valor: number;
  @Column({ default: 'ACTIVE' }) status: string;
  @Column({ default: false }) is_deleted: boolean;
}

@Entity({ schema: 'kpi_maintenance', name: 'tb_plan_mantenimiento' })
export class PlanMantenimientoEntity {
  @PrimaryGeneratedColumn('uuid') id: string;
  @Column() codigo: string;
  @Column() nombre: string;
  @Column({ default: 'PREVENTIVO' }) tipo: string;
  @Column({ type: 'text', nullable: true }) descripcion?: string | null;
  @Column({ default: 'HORAS' }) frecuencia_tipo: string;
  @Column({ default: 0 }) frecuencia_valor: number;
  @Column({ default: false }) requiere_parada: boolean;
  @Column({ default: 'ACTIVE' }) status: string;
  @Column({ default: false }) is_deleted: boolean;
}

@Entity({ schema: 'kpi_maintenance', name: 'tb_plan_tarea' })
export class PlanTareaEntity {
  @PrimaryGeneratedColumn('uuid') id: string;
  @Column({ type: 'uuid' }) plan_id: string;
  @Column({ default: 1 }) orden: number;
  @Column({ type: 'text' }) actividad: string;
  @Column({ default: 'BOOLEAN' }) field_type: string;
  @Column({ default: false }) required: boolean;
  @Column({ type: 'jsonb', default: () => "'{}'::jsonb" }) meta: Record<
    string,
    unknown
  >;
  @Column({ default: 'ACTIVE' }) status: string;
  @Column({ default: false }) is_deleted: boolean;
}

@Entity({ schema: 'kpi_maintenance', name: 'tb_programacion_plan' })
export class ProgramacionPlanEntity {
  @PrimaryGeneratedColumn('uuid') id: string;
  @Column({ type: 'text', nullable: true }) codigo?: string | null;
  @Column({ type: 'uuid' }) equipo_id: string;
  @Column({ type: 'uuid' }) plan_id: string;
  @Column({ type: 'text', default: 'DINAMICA' }) modo_programacion: string;
  @Column({ type: 'text', default: 'MANUAL' }) origen_programacion: string;
  @Column({ type: 'date', nullable: true }) ultima_ejecucion_fecha?:
    | string
    | null;
  @Column('numeric', { precision: 18, scale: 2, nullable: true })
  ultima_ejecucion_horas?: number | null;
  @Column({ type: 'date', nullable: true }) proxima_fecha?: string | null;
  @Column('numeric', { precision: 18, scale: 2, nullable: true })
  proxima_horas?: number | null;
  @Column({ type: 'text', nullable: true }) documento_origen?: string | null;
  @Column({ type: 'jsonb', default: () => "'{}'::jsonb" }) payload_json: Record<
    string,
    unknown
  >;
  @Column({ default: true }) activo: boolean;
  @Column({ default: 'ACTIVE' }) status: string;
  @Column({ default: false }) is_deleted: boolean;
}

@Entity({ schema: 'kpi_maintenance', name: 'tb_alerta_mantenimiento' })
export class AlertaMantenimientoEntity {
  @PrimaryGeneratedColumn('uuid') id: string;
  @Column({ type: 'uuid', nullable: true }) equipo_id?: string | null;
  @Column() tipo_alerta: string;
  @Column({ default: 'MANTENIMIENTO' }) categoria: string;
  @Column({ default: 'WARNING' }) nivel: string;
  @Column({ default: 'SYSTEM' }) origen: string;
  @Column({ type: 'text', nullable: true }) referencia_tipo?: string | null;
  @Column({ type: 'timestamp without time zone', default: () => 'now()' })
  fecha_generada: Date;
  @Column({ type: 'timestamp without time zone', default: () => 'now()' })
  ultima_evaluacion_at: Date;
  @Column({ type: 'timestamp without time zone', nullable: true })
  resolved_at?: Date | null;
  @Column({ default: 'ABIERTA' }) estado: string;
  @Column({ type: 'text', nullable: true }) detalle?: string | null;
  @Column({ type: 'text', nullable: true }) referencia?: string | null;
  @Column({ type: 'jsonb', default: () => "'{}'::jsonb" }) payload_json: Record<
    string,
    unknown
  >;
  @Column({ type: 'uuid', nullable: true }) work_order_id?: string | null;
  @Column({ default: 'ACTIVE' }) status: string;
  @Column({ default: false }) is_deleted: boolean;
}

@Entity({ schema: 'kpi_maintenance', name: 'tb_consumo_repuesto' })
export class ConsumoRepuestoEntity {
  @PrimaryGeneratedColumn('uuid') id: string;
  @Column({ type: 'uuid' }) work_order_id: string;
  @Column({ type: 'uuid' }) producto_id: string;
  @Column({ type: 'uuid', nullable: true }) bodega_id?: string | null;
  @Column('numeric', { precision: 18, scale: 6, default: 0 }) cantidad: number;
  @Column('numeric', { precision: 14, scale: 4, default: 0 })
  costo_unitario: number;
  @Column('numeric', { precision: 18, scale: 4, default: 0 }) subtotal: number;
  @Column({ type: 'text', nullable: true }) observacion?: string | null;
  @Column({ default: false }) is_deleted: boolean;
}

@Entity({ schema: 'kpi_maintenance', name: 'tb_work_order_tarea' })
export class WorkOrderTareaEntity {
  @PrimaryGeneratedColumn('uuid') id: string;
  @Column({ type: 'uuid' }) work_order_id: string;
  @Column({ type: 'uuid' }) plan_id: string;
  @Column({ type: 'uuid' }) tarea_id: string;
  @Column({ type: 'boolean', nullable: true }) valor_boolean?: boolean | null;
  @Column('numeric', { precision: 18, scale: 4, nullable: true })
  valor_numeric?: number | null;
  @Column({ type: 'text', nullable: true }) valor_text?: string | null;
  @Column({ type: 'jsonb', nullable: true }) valor_json?: Record<
    string,
    unknown
  > | null;
  @Column({ type: 'text', nullable: true }) observacion?: string | null;
  @Column({ default: 'ACTIVE' }) status: string;
  @Column({ default: false }) is_deleted: boolean;
}

@Entity({ schema: 'kpi_maintenance', name: 'tb_work_order_adjunto' })
export class WorkOrderAdjuntoEntity {
  @PrimaryGeneratedColumn('uuid') id: string;
  @Column({ type: 'uuid' }) work_order_id: string;
  @Column({ default: 'EVIDENCIA' }) tipo: string;
  @Column({ type: 'text', nullable: true }) nombre?: string | null;
  @Column({ type: 'text' }) url: string;
  @Column({ type: 'text', nullable: true }) hash_sha256?: string | null;
  @Column({ type: 'jsonb', default: () => "'{}'::jsonb" }) meta: Record<
    string,
    unknown
  >;
  @Column({ default: 'ACTIVE' }) status: string;
  @Column({ default: false }) is_deleted: boolean;
}

@Entity({ schema: 'kpi_maintenance', name: 'tb_procedimiento_plantilla' })
export class ProcedimientoPlantillaEntity {
  @PrimaryGeneratedColumn('uuid') id: string;
  @Column() codigo: string;
  @Column() nombre: string;
  @Column() tipo_proceso: string;
  @Column({ type: 'uuid', nullable: true }) bodega_id?: string | null;
  @Column({ type: 'text', nullable: true }) compartimiento_codigo_referencia?: string | null;
  @Column({ type: 'text', nullable: true }) compartimiento_nombre_oficial?: string | null;
  @Column({ type: 'text', nullable: true }) documento_referencia?: string | null;
  @Column({ type: 'text', nullable: true }) version?: string | null;
  @Column({ type: 'text', nullable: true }) clase_mantenimiento?: string | null;
  @Column({ type: 'integer', nullable: true }) frecuencia_horas?: number | null;
  @Column({ type: 'text', nullable: true }) objetivo?: string | null;
  @Column({ type: 'jsonb', default: () => "'[]'::jsonb" }) precauciones: string[];
  @Column({ type: 'jsonb', default: () => "'[]'::jsonb" }) herramientas: string[];
  @Column({ type: 'jsonb', default: () => "'[]'::jsonb" }) materiales: string[];
  @Column({ type: 'jsonb', default: () => "'[]'::jsonb" }) responsabilidades: string[];
  @Column({ default: 'ACTIVE' }) status: string;
  @Column({ type: 'timestamp without time zone', default: () => 'now()' }) created_at: Date;
  @Column({ type: 'timestamp without time zone', default: () => 'now()' }) updated_at: Date;
  @Column({ type: 'text', nullable: true }) created_by?: string | null;
  @Column({ type: 'text', nullable: true }) updated_by?: string | null;
  @Column({ default: false }) is_deleted: boolean;
}

@Entity({ schema: 'kpi_maintenance', name: 'tb_procedimiento_actividad' })
export class ProcedimientoActividadEntity {
  @PrimaryGeneratedColumn('uuid') id: string;
  @Column({ type: 'uuid' }) procedimiento_id: string;
  @Column({ default: 1 }) orden: number;
  @Column({ type: 'text', nullable: true }) fase?: string | null;
  @Column() actividad: string;
  @Column({ type: 'text', nullable: true }) detalle?: string | null;
  @Column({ default: false }) requiere_permiso: boolean;
  @Column({ default: false }) requiere_epp: boolean;
  @Column({ default: false }) requiere_bloqueo: boolean;
  @Column({ default: false }) requiere_evidencia: boolean;
  @Column({ type: 'jsonb', default: () => "'{}'::jsonb" }) meta: Record<
    string,
    unknown
  >;
  @Column({ default: 'ACTIVE' }) status: string;
  @Column({ type: 'timestamp without time zone', default: () => 'now()' }) created_at: Date;
  @Column({ type: 'timestamp without time zone', default: () => 'now()' }) updated_at: Date;
  @Column({ default: false }) is_deleted: boolean;
}

@Entity({ schema: 'kpi_maintenance', name: 'tb_analisis_lubricante' })
export class AnalisisLubricanteEntity {
  @PrimaryGeneratedColumn('uuid') id: string;
  @Column() codigo: string;
  @Column({ type: 'text', nullable: true }) cliente?: string | null;
  @Column({ type: 'uuid', nullable: true }) equipo_id?: string | null;
  @Column({ type: 'text', nullable: true }) lubricante?: string | null;
  @Column({ type: 'text', nullable: true }) marca_lubricante?: string | null;
  @Column({ type: 'text', nullable: true }) equipo_codigo?: string | null;
  @Column({ type: 'text', nullable: true }) equipo_nombre?: string | null;
  @Column({ type: 'text', nullable: true }) compartimento_principal?: string | null;
  @Column({ type: 'date', nullable: true }) fecha_muestra?: string | null;
  @Column({ type: 'date', nullable: true }) fecha_reporte?: string | null;
  @Column({ type: 'text', nullable: true }) diagnostico?: string | null;
  @Column({ default: 'NORMAL' }) estado_diagnostico: string;
  @Column({ type: 'text', nullable: true }) documento_origen?: string | null;
  @Column({ type: 'jsonb', default: () => "'{}'::jsonb" }) payload_json: Record<
    string,
    unknown
  >;
  @Column({ default: 'ACTIVE' }) status: string;
  @Column({ type: 'timestamp without time zone', default: () => 'now()' }) created_at: Date;
  @Column({ type: 'timestamp without time zone', default: () => 'now()' }) updated_at: Date;
  @Column({ type: 'text', nullable: true }) created_by?: string | null;
  @Column({ type: 'text', nullable: true }) updated_by?: string | null;
  @Column({ default: false }) is_deleted: boolean;
}

@Entity({ schema: 'kpi_maintenance', name: 'tb_analisis_lubricante_det' })
export class AnalisisLubricanteDetalleEntity {
  @PrimaryGeneratedColumn('uuid') id: string;
  @Column({ type: 'uuid' }) analisis_id: string;
  @Column() compartimento: string;
  @Column({ type: 'text', nullable: true }) numero_muestra?: string | null;
  @Column() parametro: string;
  @Column('numeric', { precision: 18, scale: 4, nullable: true })
  resultado_numerico?: number | null;
  @Column({ type: 'text', nullable: true }) resultado_texto?: string | null;
  @Column({ type: 'text', nullable: true }) unidad?: string | null;
  @Column('numeric', { precision: 18, scale: 4, nullable: true })
  linea_base?: number | null;
  @Column({ default: 'NORMAL' }) nivel_alerta: string;
  @Column('numeric', { precision: 18, scale: 4, nullable: true })
  tendencia?: number | null;
  @Column({ type: 'text', nullable: true }) observacion?: string | null;
  @Column({ default: 1 }) orden: number;
  @Column({ default: 'ACTIVE' }) status: string;
  @Column({ type: 'timestamp without time zone', default: () => 'now()' }) created_at: Date;
  @Column({ type: 'timestamp without time zone', default: () => 'now()' }) updated_at: Date;
  @Column({ default: false }) is_deleted: boolean;
}

@Entity({ schema: 'kpi_maintenance', name: 'tb_cronograma_semanal' })
export class CronogramaSemanalEntity {
  @PrimaryGeneratedColumn('uuid') id: string;
  @Column() codigo: string;
  @Column({ type: 'date' }) fecha_inicio: string;
  @Column({ type: 'date' }) fecha_fin: string;
  @Column({ type: 'uuid', nullable: true }) sucursal_id?: string | null;
  @Column({ type: 'text', nullable: true }) locacion?: string | null;
  @Column({ type: 'text', nullable: true }) referencia_orden?: string | null;
  @Column({ type: 'text', nullable: true }) documento_origen?: string | null;
  @Column({ type: 'text', nullable: true }) resumen?: string | null;
  @Column({ type: 'jsonb', default: () => "'{}'::jsonb" }) payload_json: Record<
    string,
    unknown
  >;
  @Column({ default: 'ACTIVE' }) status: string;
  @Column({ type: 'timestamp without time zone', default: () => 'now()' }) created_at: Date;
  @Column({ type: 'timestamp without time zone', default: () => 'now()' }) updated_at: Date;
  @Column({ type: 'text', nullable: true }) created_by?: string | null;
  @Column({ type: 'text', nullable: true }) updated_by?: string | null;
  @Column({ default: false }) is_deleted: boolean;
}

@Entity({ schema: 'kpi_maintenance', name: 'tb_cronograma_semanal_det' })
export class CronogramaSemanalDetalleEntity {
  @PrimaryGeneratedColumn('uuid') id: string;
  @Column({ type: 'uuid' }) cronograma_id: string;
  @Column() dia_semana: string;
  @Column({ type: 'date', nullable: true }) fecha_actividad?: string | null;
  @Column({ type: 'time without time zone', nullable: true }) hora_inicio?:
    | string
    | null;
  @Column({ type: 'time without time zone', nullable: true }) hora_fin?:
    | string
    | null;
  @Column({ type: 'text', nullable: true }) tipo_proceso?: string | null;
  @Column() actividad: string;
  @Column({ type: 'text', nullable: true }) responsable_area?: string | null;
  @Column({ type: 'text', nullable: true }) equipo_codigo?: string | null;
  @Column({ type: 'text', nullable: true }) observacion?: string | null;
  @Column({ default: 1 }) orden: number;
  @Column({ default: 'ACTIVE' }) status: string;
  @Column({ type: 'timestamp without time zone', default: () => 'now()' }) created_at: Date;
  @Column({ type: 'timestamp without time zone', default: () => 'now()' }) updated_at: Date;
  @Column({ default: false }) is_deleted: boolean;
}

@Entity({ schema: 'kpi_maintenance', name: 'tb_programacion_mensual' })
export class ProgramacionMensualEntity {
  @PrimaryGeneratedColumn('uuid') id: string;
  @Column() codigo: string;
  @Column({ type: 'date', nullable: true }) fecha_inicio?: string | null;
  @Column({ type: 'date', nullable: true }) fecha_fin?: string | null;
  @Column({ type: 'uuid', nullable: true }) sucursal_id?: string | null;
  @Column({ type: 'text', nullable: true }) locacion?: string | null;
  @Column({ type: 'text', nullable: true }) documento_origen?: string | null;
  @Column({ type: 'text', nullable: true }) nombre_archivo?: string | null;
  @Column({ type: 'text', nullable: true }) resumen?: string | null;
  @Column({ type: 'jsonb', default: () => "'{}'::jsonb" }) payload_json: Record<
    string,
    unknown
  >;
  @Column({ default: 'ACTIVE' }) status: string;
  @Column({ type: 'timestamp without time zone', default: () => 'now()' }) created_at: Date;
  @Column({ type: 'timestamp without time zone', default: () => 'now()' }) updated_at: Date;
  @Column({ type: 'text', nullable: true }) created_by?: string | null;
  @Column({ type: 'text', nullable: true }) updated_by?: string | null;
  @Column({ default: false }) is_deleted: boolean;
}

@Entity({ schema: 'kpi_maintenance', name: 'tb_programacion_mensual_det' })
export class ProgramacionMensualDetalleEntity {
  @PrimaryGeneratedColumn('uuid') id: string;
  @Column({ type: 'uuid' }) programacion_mensual_id: string;
  @Column({ type: 'uuid', nullable: true }) programacion_id?: string | null;
  @Column({ type: 'uuid', nullable: true }) equipo_id?: string | null;
  @Column({ type: 'text' }) equipo_codigo: string;
  @Column({ type: 'text', nullable: true }) equipo_nombre?: string | null;
  @Column({ type: 'date' }) fecha_programada: string;
  @Column({ type: 'integer', nullable: true }) dia_mes?: number | null;
  @Column({ type: 'text' }) valor_crudo: string;
  @Column({ type: 'text', nullable: true }) valor_normalizado?: string | null;
  @Column({ type: 'text', nullable: true }) tipo_mantenimiento?: string | null;
  @Column({ type: 'integer', nullable: true }) frecuencia_horas?: number | null;
  @Column({ type: 'uuid', nullable: true }) procedimiento_id?: string | null;
  @Column({ type: 'uuid', nullable: true }) plan_id?: string | null;
  @Column({ default: false }) es_sincronizable: boolean;
  @Column({ type: 'text', nullable: true }) observacion?: string | null;
  @Column({ default: 1 }) orden: number;
  @Column({ type: 'jsonb', default: () => "'{}'::jsonb" }) payload_json: Record<
    string,
    unknown
  >;
  @Column({ default: 'ACTIVE' }) status: string;
  @Column({ type: 'timestamp without time zone', default: () => 'now()' }) created_at: Date;
  @Column({ type: 'timestamp without time zone', default: () => 'now()' }) updated_at: Date;
  @Column({ default: false }) is_deleted: boolean;
}

@Entity({ schema: 'kpi_maintenance', name: 'tb_reporte_operacion_diaria' })
export class ReporteOperacionDiariaEntity {
  @PrimaryGeneratedColumn('uuid') id: string;
  @Column() codigo: string;
  @Column({ type: 'date' }) fecha_reporte: string;
  @Column({ type: 'uuid', nullable: true }) sucursal_id?: string | null;
  @Column({ type: 'text', nullable: true }) locacion?: string | null;
  @Column({ type: 'text', nullable: true }) turno?: string | null;
  @Column({ type: 'text', nullable: true }) documento_origen?: string | null;
  @Column({ type: 'text', nullable: true }) resumen?: string | null;
  @Column({ type: 'jsonb', default: () => "'{}'::jsonb" }) payload_json: Record<
    string,
    unknown
  >;
  @Column({ default: 'ACTIVE' }) status: string;
  @Column({ type: 'timestamp without time zone', default: () => 'now()' }) created_at: Date;
  @Column({ type: 'timestamp without time zone', default: () => 'now()' }) updated_at: Date;
  @Column({ type: 'text', nullable: true }) created_by?: string | null;
  @Column({ type: 'text', nullable: true }) updated_by?: string | null;
  @Column({ default: false }) is_deleted: boolean;
}

@Entity({ schema: 'kpi_maintenance', name: 'tb_reporte_operacion_diaria_unidad' })
export class ReporteOperacionDiariaUnidadEntity {
  @PrimaryGeneratedColumn('uuid') id: string;
  @Column({ type: 'uuid' }) reporte_id: string;
  @Column({ type: 'uuid', nullable: true }) equipo_id?: string | null;
  @Column() equipo_codigo: string;
  @Column({ type: 'text', nullable: true }) fabricante?: string | null;
  @Column({ type: 'text', nullable: true }) modo_operacion?: string | null;
  @Column('numeric', { precision: 18, scale: 2, nullable: true })
  carga_kw?: number | null;
  @Column('numeric', { precision: 18, scale: 2, nullable: true })
  horometro_actual?: number | null;
  @Column('numeric', { precision: 18, scale: 2, nullable: true })
  horometro_inicio?: number | null;
  @Column('numeric', { precision: 18, scale: 2, nullable: true })
  horas_operacion?: number | null;
  @Column('numeric', { precision: 18, scale: 2, nullable: true })
  mpg_actual?: number | null;
  @Column({ type: 'text', nullable: true }) proximo_mpg?: string | null;
  @Column('numeric', { precision: 18, scale: 2, nullable: true })
  horas_faltantes?: number | null;
  @Column('numeric', { precision: 18, scale: 2, nullable: true })
  dias_faltantes?: number | null;
  @Column({ type: 'date', nullable: true }) fecha_proxima?: string | null;
  @Column({ type: 'text', nullable: true }) nota?: string | null;
  @Column({ default: 'ACTIVE' }) status: string;
  @Column({ type: 'timestamp without time zone', default: () => 'now()' }) created_at: Date;
  @Column({ type: 'timestamp without time zone', default: () => 'now()' }) updated_at: Date;
  @Column({ default: false }) is_deleted: boolean;
}

@Entity({ schema: 'kpi_maintenance', name: 'tb_reporte_combustible' })
export class ReporteCombustibleEntity {
  @PrimaryGeneratedColumn('uuid') id: string;
  @Column({ type: 'uuid', nullable: true }) reporte_id?: string | null;
  @Column() tanque: string;
  @Column({ default: 'STOCK' }) tipo_lectura: string;
  @Column({ type: 'timestamp without time zone', default: () => 'now()' })
  fecha_lectura: Date;
  @Column('numeric', { precision: 18, scale: 4, nullable: true })
  medida_cm?: number | null;
  @Column('numeric', { precision: 18, scale: 4, nullable: true })
  medida_ft?: number | null;
  @Column('numeric', { precision: 18, scale: 4, nullable: true })
  medida_in?: number | null;
  @Column('numeric', { precision: 18, scale: 4, nullable: true })
  galones?: number | null;
  @Column('numeric', { precision: 18, scale: 4, nullable: true })
  stock_anterior?: number | null;
  @Column('numeric', { precision: 18, scale: 4, nullable: true })
  stock_actual?: number | null;
  @Column('numeric', { precision: 18, scale: 4, nullable: true })
  stock_minimo?: number | null;
  @Column('numeric', { precision: 18, scale: 4, nullable: true })
  stock_maximo?: number | null;
  @Column('numeric', { precision: 18, scale: 4, nullable: true })
  consumo_galones?: number | null;
  @Column({ type: 'text', nullable: true }) guia_remision?: string | null;
  @Column({ type: 'text', nullable: true }) observacion?: string | null;
  @Column({ default: 'ACTIVE' }) status: string;
  @Column({ type: 'timestamp without time zone', default: () => 'now()' }) created_at: Date;
  @Column({ type: 'timestamp without time zone', default: () => 'now()' }) updated_at: Date;
  @Column({ default: false }) is_deleted: boolean;
}

@Entity({ schema: 'kpi_maintenance', name: 'tb_control_componente' })
export class ControlComponenteEntity {
  @PrimaryGeneratedColumn('uuid') id: string;
  @Column({ type: 'uuid', nullable: true }) reporte_id?: string | null;
  @Column({ type: 'uuid', nullable: true }) equipo_id?: string | null;
  @Column() equipo_codigo: string;
  @Column() tipo_componente: string;
  @Column({ type: 'text', nullable: true }) posicion?: string | null;
  @Column({ type: 'text', nullable: true }) serie?: string | null;
  @Column({ type: 'text', nullable: true }) estado?: string | null;
  @Column({ type: 'date', nullable: true }) fecha_instalacion?: string | null;
  @Column({ type: 'date', nullable: true }) fecha_retiro?: string | null;
  @Column('numeric', { precision: 18, scale: 2, nullable: true })
  horometro_instalacion?: number | null;
  @Column('numeric', { precision: 18, scale: 2, nullable: true })
  horometro_retiro?: number | null;
  @Column('numeric', { precision: 18, scale: 2, nullable: true })
  horas_uso?: number | null;
  @Column({ type: 'text', nullable: true }) motivo?: string | null;
  @Column({ type: 'text', nullable: true }) responsable?: string | null;
  @Column({ type: 'text', nullable: true }) documento_origen?: string | null;
  @Column({ type: 'jsonb', default: () => "'{}'::jsonb" }) meta: Record<
    string,
    unknown
  >;
  @Column({ default: 'ACTIVE' }) status: string;
  @Column({ type: 'timestamp without time zone', default: () => 'now()' }) created_at: Date;
  @Column({ type: 'timestamp without time zone', default: () => 'now()' }) updated_at: Date;
  @Column({ default: false }) is_deleted: boolean;
}

@Entity({ schema: 'kpi_maintenance', name: 'tb_evento_proceso' })
export class EventoProcesoEntity {
  @PrimaryGeneratedColumn('uuid') id: string;
  @Column() tipo_proceso: string;
  @Column() accion: string;
  @Column() referencia_tabla: string;
  @Column({ type: 'uuid', nullable: true }) referencia_id?: string | null;
  @Column({ type: 'text', nullable: true }) referencia_codigo?: string | null;
  @Column({ type: 'uuid', nullable: true }) equipo_id?: string | null;
  @Column({ type: 'timestamp without time zone', default: () => 'now()' })
  fecha_evento: Date;
  @Column({ default: 'COMPLETED' }) estado: string;
  @Column({ default: false }) notificacion_enviada: boolean;
  @Column({ type: 'jsonb', default: () => "'{}'::jsonb" }) payload_notificacion: Record<
    string,
    unknown
  >;
  @Column({ type: 'jsonb', default: () => "'{}'::jsonb" }) payload_kpi: Record<
    string,
    unknown
  >;
  @Column({ type: 'text', nullable: true }) created_by?: string | null;
  @Column({ type: 'timestamp without time zone', default: () => 'now()' }) created_at: Date;
  @Column({ type: 'timestamp without time zone', default: () => 'now()' }) updated_at: Date;
  @Column({ default: false }) is_deleted: boolean;
}

@Entity({ schema: 'kpi_process', name: 'tb_work_order' })
export class WorkOrderEntity {
  @PrimaryGeneratedColumn('uuid') id: string;
  @Column() code: string;
  @Column() type: string;
  @Column({ type: 'uuid', nullable: true }) equipment_id?: string | null;
  @Column({ type: 'uuid', nullable: true }) equipo_componente_id?: string | null;
  @Column({ type: 'text', nullable: true }) equipo_componente_nombre?: string | null;
  @Column({ type: 'text', nullable: true }) equipo_componente_nombre_oficial?: string | null;
  @Column({ type: 'uuid', nullable: true }) plan_id?: string | null;
  @Column({ type: 'uuid', nullable: true }) blocked_by_work_order_id?: string | null;
  @Column({ type: 'uuid', nullable: true }) parent_work_order_id?: string | null;
  @Column({ type: 'text', nullable: true }) blocked_reason?: string | null;
  @Column({ type: 'timestamp without time zone', nullable: true })
  blocked_at?: Date | null;
  @Column({ type: 'timestamp without time zone', nullable: true })
  resumed_at?: Date | null;
  @Column() title: string;
   @Column({ type: 'jsonb', nullable: true }) valor_json?: Record<
    string,
    unknown
  > | null;
  @Column({ type: 'text', nullable: true }) description?: string | null;
  @Column({ name: 'status_workflow' }) status_workflow: string;
  @Column({ type: 'integer', default: 5 }) priority: number;
  @Column({ type: 'timestamp without time zone', nullable: true })
  scheduled_start?: Date | null;
  @Column({ type: 'timestamp without time zone', nullable: true })
  scheduled_end?: Date | null;
  @Column({ type: 'timestamp without time zone', nullable: true })
  started_at?: Date | null;
  @Column({ type: 'timestamp without time zone', nullable: true })
  closed_at?: Date | null;
  @Column({ type: 'uuid', nullable: true }) requested_by?: string | null;
  @Column({ type: 'uuid', nullable: true }) approved_by?: string | null;
  @Column({ type: 'uuid', nullable: true }) assigned_to?: string | null;
  @Column({ default: 'ACTIVE' }) status: string;
  @Column({ default: 'INTERNO' }) provider_type: string;
  @Column({ default: 'CORRECTIVO' }) maintenance_kind: string;
  @Column({ default: false }) safety_permit_required: boolean;
  @Column({ type: 'text', nullable: true }) safety_permit_code?: string | null;
  @Column({ type: 'uuid', nullable: true }) vendor_id?: string | null;
  @Column({ type: 'uuid', nullable: true }) purchase_request_id?: string | null;
  @Column({ default: false }) is_deleted: boolean;
}


@Entity({ schema: 'kpi_process', name: 'tb_work_order_status_history' })
export class WorkOrderStatusHistoryEntity {
  @PrimaryGeneratedColumn('uuid') id: string;
  @Column({ type: 'uuid' }) work_order_id: string;
  @Column({ type: 'text', nullable: true }) from_status?: string | null;
  @Column({ type: 'text' }) to_status: string;
  @Column({ type: 'uuid', nullable: true }) changed_by?: string | null;
  @Column({ type: 'timestamp without time zone', default: () => 'now()' })
  changed_at: Date;
  @Column({ type: 'text', nullable: true }) note?: string | null;
}

@Entity({ schema: 'kpi_inventory', name: 'tb_stock_bodega' })
export class StockBodegaEntity {
  @PrimaryGeneratedColumn('uuid') id: string;
  @Column({ type: 'uuid' }) bodega_id: string;
  @Column({ type: 'uuid' }) producto_id: string;
  @Column('numeric', { precision: 18, scale: 6, default: 0 })
  stock_actual: number;
  @Column('numeric', { precision: 18, scale: 6, default: 0 })
  stock_min_bodega: number;
  @Column('numeric', { precision: 18, scale: 6, default: 0 })
  stock_max_bodega: number;
  @Column('numeric', { precision: 18, scale: 6, default: 0 })
  stock_min_global: number;
  @Column('numeric', { precision: 18, scale: 6, default: 0 })
  stock_contenedores: number;
  @Column('numeric', { precision: 14, scale: 4, default: 0 })
  costo_promedio_bodega: number;
  @Column({ type: 'text', nullable: true }) created_by?: string | null;
  @Column({ type: 'text', nullable: true }) updated_by?: string | null;
  @Column({ default: false }) is_deleted: boolean;
}

@Entity({ schema: 'kpi_inventory', name: 'tb_reserva_stock' })
export class ReservaStockEntity {
  @PrimaryGeneratedColumn('uuid') id: string;
  @Column({ type: 'uuid' }) work_order_id: string;
  @Column({ type: 'uuid' }) producto_id: string;
  @Column({ type: 'uuid' }) bodega_id: string;
  @Column('numeric', { precision: 18, scale: 4 }) cantidad: number;
  @Column({ default: 'RESERVADO' }) estado: string;
  @Column({ default: false }) is_deleted: boolean;
}

@Entity({ schema: 'kpi_inventory', name: 'tb_entrega_material' })
export class EntregaMaterialEntity {
  @PrimaryGeneratedColumn('uuid') id: string;
  @Column() code: string;
  @Column({ type: 'timestamp without time zone', default: () => 'now()' })
  fecha: Date;
  @Column({ type: 'uuid' }) work_order_id: string;
  @Column({ type: 'text', nullable: true }) observacion?: string | null;
  @Column({ default: false }) is_deleted: boolean;
}

@Entity({ schema: 'kpi_inventory', name: 'tb_entrega_material_det' })
export class EntregaMaterialDetEntity {
  @PrimaryGeneratedColumn('uuid') id: string;
  @Column({ type: 'uuid' }) entrega_id: string;
  @Column({ type: 'uuid' }) producto_id: string;
  @Column({ type: 'uuid' }) bodega_id: string;
  @Column('numeric', { precision: 18, scale: 4 }) cantidad: number;
  @Column('numeric', { precision: 18, scale: 4, default: 0 })
  costo_unitario: number;
}

@Entity({ schema: 'kpi_inventory', name: 'tb_movimiento_inventario' })
export class MovimientoInventarioEntity {
  @PrimaryGeneratedColumn('uuid') id: string;
  @Column() tipo_movimiento: string;
  @Column({ type: 'timestamp without time zone', default: () => 'now()' })
  fecha_movimiento: Date;
  @Column({ type: 'uuid', nullable: true }) bodega_origen_id?: string | null;
  @Column('numeric', { precision: 18, scale: 4, default: 0 })
  total_costos: number;
  @Column({ type: 'uuid', nullable: true }) work_order_id?: string | null;
}

@Entity({ schema: 'kpi_inventory', name: 'tb_movimiento_inventario_det' })
export class MovimientoInventarioDetEntity {
  @PrimaryGeneratedColumn('uuid') id: string;
  @Column({ type: 'uuid' }) movimiento_id: string;
  @Column({ type: 'uuid' }) producto_id: string;
  @Column('numeric', { precision: 18, scale: 6 }) cantidad: number;
  @Column('numeric', { precision: 14, scale: 4 }) costo_unitario: number;
  @Column('numeric', { precision: 18, scale: 4 }) subtotal_costo: number;
}

@Entity({ schema: 'kpi_inventory', name: 'tb_kardex' })
export class KardexEntity {
  @PrimaryGeneratedColumn('uuid') id: string;
  @Column({ type: 'timestamp without time zone', default: () => 'now()' })
  fecha: Date;
  @Column({ type: 'uuid' }) bodega_id: string;
  @Column({ type: 'uuid' }) producto_id: string;
  @Column({ type: 'uuid', nullable: true }) movimiento_id?: string | null;
  @Column({ type: 'uuid', nullable: true }) movimiento_det_id?: string | null;
  @Column() tipo_movimiento: string;
  @Column('numeric', { precision: 18, scale: 6, default: 0 })
  entrada_cantidad: number;
  @Column('numeric', { precision: 18, scale: 6, default: 0 })
  salida_cantidad: number;
  @Column('numeric', { precision: 14, scale: 4, default: 0 })
  costo_unitario: number;
  @Column('numeric', { precision: 18, scale: 4, default: 0 })
  costo_total: number;
  @Column('numeric', { precision: 18, scale: 6, default: 0 })
  saldo_cantidad: number;
  @Column('numeric', { precision: 14, scale: 4, default: 0 })
  saldo_costo_promedio: number;
  @Column('numeric', { precision: 18, scale: 4, default: 0 })
  saldo_valorizado: number;
}

@Entity({ schema: 'kpi_inventory', name: 'tb_producto' })
export class ProductoEntity {
  @PrimaryGeneratedColumn('uuid') id: string;
  @Column({ type: 'varchar', length: 60, nullable: true }) codigo?: string | null;
  @Column({ type: 'varchar', length: 250, nullable: true }) nombre?: string | null;
  @Column('numeric', { precision: 14, scale: 4, default: 0 })
  ultimo_costo: number;
  @Column({ default: false }) is_deleted: boolean;
}

@Entity({ schema: 'kpi_inventory', name: 'tb_bodega' })
export class BodegaEntity {
  @PrimaryGeneratedColumn('uuid') id: string;
  @Column({ type: 'uuid', nullable: true }) sucursal_id?: string | null;
  @Column({ type: 'varchar', length: 30, nullable: true }) codigo?: string | null;
  @Column({ type: 'varchar', length: 150, nullable: true }) nombre?: string | null;
  @Column({ default: false }) is_deleted: boolean;
}

@Entity({ schema: 'kpi_inventory', name: 'tb_sucursal' })
export class InventorySucursalEntity {
  @PrimaryGeneratedColumn('uuid') id: string;
  @Column({ type: 'varchar', length: 30, nullable: true }) codigo?: string | null;
  @Column({ type: 'varchar', length: 150, nullable: true }) nombre?: string | null;
  @Column({ type: 'text', nullable: true }) status?: string | null;
  @Column({ default: false }) is_deleted: boolean;
}
