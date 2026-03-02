import { Column, Entity, PrimaryGeneratedColumn } from 'typeorm';

@Entity({ schema: 'kpi_maintenance', name: 'tb_equipo' })
export class EquipoEntity {
  @PrimaryGeneratedColumn('uuid') id: string;
  @Column() codigo: string;
  @Column() nombre: string;
  @Column() equipo_tipo_id: string;
  @Column({ type: 'uuid', nullable: true }) location_id?: string | null;
  @Column({ default: 'MEDIA' }) criticidad: string;
  @Column({ default: 'OPERATIVO' }) estado_operativo: string;
  @Column('numeric', { precision: 18, scale: 2, default: 0 }) horometro_actual: number;
  @Column({ type: 'timestamp without time zone', nullable: true }) fecha_ultima_lectura?: Date | null;
  @Column({ default: 'ACTIVE' }) status: string;
  @Column({ type: 'timestamp without time zone', default: () => 'now()' }) created_at: Date;
  @Column({ type: 'timestamp without time zone', default: () => 'now()' }) updated_at: Date;
  @Column({ type: 'text', nullable: true }) created_by?: string | null;
  @Column({ type: 'text', nullable: true }) updated_by?: string | null;
  @Column({ default: false }) is_deleted: boolean;
  @Column({ type: 'timestamp without time zone', nullable: true }) deleted_at?: Date | null;
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
  @Column({ type: 'timestamp without time zone', default: () => 'now()' }) created_at: Date;
  @Column({ type: 'timestamp without time zone', default: () => 'now()' }) updated_at: Date;
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
  @Column({type: 'uuid'}) equipo_id: string;
  @Column({type: 'uuid'}) estado_id: string;
  @Column({ type: 'timestamp without time zone', default: () => 'now()' }) fecha_inicio: Date;
  @Column({ type: 'timestamp without time zone', nullable: true }) fecha_fin?: Date | null;
  @Column({ type: 'text', nullable: true }) motivo?: string | null;
  @Column({ default: 'ACTIVE' }) status: string;
  @Column({ default: false }) is_deleted: boolean;
}

@Entity({ schema: 'kpi_maintenance', name: 'tb_evento_equipo' })
export class EventoEquipoEntity {
  @PrimaryGeneratedColumn('uuid') id: string;
  @Column({type: 'uuid'}) equipo_id: string;
  @Column({type: 'uuid', nullable: true }) work_order_id?: string | null;
  @Column() tipo_evento: string;
  @Column({ type: 'timestamp without time zone', default: () => 'now()' }) fecha_inicio: Date;
  @Column({ type: 'timestamp without time zone', nullable: true }) fecha_fin?: Date | null;
  @Column({ default: 3 }) severidad: number;
  @Column({ type: 'text', nullable: true }) descripcion?: string | null;
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
  @Column({type: 'uuid'}) plan_id: string;
  @Column({ default: 1 }) orden: number;
  @Column({ type: 'text' }) actividad: string;
  @Column({ default: 'BOOLEAN' }) field_type: string;
  @Column({ default: false }) required: boolean;
  @Column({ type: 'jsonb', default: () => "'{}'::jsonb" }) meta: Record<string, unknown>;
  @Column({ default: 'ACTIVE' }) status: string;
  @Column({ default: false }) is_deleted: boolean;
}

@Entity({ schema: 'kpi_maintenance', name: 'tb_programacion_plan' })
export class ProgramacionPlanEntity {
  @PrimaryGeneratedColumn('uuid') id: string;
  @Column({type: 'uuid'}) equipo_id: string;
  @Column({type: 'uuid'}) plan_id: string;
  @Column({ type: 'date', nullable: true }) ultima_ejecucion_fecha?: string | null;
  @Column('numeric', { precision: 18, scale: 2, nullable: true }) ultima_ejecucion_horas?: number | null;
  @Column({ type: 'date', nullable: true }) proxima_fecha?: string | null;
  @Column('numeric', { precision: 18, scale: 2, nullable: true }) proxima_horas?: number | null;
  @Column({ default: true }) activo: boolean;
  @Column({ default: 'ACTIVE' }) status: string;
  @Column({ default: false }) is_deleted: boolean;
}

@Entity({ schema: 'kpi_maintenance', name: 'tb_alerta_mantenimiento' })
export class AlertaMantenimientoEntity {
  @PrimaryGeneratedColumn('uuid') id: string;
  @Column({type: 'uuid'}) equipo_id: string;
  @Column({type: 'uuid'}) tipo_alerta: string;
  @Column({ type: 'timestamp without time zone', default: () => 'now()' }) fecha_generada: Date;
  @Column({ default: 'ABIERTA' }) estado: string;
  @Column({ type: 'text', nullable: true }) detalle?: string | null;
  @Column({ type: 'text', nullable: true }) referencia?: string | null;
  @Column({ type: 'uuid', nullable: true }) work_order_id?: string | null;
  @Column({ default: 'ACTIVE' }) status: string;
  @Column({ default: false }) is_deleted: boolean;
}

@Entity({ schema: 'kpi_maintenance', name: 'tb_consumo_repuesto' })
export class ConsumoRepuestoEntity {
  @PrimaryGeneratedColumn('uuid') id: string;
  @Column({type: 'uuid'}) work_order_id: string;
  @Column({type: 'uuid'}) producto_id: string;
  @Column({type: 'uuid', nullable: true }) bodega_id?: string | null;
  @Column('numeric', { precision: 18, scale: 6, default: 0 }) cantidad: number;
  @Column('numeric', { precision: 14, scale: 4, default: 0 }) costo_unitario: number;
  @Column('numeric', { precision: 18, scale: 4, default: 0 }) subtotal: number;
  @Column({ type: 'text', nullable: true }) observacion?: string | null;
  @Column({ default: false }) is_deleted: boolean;
}

@Entity({ schema: 'kpi_process', name: 'tb_work_order' })
export class WorkOrderEntity {
  @PrimaryGeneratedColumn('uuid') id: string;
  @Column({type: 'uuid', nullable: true }) equipment_id?: string | null;
  @Column({ type: 'uuid', nullable: true }) maintenance_kind?: string | null;
  @Column({ name: 'status_workflow' }) status_workflow: string;
  @Column({ default: false }) is_deleted: boolean;
}

@Entity({ schema: 'kpi_inventory', name: 'tb_stock_bodega' })
export class StockBodegaEntity {
  @PrimaryGeneratedColumn('uuid') id: string;
  @Column({type: 'uuid'}) bodega_id: string;
  @Column({type: 'uuid'}) producto_id: string;
  @Column('numeric', { precision: 18, scale: 6, default: 0 }) stock_actual: number;
}

@Entity({ schema: 'kpi_inventory', name: 'tb_reserva_stock' })
export class ReservaStockEntity {
  @PrimaryGeneratedColumn('uuid') id: string;
  @Column({type: 'uuid'}) work_order_id: string;
  @Column({type: 'uuid'}) producto_id: string;
  @Column({type: 'uuid'}) bodega_id: string;
  @Column('numeric', { precision: 18, scale: 4 }) cantidad: number;
  @Column({ default: 'RESERVADO' }) estado: string;
  @Column({ default: false }) is_deleted: boolean;
}

@Entity({ schema: 'kpi_inventory', name: 'tb_entrega_material' })
export class EntregaMaterialEntity {
  @PrimaryGeneratedColumn('uuid') id: string;
  @Column() code: string;
  @Column({ type: 'timestamp without time zone', default: () => 'now()' }) fecha: Date;
  @Column({type: 'uuid'}) work_order_id: string;
  @Column({ type: 'text', nullable: true }) observacion?: string | null;
  @Column({ default: false }) is_deleted: boolean;
}

@Entity({ schema: 'kpi_inventory', name: 'tb_entrega_material_det' })
export class EntregaMaterialDetEntity {
  @PrimaryGeneratedColumn('uuid') id: string;
  @Column({type: 'uuid'}) entrega_id: string;
  @Column({type: 'uuid'}) producto_id: string;
  @Column({type: 'uuid'}) bodega_id: string;
  @Column('numeric', { precision: 18, scale: 4 }) cantidad: number;
  @Column('numeric', { precision: 18, scale: 4, default: 0 }) costo_unitario: number;
}

@Entity({ schema: 'kpi_inventory', name: 'tb_movimiento_inventario' })
export class MovimientoInventarioEntity {
  @PrimaryGeneratedColumn('uuid') id: string;
  @Column() tipo_movimiento: string;
  @Column({ type: 'timestamp without time zone', default: () => 'now()' }) fecha_movimiento: Date;
  @Column({ type: 'uuid', nullable: true }) bodega_origen_id?: string | null;
  @Column('numeric', { precision: 18, scale: 4, default: 0 }) total_costos: number;
  @Column({type: 'uuid', nullable: true }) work_order_id?: string | null;
}

@Entity({ schema: 'kpi_inventory', name: 'tb_movimiento_inventario_det' })
export class MovimientoInventarioDetEntity {
  @PrimaryGeneratedColumn('uuid') id: string;
  @Column({type: 'uuid'}) movimiento_id: string;
  @Column({type: 'uuid'}) producto_id: string;
  @Column('numeric', { precision: 18, scale: 6 }) cantidad: number;
  @Column('numeric', { precision: 14, scale: 4 }) costo_unitario: number;
  @Column('numeric', { precision: 18, scale: 4 }) subtotal_costo: number;
}

@Entity({ schema: 'kpi_inventory', name: 'tb_kardex' })
export class KardexEntity {
  @PrimaryGeneratedColumn('uuid') id: string;
  @Column({ type: 'timestamp without time zone', default: () => 'now()' }) fecha: Date;
  @Column({type: 'uuid'}) bodega_id: string;
  @Column({type: 'uuid'}) producto_id: string;
  @Column({type: 'uuid', nullable: true }) movimiento_id?: string | null;
  @Column({type: 'uuid', nullable: true }) movimiento_det_id?: string | null;
  @Column() tipo_movimiento: string;
  @Column('numeric', { precision: 18, scale: 6, default: 0 }) entrada_cantidad: number;
  @Column('numeric', { precision: 18, scale: 6, default: 0 }) salida_cantidad: number;
  @Column('numeric', { precision: 14, scale: 4, default: 0 }) costo_unitario: number;
  @Column('numeric', { precision: 18, scale: 4, default: 0 }) costo_total: number;
  @Column('numeric', { precision: 18, scale: 6, default: 0 }) saldo_cantidad: number;
  @Column('numeric', { precision: 14, scale: 4, default: 0 }) saldo_costo_promedio: number;
  @Column('numeric', { precision: 18, scale: 4, default: 0 }) saldo_valorizado: number;
}

@Entity({ schema: 'kpi_inventory', name: 'tb_producto' })
export class ProductoEntity {
  @PrimaryGeneratedColumn('uuid') id: string;
  @Column('numeric', { precision: 14, scale: 4, default: 0 }) ultimo_costo: number;
}

@Entity({ schema: 'kpi_inventory', name: 'tb_bodega' })
export class BodegaEntity {
  @PrimaryGeneratedColumn('uuid') id: string;
}
