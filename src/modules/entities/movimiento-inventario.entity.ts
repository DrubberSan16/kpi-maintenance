import { Column, Entity } from 'typeorm';
import { BaseAuditEntity } from '../../common/entities/base-audit.entity';

@Entity({ schema: 'kpi_maintenance', name: 'tb_movimiento_inventario' })
export class MovimientoInventario extends BaseAuditEntity {
  @Column({ type: 'text' })
  tipo_movimiento: string;

  @Column({ type: 'timestamp without time zone', default: () => 'now()' })
  fecha_movimiento: Date;

  @Column({ type: 'text', nullable: true })
  tipo_documento?: string | null;

  @Column({ type: 'varchar', length: 60, nullable: true })
  numero_documento?: string | null;

  @Column({ type: 'varchar', length: 120, nullable: true })
  referencia?: string | null;

  @Column({ type: 'text', nullable: true })
  observacion?: string | null;

  @Column({ type: 'uuid', nullable: true })
  bodega_origen_id?: string | null;

  @Column({ type: 'uuid', nullable: true })
  bodega_destino_id?: string | null;

  @Column({ type: 'uuid', nullable: true })
  tercero_id?: string | null;

  @Column({ type: 'varchar', length: 10, nullable: true, default: 'USD' })
  moneda?: string | null;

  @Column({ type: 'numeric', precision: 14, scale: 6, default: 1 })
  tipo_cambio: string;

  @Column({ type: 'numeric', precision: 18, scale: 4, default: 0 })
  total_costos: string;

  @Column({ type: 'text', default: 'CONFIRMADO' })
  estado: string;
}
