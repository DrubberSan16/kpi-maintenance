import { Column, Entity } from 'typeorm';
import { BaseAuditEntity } from '../../common/entities/base-audit.entity';

@Entity({ schema: 'kpi_maintenance', name: 'tb_movimiento_inventario_det' })
export class MovimientoInventarioDet extends BaseAuditEntity {
  @Column({ type: 'uuid' })
  movimiento_id: string;

  @Column({ type: 'uuid' })
  producto_id: string;

  @Column({ type: 'numeric', precision: 18, scale: 6 })
  cantidad: string;

  @Column({ type: 'uuid', nullable: true })
  unidad_medida_id?: string | null;

  @Column({ type: 'numeric', precision: 14, scale: 4, default: 0 })
  costo_unitario: string;

  @Column({ type: 'numeric', precision: 18, scale: 4, default: 0 })
  subtotal_costo: string;

  @Column({ type: 'varchar', length: 80, nullable: true })
  lote?: string | null;

  @Column({ type: 'varchar', length: 120, nullable: true })
  serie?: string | null;

  @Column({ type: 'date', nullable: true })
  fecha_vencimiento?: string | null;

  @Column({ type: 'text', nullable: true })
  observacion?: string | null;
}
