import { Column, Entity } from 'typeorm';
import { BaseAuditEntity } from '../../common/entities/base-audit.entity';

@Entity({ schema: 'kpi_maintenance', name: 'tb_kardex' })
export class Kardex extends BaseAuditEntity {
  @Column({ type: 'timestamp without time zone', default: () => 'now()' })
  fecha: Date;

  @Column({ type: 'uuid' })
  bodega_id: string;

  @Column({ type: 'uuid' })
  producto_id: string;

  @Column({ type: 'uuid', nullable: true })
  movimiento_id?: string | null;

  @Column({ type: 'uuid', nullable: true })
  movimiento_det_id?: string | null;

  @Column({ type: 'text' })
  tipo_movimiento: string;

  @Column({ type: 'numeric', precision: 18, scale: 6, default: 0 })
  entrada_cantidad: string;

  @Column({ type: 'numeric', precision: 18, scale: 6, default: 0 })
  salida_cantidad: string;

  @Column({ type: 'numeric', precision: 14, scale: 4, default: 0 })
  costo_unitario: string;

  @Column({ type: 'numeric', precision: 18, scale: 4, default: 0 })
  costo_total: string;

  @Column({ type: 'numeric', precision: 18, scale: 6, default: 0 })
  saldo_cantidad: string;

  @Column({ type: 'numeric', precision: 14, scale: 4, default: 0 })
  saldo_costo_promedio: string;

  @Column({ type: 'numeric', precision: 18, scale: 4, default: 0 })
  saldo_valorizado: string;

  @Column({ type: 'text', nullable: true })
  observacion?: string | null;
}
