import { Column, Entity } from 'typeorm';
import { BaseAuditEntity } from '../../common/entities/base-audit.entity';

@Entity({ schema: 'kpi_maintenance', name: 'tb_stock_bodega' })
export class StockBodega extends BaseAuditEntity {
  @Column({ type: 'uuid' })
  bodega_id: string;

  @Column({ type: 'uuid' })
  producto_id: string;

  @Column({ type: 'numeric', precision: 18, scale: 6, default: 0 })
  stock_actual: string;

  @Column({ type: 'numeric', precision: 18, scale: 6, default: 0 })
  stock_min_bodega: string;

  @Column({ type: 'numeric', precision: 18, scale: 6, default: 0 })
  stock_max_bodega: string;

  @Column({ type: 'numeric', precision: 18, scale: 6, default: 0 })
  stock_min_global: string;

  @Column({ type: 'numeric', precision: 18, scale: 6, default: 0 })
  stock_contenedores: string;

  @Column({ type: 'numeric', precision: 14, scale: 4, default: 0 })
  costo_promedio_bodega: string;
}
