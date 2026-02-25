import { Column, Entity } from 'typeorm';
import { BaseAuditEntity } from '../../common/entities/base-audit.entity';

@Entity({ schema: 'kpi_maintenance', name: 'tb_producto' })
export class Producto extends BaseAuditEntity {
  @Column({ type: 'varchar', length: 60, unique: true })
  codigo: string;

  @Column({ type: 'varchar', length: 250 })
  nombre: string;

  @Column({ type: 'text', nullable: true })
  descripcion?: string | null;

  @Column({ type: 'uuid', nullable: true })
  linea_id?: string | null;

  @Column({ type: 'uuid', nullable: true })
  categoria_id?: string | null;

  @Column({ type: 'uuid', nullable: true })
  marca_id?: string | null;

  @Column({ type: 'varchar', length: 100, nullable: true })
  registro_sanitario?: string | null;

  @Column({ type: 'uuid', nullable: true })
  unidad_medida_id?: string | null;

  @Column({ type: 'boolean', default: false })
  por_contenedores: boolean;

  @Column({ type: 'varchar', length: 80, nullable: true })
  sku?: string | null;

  @Column({ type: 'varchar', length: 80, nullable: true })
  codigo_barras?: string | null;

  @Column({ type: 'boolean', default: false })
  es_servicio: boolean;

  @Column({ type: 'boolean', default: false })
  requiere_lote: boolean;

  @Column({ type: 'boolean', default: false })
  requiere_serie: boolean;

  @Column({ type: 'numeric', precision: 14, scale: 4, default: 0 })
  ultimo_costo: string;

  @Column({ type: 'numeric', precision: 14, scale: 4, default: 0 })
  costo_promedio: string;

  @Column({ type: 'numeric', precision: 14, scale: 4, default: 0 })
  precio_venta: string;

  @Column({ type: 'numeric', precision: 9, scale: 4, default: 0 })
  porcentaje_utilidad: string;
}
