import { Column, Entity } from 'typeorm';
import { BaseAuditEntity } from '../../common/entities/base-audit.entity';

@Entity({ schema: 'kpi_maintenance', name: 'tb_bodega' })
export class Bodega extends BaseAuditEntity {
  @Column({ type: 'uuid' })
  sucursal_id: string;

  @Column({ type: 'varchar', length: 30 })
  codigo: string;

  @Column({ type: 'varchar', length: 150 })
  nombre: string;

  @Column({ type: 'text', nullable: true })
  direccion?: string | null;

  @Column({ type: 'boolean', default: false })
  es_principal: boolean;
}
