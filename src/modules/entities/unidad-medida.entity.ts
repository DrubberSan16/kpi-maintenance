import { Column, Entity } from 'typeorm';
import { BaseAuditEntity } from '../../common/entities/base-audit.entity';

@Entity({ schema: 'kpi_maintenance', name: 'tb_unidad_medida' })
export class UnidadMedida extends BaseAuditEntity {
  @Column({ type: 'varchar', length: 30, unique: true, nullable: true })
  codigo?: string | null;

  @Column({ type: 'varchar', length: 100, unique: true })
  nombre: string;

  @Column({ type: 'varchar', length: 20, nullable: true })
  abreviatura?: string | null;

  @Column({ type: 'boolean', default: false })
  es_base: boolean;
}
