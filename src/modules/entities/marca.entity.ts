import { Column, Entity } from 'typeorm';
import { BaseAuditEntity } from '../../common/entities/base-audit.entity';

@Entity({ schema: 'kpi_maintenance', name: 'tb_marca' })
export class Marca extends BaseAuditEntity {
  @Column({ type: 'varchar', length: 150, unique: true })
  nombre: string;
}
