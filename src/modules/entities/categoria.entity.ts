import { Column, Entity } from 'typeorm';
import { BaseAuditEntity } from '../../common/entities/base-audit.entity';

@Entity({ schema: 'kpi_maintenance', name: 'tb_categoria' })
export class Categoria extends BaseAuditEntity {
  @Column({ type: 'varchar', length: 30, unique: true, nullable: true })
  codigo?: string | null;

  @Column({ type: 'varchar', length: 150 })
  nombre: string;

  @Column({ type: 'text', nullable: true })
  descripcion?: string | null;
}
