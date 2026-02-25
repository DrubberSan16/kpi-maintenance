import { Column, Entity } from 'typeorm';
import { BaseAuditEntity } from '../../common/entities/base-audit.entity';

@Entity({ schema: 'kpi_maintenance', name: 'tb_tercero' })
export class Tercero extends BaseAuditEntity {
  @Column({ type: 'text' })
  tipo: string;

  @Column({ type: 'varchar', length: 30, nullable: true })
  identificacion?: string | null;

  @Column({ type: 'varchar', length: 200 })
  razon_social: string;

  @Column({ type: 'varchar', length: 200, nullable: true })
  nombre_comercial?: string | null;

  @Column({ type: 'varchar', length: 50, nullable: true })
  telefono?: string | null;

  @Column({ type: 'varchar', length: 150, nullable: true })
  email?: string | null;

  @Column({ type: 'text', nullable: true })
  direccion?: string | null;
}
