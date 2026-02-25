import {
  Column,
  CreateDateColumn,
  PrimaryGeneratedColumn,
  UpdateDateColumn,
} from 'typeorm';

export abstract class BaseAuditEntity {
  @PrimaryGeneratedColumn('uuid')
  id: string;

  @Column({ type: 'text', default: 'ACTIVE' })
  status: string;

  @CreateDateColumn({
    type: 'timestamp without time zone',
    default: () => 'now()',
  })
  created_at: Date;

  @UpdateDateColumn({
    type: 'timestamp without time zone',
    default: () => 'now()',
  })
  updated_at: Date;

  @Column({ type: 'text', nullable: true })
  created_by?: string | null;

  @Column({ type: 'text', nullable: true })
  updated_by?: string | null;

  @Column({ type: 'boolean', default: false })
  is_deleted: boolean;

  @Column({ type: 'timestamp without time zone', nullable: true })
  deleted_at?: Date | null;

  @Column({ type: 'text', nullable: true })
  deleted_by?: string | null;
}
