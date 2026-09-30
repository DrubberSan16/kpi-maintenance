import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';
import { Column, Entity } from 'typeorm';
import { BaseAuditEntity } from '../../common/entities/base-audit.entity';

/** `numeric` llega de PostgreSQL como texto; la API devuelve números. */
const numerico = {
  to: (value?: number | null) => value ?? null,
  from: (value?: string | number | null) =>
    value === null || value === undefined ? null : Number(value),
};

/**
 * Personal de la empresa. Ver sql/20260929_empleados.sql para las reglas de
 * `valor_hora`, `cargo` y de unicidad de cédula y usuario.
 */
@Entity({ schema: 'kpi_maintenance', name: 'tb_empleado' })
export class EmpleadoEntity extends BaseAuditEntity {
  @Column({ type: 'uuid', nullable: true })
  @ApiPropertyOptional({
    description: 'Usuario del sistema del empleado; nulo si no tiene uno',
    nullable: true,
  })
  user_id?: string | null;

  @Column({ type: 'varchar', length: 200 })
  @ApiProperty({ description: 'Nombres y apellidos' })
  nombres_apellidos: string;

  @Column({ type: 'varchar', length: 10 })
  @ApiProperty({ description: 'Cédula, diez dígitos' })
  cedula: string;

  @Column({ type: 'numeric', precision: 12, scale: 2, transformer: numerico })
  @ApiProperty({ description: 'Sueldo mensual en USD' })
  sueldo: number;

  @Column({ type: 'numeric', precision: 14, scale: 4, transformer: numerico })
  @ApiProperty({ description: 'Valor de la hora ordinaria en USD' })
  valor_hora: number;

  @Column({ type: 'boolean', default: false })
  @ApiProperty({
    description:
      'true si el valor por hora se fijó a mano y no sigue al sueldo',
  })
  valor_hora_manual: boolean;

  @Column({ type: 'varchar', length: 150 })
  @ApiProperty({ description: 'Cargo' })
  cargo: string;
}
