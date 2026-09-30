import { ApiProperty, ApiPropertyOptional, PartialType } from '@nestjs/swagger';
import { Transform, Type } from 'class-transformer';
import {
  ArrayMaxSize,
  ArrayMinSize,
  IsArray,
  IsIn,
  IsNotEmpty,
  IsNumber,
  IsOptional,
  IsString,
  IsUUID,
  Max,
  MaxLength,
  Min,
} from 'class-validator';
import { PaginationQueryDto } from '../../common/dto/pagination-query.dto';

export const ESTADOS_EMPLEADO = ['ACTIVE', 'INACTIVE'] as const;

/** La cédula puede llegar como número (Excel) o con espacios: se lee como texto. */
const comoTexto = ({ value }: { value: unknown }) =>
  typeof value === 'number' ? String(value) : value;

export class CreateEmpleadoDto {
  @ApiPropertyOptional({
    description: 'Usuario del sistema del empleado; vacío si no tiene uno',
    nullable: true,
  })
  @IsOptional()
  @IsUUID()
  user_id?: string | null;

  @ApiProperty({ description: 'Nombres y apellidos', maxLength: 200 })
  @IsString()
  @IsNotEmpty()
  @MaxLength(200)
  nombres_apellidos: string;

  @ApiProperty({ description: 'Cédula de diez dígitos' })
  @Transform(comoTexto)
  @IsString()
  @IsNotEmpty()
  cedula: string;

  @ApiProperty({ description: 'Sueldo mensual en USD', example: 700 })
  @Type(() => Number)
  @IsNumber({ maxDecimalPlaces: 2 })
  @Min(0.01)
  @Max(1000000)
  sueldo: number;

  @ApiPropertyOptional({
    description:
      'Valor por hora fijado a mano. Si falta o es nulo se calcula con el sueldo (sueldo / 240).',
    nullable: true,
  })
  @IsOptional()
  @Type(() => Number)
  @IsNumber({ maxDecimalPlaces: 4 })
  @Min(0)
  @Max(100000)
  valor_hora?: number | null;

  @ApiProperty({ description: 'Cargo', maxLength: 150 })
  @IsString()
  @IsNotEmpty()
  @MaxLength(150)
  cargo: string;

  @ApiPropertyOptional({ enum: ESTADOS_EMPLEADO })
  @IsOptional()
  @IsIn(ESTADOS_EMPLEADO)
  status?: string;
}

export class UpdateEmpleadoDto extends PartialType(CreateEmpleadoDto) {}

export class EmpleadoQueryDto extends PaginationQueryDto {
  @ApiPropertyOptional({ enum: ESTADOS_EMPLEADO })
  @IsOptional()
  @IsIn(ESTADOS_EMPLEADO)
  status?: string;
}

/**
 * Las filas de la importación se dejan sueltas a propósito: si una sola fila
 * mal escrita hiciera fallar la validación del DTO, se rechazaría el archivo
 * entero. Cada fila se valida en el servicio y el resultado dice cuáles se
 * omitieron y por qué.
 */
export class ImportarEmpleadosDto {
  @ApiProperty({
    description:
      'Filas del Excel: nombres_apellidos, cedula, sueldo, cargo y, opcional, fila y user_id',
    type: 'array',
    items: { type: 'object', additionalProperties: true },
  })
  @IsArray()
  @ArrayMinSize(1)
  @ArrayMaxSize(1000)
  empleados: Array<Record<string, unknown>>;
}

export type ErrorImportacion = {
  fila: number;
  cedula: string;
  nombres_apellidos: string;
  mensaje: string;
};

export type ResultadoImportacion = {
  total: number;
  creados: number;
  actualizados: number;
  sin_cambios: number;
  omitidos: number;
  errores: ErrorImportacion[];
};
