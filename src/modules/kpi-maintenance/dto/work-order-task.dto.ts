import { Type } from 'class-transformer';
import {
  IsArray,
  IsBoolean,
  IsNumber,
  IsObject,
  IsOptional,
  IsString,
  IsUUID,
  Min,
  ValidateNested,
} from 'class-validator';
import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';

export class WorkOrderTareaResponsableDto {
  @ApiProperty({
    description: 'ID del usuario responsable',
    format: 'uuid',
  })
  @IsUUID()
  user_id: string;

  @ApiPropertyOptional({
    description: 'Horas acumuladas del responsable en la tarea',
    type: Number,
    default: 0,
  })
  @IsOptional()
  @Type(() => Number)
  @IsNumber()
  @Min(0)
  horas?: number;
}

export class CreateWorkOrderTareaDto {
  @ApiPropertyOptional({ description: 'ID del plan', format: 'uuid' })
  @IsOptional()
  @IsUUID()
  plan_id?: string;

  @ApiPropertyOptional({ description: 'ID de la tarea de plan', format: 'uuid' })
  @IsOptional()
  @IsUUID()
  tarea_id?: string;

  @ApiPropertyOptional({
    description: 'Indica si la tarea fue agregada manualmente a la OT',
  })
  @IsOptional()
  @IsBoolean()
  es_adicional?: boolean;

  @ApiPropertyOptional({ description: 'Nombre libre de la tarea adicional' })
  @IsOptional()
  @IsString()
  actividad_adicional?: string;

  @ApiPropertyOptional({ description: 'Tipo de captura de la tarea' })
  @IsOptional()
  @IsString()
  field_type?: string;

  @ApiPropertyOptional({ description: 'Indica si la captura es obligatoria' })
  @IsOptional()
  @IsBoolean()
  required?: boolean;

  @ApiPropertyOptional({ description: 'Valor booleano' })
  @IsOptional()
  @IsBoolean()
  valor_boolean?: boolean;

  @ApiPropertyOptional({ description: 'Valor numérico', type: Number })
  @IsOptional()
  @Type(() => Number)
  @IsNumber()
  valor_numeric?: number;

  @ApiPropertyOptional({ description: 'Valor textual' })
  @IsOptional()
  @IsString()
  valor_text?: string;

  @ApiPropertyOptional({ description: 'Valor estructurado en json' })
  @IsOptional()
  @IsObject()
  valor_json?: Record<string, unknown>;

  @ApiPropertyOptional({ description: 'Metadatos de la tarea' })
  @IsOptional()
  @IsObject()
  task_meta?: Record<string, unknown>;

  @ApiPropertyOptional({
    description: 'Responsables y horas acumuladas por usuario',
    type: [WorkOrderTareaResponsableDto],
  })
  @IsOptional()
  @IsArray()
  @ValidateNested({ each: true })
  @Type(() => WorkOrderTareaResponsableDto)
  responsables?: WorkOrderTareaResponsableDto[];

  @ApiPropertyOptional({ description: 'Observación de la ejecución' })
  @IsOptional()
  @IsString()
  observacion?: string;
}

export class UpdateWorkOrderTareaDto {
  @ApiPropertyOptional({ description: 'Nombre libre de la tarea adicional' })
  @IsOptional()
  @IsString()
  actividad_adicional?: string;

  @ApiPropertyOptional({ description: 'Tipo de captura de la tarea' })
  @IsOptional()
  @IsString()
  field_type?: string;

  @ApiPropertyOptional({ description: 'Indica si la captura es obligatoria' })
  @IsOptional()
  @IsBoolean()
  required?: boolean;

  @ApiPropertyOptional({ description: 'Valor booleano' })
  @IsOptional()
  @IsBoolean()
  valor_boolean?: boolean;

  @ApiPropertyOptional({ description: 'Valor numérico', type: Number })
  @IsOptional()
  @Type(() => Number)
  @IsNumber()
  valor_numeric?: number;

  @ApiPropertyOptional({ description: 'Valor textual' })
  @IsOptional()
  @IsString()
  valor_text?: string;

  @ApiPropertyOptional({ description: 'Valor estructurado en json' })
  @IsOptional()
  @IsObject()
  valor_json?: Record<string, unknown>;

  @ApiPropertyOptional({ description: 'Metadatos de la tarea' })
  @IsOptional()
  @IsObject()
  task_meta?: Record<string, unknown>;

  @ApiPropertyOptional({
    description: 'Responsables y horas acumuladas por usuario',
    type: [WorkOrderTareaResponsableDto],
  })
  @IsOptional()
  @IsArray()
  @ValidateNested({ each: true })
  @Type(() => WorkOrderTareaResponsableDto)
  responsables?: WorkOrderTareaResponsableDto[];

  @ApiPropertyOptional({ description: 'Observación de la ejecución' })
  @IsOptional()
  @IsString()
  observacion?: string;

  @ApiPropertyOptional({ description: 'Estado de registro' })
  @IsOptional()
  @IsString()
  status?: string;
}
