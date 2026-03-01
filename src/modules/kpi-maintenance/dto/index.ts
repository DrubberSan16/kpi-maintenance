import { Type } from 'class-transformer';
import { IsDateString, IsEnum, IsNotEmpty, IsNumber, IsOptional, IsString, IsUUID, Min, ValidateNested } from 'class-validator';

export class EquipoQueryDto {
  @IsOptional() @IsString() codigo?: string;
  @IsOptional() @IsUUID() location_id?: string;
  @IsOptional() @IsUUID() equipo_tipo_id?: string;
  @IsOptional() @IsString() estado_operativo?: string;
  @IsOptional() @IsString() criticidad?: string;
  @IsOptional() @Type(() => Number) @IsNumber() @Min(1) page?: number;
  @IsOptional() @Type(() => Number) @IsNumber() @Min(1) limit?: number;
}

export class CreateEquipoDto {
  @IsString() @IsNotEmpty() codigo: string;
  @IsString() @IsNotEmpty() nombre: string;
  @IsUUID() equipo_tipo_id: string;
  @IsOptional() @IsUUID() location_id?: string;
  @IsOptional() @IsString() criticidad?: string;
  @IsOptional() @IsString() estado_operativo?: string;
  @IsOptional() @Type(() => Number) @IsNumber() horometro_actual?: number;
}

export class UpdateEquipoDto extends CreateEquipoDto {}

export class CreateBitacoraDto {
  @IsDateString() fecha: string;
  @Type(() => Number) @IsNumber() horometro: number;
  @IsOptional() @IsUUID() estado_id?: string;
  @IsOptional() @IsString() observaciones?: string;
  @IsOptional() @IsUUID() registrado_por?: string;
}

export class UpdateBitacoraDto extends CreateBitacoraDto {}

export class DateRangeDto {
  @IsOptional() @IsDateString() from?: string;
  @IsOptional() @IsDateString() to?: string;
}

export class ChangeEstadoDto {
  @IsUUID() estado_id: string;
  @IsDateString() fecha_inicio: string;
  @IsOptional() @IsString() motivo?: string;
}

export class CreateEventoDto {
  @IsEnum(['FALLA', 'PARADA', 'CAMBIO_UNIDAD', 'INSPECCION', 'OBSERVACION', 'ALARMA']) tipo_evento: string;
  @IsOptional() @IsUUID() work_order_id?: string;
  @IsOptional() @IsDateString() fecha_inicio?: string;
  @IsOptional() @IsDateString() fecha_fin?: string;
  @IsOptional() @Type(() => Number) @IsNumber() severidad?: number;
  @IsOptional() @IsString() descripcion?: string;
}

export class CreatePlanDto {
  @IsString() codigo: string;
  @IsString() nombre: string;
  @IsOptional() @IsString() tipo?: string;
  @IsOptional() @IsString() frecuencia_tipo?: string;
  @IsOptional() @Type(() => Number) @IsNumber() frecuencia_valor?: number;
}
export class UpdatePlanDto extends CreatePlanDto {}

export class CreatePlanTareaDto {
  @Type(() => Number) @IsNumber() orden: number;
  @IsString() actividad: string;
  @IsOptional() @IsString() field_type?: string;
}
export class UpdatePlanTareaDto extends CreatePlanTareaDto {}

export class CreateProgramacionDto {
  @IsUUID() equipo_id: string;
  @IsUUID() plan_id: string;
  @IsOptional() @IsDateString() ultima_ejecucion_fecha?: string;
  @IsOptional() @Type(() => Number) @IsNumber() ultima_ejecucion_horas?: number;
  @IsOptional() @IsDateString() proxima_fecha?: string;
  @IsOptional() @Type(() => Number) @IsNumber() proxima_horas?: number;
  @IsOptional() activo?: boolean;
}
export class UpdateProgramacionDto extends CreateProgramacionDto {}

export class AlertaQueryDto {
  @IsOptional() @IsString() estado?: string;
  @IsOptional() @IsString() tipo_alerta?: string;
  @IsOptional() @IsUUID() equipo_id?: string;
}

export class WorkOrderQueryDto {
  @IsOptional() @IsUUID() equipo_id?: string;
  @IsOptional() @IsString() estado?: string;
  @IsOptional() @IsString() maintenance_kind?: string;
}

export class CreateConsumoDto {
  @IsUUID() producto_id: string;
  @IsOptional() @IsUUID() bodega_id?: string;
  @Type(() => Number) @IsNumber() @Min(0.000001) cantidad: number;
  @Type(() => Number) @IsNumber() @Min(0) costo_unitario: number;
  @IsOptional() @IsString() observacion?: string;
}

export class IssueMaterialItemDto {
  @IsUUID() producto_id: string;
  @IsUUID() bodega_id: string;
  @Type(() => Number) @IsNumber() @Min(0.0001) cantidad: number;
}

export class IssueMaterialsDto {
  @ValidateNested({ each: true }) @Type(() => IssueMaterialItemDto) items: IssueMaterialItemDto[];
  @IsOptional() @IsString() observacion?: string;
}
