import { Type } from 'class-transformer';
import {
  IsBoolean,
  IsDateString,
  IsEnum,
  IsNotEmpty,
  IsNumber,
  IsObject,
  IsOptional,
  IsString,
  IsUUID,
  Min,
  ValidateNested,
} from 'class-validator';
import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';

export class EquipoQueryDto {
  @ApiPropertyOptional({ description: 'Código del equipo' })
  @IsOptional()
  @IsString()
  codigo?: string;
  @ApiPropertyOptional({ description: 'ID de la ubicación', format: 'uuid' })
  @IsOptional()
  @IsUUID()
  location_id?: string;
  @ApiPropertyOptional({ description: 'ID del tipo de equipo', format: 'uuid' })
  @IsOptional()
  @IsUUID()
  equipo_tipo_id?: string;
  @ApiPropertyOptional({ description: 'ID de la marca', format: 'uuid' })
  @IsOptional()
  @IsUUID()
  marca_id?: string;
  @ApiPropertyOptional({ description: 'Estado operativo del equipo' })
  @IsOptional()
  @IsString()
  estado_operativo?: string;
  @ApiPropertyOptional({ description: 'Nivel de criticidad del equipo' })
  @IsOptional()
  @IsString()
  criticidad?: string;
  @ApiPropertyOptional({
    description: 'Página a consultar',
    minimum: 1,
    type: Number,
  })
  @IsOptional()
  @Type(() => Number)
  @IsNumber()
  @Min(1)
  page?: number;
  @ApiPropertyOptional({
    description: 'Cantidad de registros por página',
    minimum: 1,
    type: Number,
  })
  @IsOptional()
  @Type(() => Number)
  @IsNumber()
  @Min(1)
  limit?: number;
}

export class CreateEquipoDto {
  @ApiProperty({ description: 'Código único del equipo' })
  @IsString()
  @IsNotEmpty()
  codigo: string;
  @ApiProperty({ description: 'Nombre del equipo' })
  @IsString()
  @IsNotEmpty()
  nombre: string;
  @ApiProperty({ description: 'ID del tipo de equipo', format: 'uuid' })
  @IsUUID()
  equipo_tipo_id: string;
  @ApiPropertyOptional({ description: 'ID de la ubicación', format: 'uuid' })
  @IsOptional()
  @IsUUID()
  location_id?: string;
  @ApiPropertyOptional({ description: 'ID de la marca', format: 'uuid' })
  @IsOptional()
  @IsUUID()
  marca_id?: string;
  @ApiPropertyOptional({ description: 'Nivel de criticidad' })
  @IsOptional()
  @IsString()
  criticidad?: string;
  @ApiPropertyOptional({ description: 'Estado operativo actual' })
  @IsOptional()
  @IsString()
  estado_operativo?: string;
  @ApiPropertyOptional({ description: 'Horómetro actual', type: Number })
  @IsOptional()
  @Type(() => Number)
  @IsNumber()
  horometro_actual?: number;
}

export class UpdateEquipoDto extends CreateEquipoDto {}

export class CreateEquipoTipoDto {
  @ApiProperty({ description: 'Código único del Tipo de Equipo' })
  @IsString()
  @IsNotEmpty()
  codigo: string;
  @ApiProperty({ description: 'Nombre del Tipo de Equipo' })
  @IsString()
  @IsNotEmpty()
  nombre: string;
  @ApiPropertyOptional({ description: 'Descripcion del tipo de equipo' })
  @IsOptional()
  @IsString()
  descripcion?: string;
  @ApiPropertyOptional({
    description: 'ID de usuario que registra',
    format: 'uuid',
  })
  @IsOptional()
  @IsUUID()
  registrado_por?: string;
}

export class UpdateEquipoTipoDto extends CreateEquipoTipoDto {}

export class EquipoTipoQueryDto {
  @ApiPropertyOptional({ description: 'Código del tipo de equipo' })
  @IsOptional()
  @IsString()
  codigo?: string;
  @ApiPropertyOptional({ description: 'Nombre del tipo de equipo' })
  @IsOptional()
  @IsString()
  nombre?: string;
  @ApiPropertyOptional({
    description: 'Página a consultar',
    minimum: 1,
    type: Number,
  })
  @IsOptional()
  @Type(() => Number)
  @IsNumber()
  @Min(1)
  page?: number;
  @ApiPropertyOptional({
    description: 'Cantidad de registros por página',
    minimum: 1,
    type: Number,
  })
  @IsOptional()
  @Type(() => Number)
  @IsNumber()
  @Min(1)
  limit?: number;
}

export class LocationQueryDto {
  @ApiPropertyOptional({ description: 'Código de la ubicación' })
  @IsOptional()
  @IsString()
  codigo?: string;
  @ApiPropertyOptional({ description: 'Nombre de la ubicación' })
  @IsOptional()
  @IsString()
  nombre?: string;
  @ApiPropertyOptional({
    description: 'Página a consultar',
    minimum: 1,
    type: Number,
  })
  @IsOptional()
  @Type(() => Number)
  @IsNumber()
  @Min(1)
  page?: number;
  @ApiPropertyOptional({
    description: 'Cantidad de registros por página',
    minimum: 1,
    type: Number,
  })
  @IsOptional()
  @Type(() => Number)
  @IsNumber()
  @Min(1)
  limit?: number;
}

export class CreateLocationDto {
  @ApiProperty({ description: 'Código único de la ubicación' })
  @IsString()
  @IsNotEmpty()
  codigo: string;
  @ApiProperty({ description: 'Nombre de la ubicación' })
  @IsString()
  @IsNotEmpty()
  nombre: string;
  @ApiPropertyOptional({ description: 'Descripción de la ubicación' })
  @IsOptional()
  @IsString()
  descripcion?: string;
}

export class UpdateLocationDto extends CreateLocationDto {}

export class CreateBitacoraDto {
  @ApiProperty({ description: 'Fecha del registro en formato ISO 8601' })
  @IsDateString()
  fecha: string;
  @ApiProperty({ description: 'Lectura del horómetro', type: Number })
  @Type(() => Number)
  @IsNumber()
  horometro: number;
  @ApiPropertyOptional({
    description: 'ID del estado asociado',
    format: 'uuid',
  })
  @IsOptional()
  @IsUUID()
  estado_id?: string;
  @ApiPropertyOptional({ description: 'Observaciones del registro' })
  @IsOptional()
  @IsString()
  observaciones?: string;
  @ApiPropertyOptional({
    description: 'ID de usuario que registra',
    format: 'uuid',
  })
  @IsOptional()
  @IsUUID()
  registrado_por?: string;
}

export class UpdateBitacoraDto extends CreateBitacoraDto {}

export class DateRangeDto {
  @ApiPropertyOptional({ description: 'Fecha de inicio del rango (ISO 8601)' })
  @IsOptional()
  @IsDateString()
  from?: string;
  @ApiPropertyOptional({ description: 'Fecha de fin del rango (ISO 8601)' })
  @IsOptional()
  @IsDateString()
  to?: string;
}

export class ChangeEstadoDto {
  @ApiProperty({ description: 'ID del estado a establecer', format: 'uuid' })
  @IsUUID()
  estado_id: string;
  @ApiProperty({ description: 'Fecha de inicio del estado (ISO 8601)' })
  @IsDateString()
  fecha_inicio: string;
  @ApiPropertyOptional({ description: 'Motivo del cambio de estado' })
  @IsOptional()
  @IsString()
  motivo?: string;
}

export class CreateEventoDto {
  @ApiProperty({
    enum: [
      'FALLA',
      'PARADA',
      'CAMBIO_UNIDAD',
      'INSPECCION',
      'OBSERVACION',
      'ALARMA',
    ],
    description: 'Tipo de evento',
  })
  @IsEnum([
    'FALLA',
    'PARADA',
    'CAMBIO_UNIDAD',
    'INSPECCION',
    'OBSERVACION',
    'ALARMA',
  ])
  tipo_evento: string;
  @ApiPropertyOptional({
    description: 'ID de la orden de trabajo relacionada',
    format: 'uuid',
  })
  @IsOptional()
  @IsUUID()
  work_order_id?: string;
  @ApiPropertyOptional({ description: 'Fecha de inicio del evento (ISO 8601)' })
  @IsOptional()
  @IsDateString()
  fecha_inicio?: string;
  @ApiPropertyOptional({ description: 'Fecha de fin del evento (ISO 8601)' })
  @IsOptional()
  @IsDateString()
  fecha_fin?: string;
  @ApiPropertyOptional({ description: 'Severidad del evento', type: Number })
  @IsOptional()
  @Type(() => Number)
  @IsNumber()
  severidad?: number;
  @ApiPropertyOptional({ description: 'Descripción del evento' })
  @IsOptional()
  @IsString()
  descripcion?: string;
}

export class CreatePlanDto {
  @ApiProperty({ description: 'Código del plan' })
  @IsString()
  codigo: string;
  @ApiProperty({ description: 'Nombre del plan' })
  @IsString()
  nombre: string;
  @ApiPropertyOptional({ description: 'Tipo de plan' })
  @IsOptional()
  @IsString()
  tipo?: string;
  @ApiPropertyOptional({ description: 'Tipo de frecuencia' })
  @IsOptional()
  @IsString()
  frecuencia_tipo?: string;
  @ApiPropertyOptional({ description: 'Valor de la frecuencia', type: Number })
  @IsOptional()
  @Type(() => Number)
  @IsNumber()
  frecuencia_valor?: number;
}
export class UpdatePlanDto extends CreatePlanDto {}

export class CreatePlanTareaDto {
  @ApiProperty({ description: 'Orden de ejecución de la tarea', type: Number })
  @Type(() => Number)
  @IsNumber()
  orden: number;
  @ApiProperty({ description: 'Actividad a realizar' })
  @IsString()
  actividad: string;
  @ApiPropertyOptional({ description: 'Tipo de campo de captura asociado' })
  @IsOptional()
  @IsString()
  field_type?: string;
}
export class UpdatePlanTareaDto extends CreatePlanTareaDto {}

export class CreateProgramacionDto {
  @ApiProperty({ description: 'ID del equipo', format: 'uuid' })
  @IsUUID()
  equipo_id: string;
  @ApiProperty({ description: 'ID del plan', format: 'uuid' })
  @IsUUID()
  plan_id: string;
  @ApiPropertyOptional({ description: 'Última fecha de ejecución (ISO 8601)' })
  @IsOptional()
  @IsDateString()
  ultima_ejecucion_fecha?: string;
  @ApiPropertyOptional({
    description: 'Última ejecución en horas',
    type: Number,
  })
  @IsOptional()
  @Type(() => Number)
  @IsNumber()
  ultima_ejecucion_horas?: number;
  @ApiPropertyOptional({ description: 'Próxima fecha estimada (ISO 8601)' })
  @IsOptional()
  @IsDateString()
  proxima_fecha?: string;
  @ApiPropertyOptional({
    description: 'Próxima ejecución en horas',
    type: Number,
  })
  @IsOptional()
  @Type(() => Number)
  @IsNumber()
  proxima_horas?: number;
  @ApiPropertyOptional({ description: 'Indica si la programación está activa' })
  @IsOptional()
  activo?: boolean;
}
export class UpdateProgramacionDto extends CreateProgramacionDto {}

export class ComponenteQueryDto {
  @ApiPropertyOptional({ description: 'ID del equipo', format: 'uuid' })
  @IsOptional()
  @IsUUID()
  equipo_id?: string;
}

export class CreateComponenteDto {
  @ApiProperty({ description: 'ID del equipo', format: 'uuid' })
  @IsUUID()
  equipo_id: string;
  @ApiPropertyOptional({
    description: 'ID del componente padre',
    format: 'uuid',
  })
  @IsOptional()
  @IsUUID()
  parent_id?: string;
  @ApiPropertyOptional({ description: 'Código del componente' })
  @IsOptional()
  @IsString()
  codigo?: string;
  @ApiProperty({ description: 'Nombre del componente' })
  @IsString()
  @IsNotEmpty()
  nombre: string;
  @ApiPropertyOptional({ description: 'Descripción del componente' })
  @IsOptional()
  @IsString()
  descripcion?: string;
}

export class UpdateComponenteDto extends CreateComponenteDto {}

export class CreateFallaCatalogoDto {
  @ApiPropertyOptional({ description: 'Código de falla' })
  @IsOptional()
  @IsString()
  codigo?: string;
  @ApiProperty({ description: 'Síntoma' })
  @IsString()
  @IsNotEmpty()
  sintoma: string;
  @ApiPropertyOptional({ description: 'Causa' })
  @IsOptional()
  @IsString()
  causa?: string;
  @ApiPropertyOptional({ description: 'Acción recomendada' })
  @IsOptional()
  @IsString()
  accion_recomendada?: string;
  @ApiPropertyOptional({ description: 'Tipo de equipo', format: 'uuid' })
  @IsOptional()
  @IsUUID()
  equipo_tipo_id?: string;
  @ApiPropertyOptional({ description: 'Severidad' })
  @IsOptional()
  @IsString()
  severidad?: string;
}

export class UpdateFallaCatalogoDto extends CreateFallaCatalogoDto {}

export class CreateLecturaEquipoDto {
  @ApiProperty({ description: 'ID del equipo', format: 'uuid' })
  @IsUUID()
  equipo_id: string;
  @ApiProperty({ description: 'Tipo de lectura' })
  @IsString()
  @IsNotEmpty()
  tipo: string;
  @ApiPropertyOptional({ description: 'Fecha (ISO)' })
  @IsOptional()
  @IsDateString()
  fecha?: string;
  @ApiPropertyOptional({ description: 'Valor numérico', type: Number })
  @IsOptional()
  @Type(() => Number)
  @IsNumber()
  valor?: number;
  @ApiPropertyOptional({ description: 'Valor texto' })
  @IsOptional()
  @IsString()
  valor_texto?: string;
  @ApiPropertyOptional({ description: 'Unidad' })
  @IsOptional()
  @IsString()
  unidad?: string;
  @ApiPropertyOptional({
    description: 'ID de usuario que toma lectura',
    format: 'uuid',
  })
  @IsOptional()
  @IsUUID()
  tomado_por?: string;
  @ApiPropertyOptional({ description: 'Observación' })
  @IsOptional()
  @IsString()
  observacion?: string;
}

export class UpdateLecturaEquipoDto extends CreateLecturaEquipoDto {}

export class CreateLubricacionPuntoDto {
  @ApiProperty({ description: 'ID del equipo', format: 'uuid' })
  @IsUUID()
  equipo_id: string;
  @ApiPropertyOptional({ description: 'ID del componente', format: 'uuid' })
  @IsOptional()
  @IsUUID()
  componente_id?: string;
  @ApiProperty({ description: 'Nombre del punto' })
  @IsString()
  @IsNotEmpty()
  nombre: string;
  @ApiPropertyOptional({ description: 'Lubricante' })
  @IsOptional()
  @IsString()
  lubricante?: string;
  @ApiPropertyOptional({ description: 'Cantidad recomendada', type: Number })
  @IsOptional()
  @Type(() => Number)
  @IsNumber()
  cantidad_recomendada?: number;
  @ApiPropertyOptional({ description: 'Unidad' })
  @IsOptional()
  @IsString()
  unidad?: string;
  @ApiPropertyOptional({ description: 'Tipo de frecuencia' })
  @IsOptional()
  @IsString()
  frecuencia_tipo?: string;
  @ApiPropertyOptional({ description: 'Valor de frecuencia', type: Number })
  @IsOptional()
  @Type(() => Number)
  @IsNumber()
  frecuencia_valor?: number;
}

export class UpdateLubricacionPuntoDto extends CreateLubricacionPuntoDto {}

export class AlertaQueryDto {
  @ApiPropertyOptional({ description: 'Estado de la alerta' })
  @IsOptional()
  @IsString()
  estado?: string;
  @ApiPropertyOptional({ description: 'Tipo de alerta' })
  @IsOptional()
  @IsString()
  tipo_alerta?: string;
  @ApiPropertyOptional({ description: 'ID del equipo', format: 'uuid' })
  @IsOptional()
  @IsUUID()
  equipo_id?: string;
}

export class WorkOrderQueryDto {
  @ApiPropertyOptional({ description: 'ID del equipo', format: 'uuid' })
  @IsOptional()
  @IsUUID()
  equipo_id?: string;
  @ApiPropertyOptional({ description: 'Estado de la orden de trabajo' })
  @IsOptional()
  @IsString()
  status_workflow?: string;
  @ApiPropertyOptional({ description: 'Tipo de mantenimiento' })
  @IsOptional()
  @IsString()
  maintenance_kind?: string;
}

export class CreateWorkOrderDto {
  @ApiProperty({ description: 'Código único de OT' })
  @IsString()
  @IsNotEmpty()
  code: string;
  @ApiProperty({ description: 'Tipo de orden de trabajo' })
  @IsString()
  @IsNotEmpty()
  type: string;
  @ApiPropertyOptional({ description: 'ID del equipo', format: 'uuid' })
  @IsOptional()
  @IsUUID()
  equipment_id?: string;
  @ApiPropertyOptional({ description: 'ID del plan', format: 'uuid' })
  @IsOptional()
  @IsUUID()
  plan_id?: string;
  @ApiPropertyOptional({ description: 'Valor estructurado en json' })
  @IsOptional()
  @IsObject()
  valor_json?: Record<string, unknown>;
  @ApiProperty({ description: 'Título de la OT' })
  @IsString()
  @IsNotEmpty()
  title: string;
  @ApiPropertyOptional({ description: 'Descripción de la OT' })
  @IsOptional()
  @IsString()
  description?: string;
  @ApiPropertyOptional({ description: 'Estado de workflow inicial' })
  @IsOptional()
  @IsString()
  status_workflow?: string;
  @ApiPropertyOptional({ description: 'Prioridad', type: Number })
  @IsOptional()
  @Type(() => Number)
  @IsNumber()
  priority?: number;
  @ApiPropertyOptional({ description: 'Tipo de proveedor' })
  @IsOptional()
  @IsString()
  provider_type?: string;
  @ApiPropertyOptional({ description: 'Tipo de mantenimiento' })
  @IsOptional()
  @IsString()
  maintenance_kind?: string;
  @ApiPropertyOptional({ description: 'Requiere permiso de seguridad' })
  @IsOptional()
  @IsBoolean()
  safety_permit_required?: boolean;
  @ApiPropertyOptional({ description: 'Código permiso de seguridad' })
  @IsOptional()
  @IsString()
  safety_permit_code?: string;
  @ApiPropertyOptional({
    description: 'ID de tercero proveedor',
    format: 'uuid',
  })
  @IsOptional()
  @IsUUID()
  vendor_id?: string;
  @ApiPropertyOptional({
    description: 'ID de solicitud de compra',
    format: 'uuid',
  })
  @IsOptional()
  @IsUUID()
  purchase_request_id?: string;
  @ApiPropertyOptional({
    description: 'ID de la alerta que origina la OT',
    format: 'uuid',
  })
  @IsOptional()
  @IsUUID()
  alerta_id?: string;
}

export class UpdateWorkOrderDto {
  @ApiPropertyOptional({ description: 'Estado de workflow de la OT' })
  @IsOptional()
  @IsString()
  status_workflow?: string;
  @ApiPropertyOptional({ description: 'Tipo de mantenimiento de la OT' })
  @IsOptional()
  @IsString()
  maintenance_kind?: string;
  @ApiPropertyOptional({ description: 'Prioridad', type: Number })
  @IsOptional()
  @Type(() => Number)
  @IsNumber()
  priority?: number;
  @ApiPropertyOptional({
    description: 'ID de responsable asignado',
    format: 'uuid',
  })
  @IsOptional()
  @IsUUID()
  assigned_to?: string;
  @ApiPropertyOptional({ description: 'Fecha inicio programada (ISO 8601)' })
  @IsOptional()
  @IsDateString()
  scheduled_start?: string;
  @ApiPropertyOptional({ description: 'Fecha fin programada (ISO 8601)' })
  @IsOptional()
  @IsDateString()
  scheduled_end?: string;
  @ApiPropertyOptional({ description: 'Fecha inicio real (ISO 8601)' })
  @IsOptional()
  @IsDateString()
  started_at?: string;
  @ApiPropertyOptional({ description: 'Fecha cierre real (ISO 8601)' })
  @IsOptional()
  @IsDateString()
  closed_at?: string;  
  @ApiPropertyOptional({ description: 'Valor estructurado en json' })
  @IsOptional()
  @IsObject()
  valor_json?: Record<string, unknown>;
}

export class CreateConsumoDto {
  @ApiProperty({ description: 'ID del producto consumido', format: 'uuid' })
  @IsUUID()
  producto_id: string;
  @ApiProperty({ description: 'ID de la bodega', format: 'uuid' })
  @IsUUID()
  bodega_id: string;
  @ApiProperty({
    description: 'Cantidad consumida',
    type: Number,
    minimum: 0.000001,
  })
  @Type(() => Number)
  @IsNumber()
  @Min(0.000001)
  cantidad: number;
  @ApiProperty({
    description: 'Costo unitario del consumo',
    type: Number,
    minimum: 0,
  })
  @Type(() => Number)
  @IsNumber()
  @Min(0)
  costo_unitario: number;
  @ApiPropertyOptional({ description: 'Observación del consumo' })
  @IsOptional()
  @IsString()
  observacion?: string;
}

export class IssueMaterialItemDto {
  @ApiProperty({ description: 'ID del producto a descontar', format: 'uuid' })
  @IsUUID()
  producto_id: string;
  @ApiProperty({ description: 'ID de la bodega origen', format: 'uuid' })
  @IsUUID()
  bodega_id: string;
  @ApiProperty({
    description: 'Cantidad a descontar',
    type: Number,
    minimum: 0.0001,
  })
  @Type(() => Number)
  @IsNumber()
  @Min(0.0001)
  cantidad: number;
}

export class IssueMaterialsDto {
  @ApiProperty({
    description: 'Listado de materiales a consumir',
    type: [IssueMaterialItemDto],
  })
  @ValidateNested({ each: true })
  @Type(() => IssueMaterialItemDto)
  items: IssueMaterialItemDto[];
  @ApiPropertyOptional({
    description: 'Observación general de la salida de materiales',
  })
  @IsOptional()
  @IsString()
  observacion?: string;
}

export class CreateWorkOrderTareaDto {
  @ApiProperty({ description: 'ID del plan', format: 'uuid' })
  @IsUUID()
  plan_id: string;
  @ApiProperty({ description: 'ID de la tarea de plan', format: 'uuid' })
  @IsUUID()
  tarea_id: string;
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
  @ApiPropertyOptional({ description: 'Observación de la ejecución' })
  @IsOptional()
  @IsString()
  observacion?: string;
}

export class UpdateWorkOrderTareaDto {
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
  @ApiPropertyOptional({ description: 'Observación de la ejecución' })
  @IsOptional()
  @IsString()
  observacion?: string;
  @ApiPropertyOptional({ description: 'Estado de registro' })
  @IsOptional()
  @IsString()
  status?: string;
}

export class UploadWorkOrderAdjuntoDto {
  @ApiPropertyOptional({ description: 'Tipo de adjunto', default: 'EVIDENCIA' })
  @IsOptional()
  @IsString()
  tipo?: string;
  @ApiProperty({ description: 'Nombre original del archivo (ej. foto1.jpg)' })
  @IsString()
  @IsNotEmpty()
  nombre: string;
  @ApiProperty({ description: 'Contenido codificado en base64 (sin data-url)' })
  @IsString()
  @IsNotEmpty()
  contenido_base64: string;
  @ApiPropertyOptional({
    description: 'Mime type del archivo (ej. image/jpeg, application/pdf)',
  })
  @IsOptional()
  @IsString()
  mime_type?: string;
  @ApiPropertyOptional({ description: 'Metadatos del archivo en formato json' })
  @IsOptional()
  @IsObject()
  meta?: Record<string, unknown>;
}

export class WorkOrderAdjuntoQueryDto {
  @ApiPropertyOptional({ description: 'Filtrar por tipo de adjunto' })
  @IsOptional()
  @IsString()
  tipo?: string;
}
