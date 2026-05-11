import { Type } from 'class-transformer';
import {
  IsArray,
  IsBoolean,
  IsObject,
  IsOptional,
  IsString,
  IsUUID,
  ValidateNested,
} from 'class-validator';
import { ApiPropertyOptional } from '@nestjs/swagger';
import {
  CreateConsumoDto,
  IssueMaterialsDto,
  UploadWorkOrderAdjuntoDto,
} from '../dto';
import {
  CreateWorkOrderTareaDto,
  UpdateWorkOrderTareaDto,
} from './work-order-task.dto';

export class SaveWorkOrderHeaderDto {
  @ApiPropertyOptional({ description: 'Codigo de la OT' })
  @IsOptional()
  @IsString()
  code?: string | null;

  @ApiPropertyOptional({ description: 'Tipo de OT' })
  @IsOptional()
  @IsString()
  type?: string | null;

  @ApiPropertyOptional({ description: 'Titulo de la OT' })
  @IsOptional()
  @IsString()
  title?: string | null;

  @ApiPropertyOptional({ description: 'Descripcion de la OT' })
  @IsOptional()
  @IsString()
  description?: string | null;

  @ApiPropertyOptional({ description: 'ID del equipo', format: 'uuid' })
  @IsOptional()
  @IsUUID()
  equipment_id?: string | null;

  @ApiPropertyOptional({
    description: 'ID del compartimiento del equipo',
    format: 'uuid',
  })
  @IsOptional()
  @IsUUID()
  equipo_componente_id?: string | null;

  @ApiPropertyOptional({ description: 'Estado workflow de la OT' })
  @IsOptional()
  @IsString()
  status_workflow?: string | null;

  @ApiPropertyOptional({ description: 'Tipo de mantenimiento' })
  @IsOptional()
  @IsString()
  maintenance_kind?: string | null;

  @ApiPropertyOptional({ description: 'ID del plan', format: 'uuid' })
  @IsOptional()
  @IsUUID()
  plan_id?: string | null;

  @ApiPropertyOptional({ description: 'ID del procedimiento', format: 'uuid' })
  @IsOptional()
  @IsUUID()
  procedimiento_id?: string | null;

  @ApiPropertyOptional({ description: 'ID de alerta vinculada', format: 'uuid' })
  @IsOptional()
  @IsUUID()
  alerta_id?: string | null;

  @ApiPropertyOptional({
    description: 'ID de OT bloqueante o anexada',
    format: 'uuid',
  })
  @IsOptional()
  @IsUUID()
  blocked_by_work_order_id?: string | null;

  @ApiPropertyOptional({ description: 'Motivo de bloqueo' })
  @IsOptional()
  @IsString()
  blocked_reason?: string | null;

  @ApiPropertyOptional({ description: 'Prioridad' })
  @IsOptional()
  priority?: number | null;

  @ApiPropertyOptional({ description: 'Tipo de proveedor' })
  @IsOptional()
  @IsString()
  provider_type?: string | null;

  @ApiPropertyOptional({ description: 'Requiere permiso de seguridad' })
  @IsOptional()
  @IsBoolean()
  safety_permit_required?: boolean | null;

  @ApiPropertyOptional({ description: 'Codigo de permiso de seguridad' })
  @IsOptional()
  @IsString()
  safety_permit_code?: string | null;

  @ApiPropertyOptional({ description: 'ID de proveedor', format: 'uuid' })
  @IsOptional()
  @IsUUID()
  vendor_id?: string | null;

  @ApiPropertyOptional({
    description: 'ID de solicitud de compra',
    format: 'uuid',
  })
  @IsOptional()
  @IsUUID()
  purchase_request_id?: string | null;

  @ApiPropertyOptional({ description: 'Payload adicional de cabecera' })
  @IsOptional()
  @IsObject()
  valor_json?: Record<string, unknown> | null;
}

export class SaveWorkOrderAttachmentDto extends UploadWorkOrderAdjuntoDto {
  @ApiPropertyOptional({
    description: 'Identificador temporal local para enlazar evidencias',
  })
  @IsOptional()
  @IsString()
  temp_id?: string | null;
}

export class SaveWorkOrderTaskUpdateDto extends UpdateWorkOrderTareaDto {
  @ApiPropertyOptional({ description: 'ID de la tarea de OT', format: 'uuid' })
  @IsUUID()
  id: string;
}

export class SaveWorkOrderBundleDto {
  @ApiPropertyOptional({ type: SaveWorkOrderHeaderDto })
  @IsOptional()
  @ValidateNested()
  @Type(() => SaveWorkOrderHeaderDto)
  header?: SaveWorkOrderHeaderDto;

  @ApiPropertyOptional({
    description: 'Listado de nuevas tareas a persistir',
    type: [CreateWorkOrderTareaDto],
  })
  @IsOptional()
  @IsArray()
  @ValidateNested({ each: true })
  @Type(() => CreateWorkOrderTareaDto)
  tareas_nuevas?: CreateWorkOrderTareaDto[];

  @ApiPropertyOptional({
    description: 'Listado de tareas existentes a actualizar',
    type: [SaveWorkOrderTaskUpdateDto],
  })
  @IsOptional()
  @IsArray()
  @ValidateNested({ each: true })
  @Type(() => SaveWorkOrderTaskUpdateDto)
  tareas_editadas?: SaveWorkOrderTaskUpdateDto[];

  @ApiPropertyOptional({
    description: 'Listado de nuevos adjuntos pendientes',
    type: [SaveWorkOrderAttachmentDto],
  })
  @IsOptional()
  @IsArray()
  @ValidateNested({ each: true })
  @Type(() => SaveWorkOrderAttachmentDto)
  adjuntos_nuevos?: SaveWorkOrderAttachmentDto[];

  @ApiPropertyOptional({
    description: 'Consumo pendiente por persistir',
    type: CreateConsumoDto,
  })
  @IsOptional()
  @ValidateNested()
  @Type(() => CreateConsumoDto)
  consumo_pendiente?: CreateConsumoDto | null;

  @ApiPropertyOptional({
    description: 'Salida de materiales pendiente por persistir',
    type: IssueMaterialsDto,
  })
  @IsOptional()
  @ValidateNested()
  @Type(() => IssueMaterialsDto)
  salida_materiales_pendiente?: IssueMaterialsDto | null;
}
