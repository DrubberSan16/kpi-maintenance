import { Type } from '@nestjs/common';
import { OmitType, PartialType } from '@nestjs/swagger';

export const AUDIT_FIELDS = [
  'id',
  'created_at',
  'updated_at',
  'created_by',
  'updated_by',
  'is_deleted',
  'deleted_at',
  'deleted_by',
] as const;

export function buildCrudRequestDtos<T extends Type<object>>(entity: T) {
  class CreateDto extends OmitType(entity, AUDIT_FIELDS as never) {}
  class UpdateDto extends PartialType(CreateDto) {}

  return { CreateDto, UpdateDto };
}
