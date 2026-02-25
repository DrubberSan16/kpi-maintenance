import { Body, Delete, Get, Param, Patch, Post, Query } from '@nestjs/common';
import { PaginationQueryDto } from '../dto/pagination-query.dto';
import { CrudService } from './crud.service';

export abstract class CrudController<
  T extends {
    id: string;
    is_deleted: boolean;
    deleted_at?: Date | null;
    deleted_by?: string | null;
  },
> {
  constructor(protected readonly service: CrudService<T>) {}

  @Post()
  create(@Body() payload: Record<string, unknown>) {
    return this.service.create(payload as never);
  }

  @Get()
  findAll(@Query() query: PaginationQueryDto) {
    return this.service.findAll(query.page, query.limit);
  }

  @Get(':id')
  findOne(@Param('id') id: string) {
    return this.service.findOne(id);
  }

  @Patch(':id')
  update(@Param('id') id: string, @Body() payload: Record<string, unknown>) {
    return this.service.update(id, payload as never);
  }

  @Delete(':id')
  remove(@Param('id') id: string) {
    return this.service.remove(id);
  }
}
