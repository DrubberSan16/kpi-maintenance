import { Controller } from '@nestjs/common';

import { ApiTags } from '@nestjs/swagger';
import { CrudController } from '../../common/crud/crud.controller';
import { Categoria } from '../entities/categoria.entity';
import { CategoriaService } from './categoria.service';

@ApiTags('categorias')
@Controller('categorias')
export class CategoriaController extends CrudController<Categoria> {
  constructor(protected readonly service: CategoriaService) {
    super(service);
  }
}
