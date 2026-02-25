import { Controller } from '@nestjs/common';
import { CrudController } from '../../common/crud/crud.controller';
import { Categoria } from '../entities/categoria.entity';
import { CategoriaService } from './categoria.service';

@Controller('categorias')
export class CategoriaController extends CrudController<Categoria> {
  constructor(protected readonly service: CategoriaService) {
    super(service);
  }
}
