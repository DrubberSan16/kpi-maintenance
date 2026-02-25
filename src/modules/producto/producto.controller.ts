import { Controller } from '@nestjs/common';
import { CrudController } from '../../common/crud/crud.controller';
import { Producto } from '../entities/producto.entity';
import { ProductoService } from './producto.service';

@Controller('productos')
export class ProductoController extends CrudController<Producto> {
  constructor(protected readonly service: ProductoService) {
    super(service);
  }
}
