import { Controller } from '@nestjs/common';
import { ApiTags } from '@nestjs/swagger';
import { CrudController } from '../../common/crud/crud.controller';
import { Producto } from '../entities/producto.entity';
import { ProductoService } from './producto.service';

@ApiTags('productos')
@Controller('productos')
export class ProductoController extends CrudController<Producto> {
  constructor(protected readonly service: ProductoService) {
    super(service);
  }
}
