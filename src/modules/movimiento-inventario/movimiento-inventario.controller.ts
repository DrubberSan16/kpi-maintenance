import { Controller } from '@nestjs/common';
import { ApiTags } from '@nestjs/swagger';
import { CrudController } from '../../common/crud/crud.controller';
import { MovimientoInventario } from '../entities/movimiento-inventario.entity';
import { MovimientoInventarioService } from './movimiento-inventario.service';

@ApiTags('movimientos-inventario')
@Controller('movimientos-inventario')
export class MovimientoInventarioController extends CrudController<MovimientoInventario> {
  constructor(protected readonly service: MovimientoInventarioService) {
    super(service);
  }
}
