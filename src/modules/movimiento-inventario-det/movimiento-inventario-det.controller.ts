import { Controller } from '@nestjs/common';
import { ApiTags } from '@nestjs/swagger';
import { CrudController } from '../../common/crud/crud.controller';
import { MovimientoInventarioDet } from '../entities/movimiento-inventario-det.entity';
import { MovimientoInventarioDetService } from './movimiento-inventario-det.service';


@ApiTags('movimientos-inventario-det')
@Controller('movimientos-inventario-det')
export class MovimientoInventarioDetController extends CrudController<MovimientoInventarioDet> {
  constructor(protected readonly service: MovimientoInventarioDetService) {
    super(service);
  }
}
