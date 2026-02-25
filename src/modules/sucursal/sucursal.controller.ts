import { Controller } from '@nestjs/common';
import { CrudController } from '../../common/crud/crud.controller';
import { Sucursal } from '../entities/sucursal.entity';
import { SucursalService } from './sucursal.service';

@Controller('sucursales')
export class SucursalController extends CrudController<Sucursal> {
  constructor(protected readonly service: SucursalService) {
    super(service);
  }
}
