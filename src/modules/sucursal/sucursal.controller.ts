import { Controller } from '@nestjs/common';
import { ApiTags } from '@nestjs/swagger';
import { CrudController } from '../../common/crud/crud.controller';
import { Sucursal } from '../entities/sucursal.entity';
import { SucursalService } from './sucursal.service';

@ApiTags('sucursales')
@Controller('sucursales')
export class SucursalController extends CrudController<Sucursal> {
  constructor(protected readonly service: SucursalService) {
    super(service);
  }
}
