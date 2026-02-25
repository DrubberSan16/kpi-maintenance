import { Controller } from '@nestjs/common';
import { CrudController } from '../../common/crud/crud.controller';
import { Bodega } from '../entities/bodega.entity';
import { BodegaService } from './bodega.service';

@Controller('bodegas')
export class BodegaController extends CrudController<Bodega> {
  constructor(protected readonly service: BodegaService) {
    super(service);
  }
}
