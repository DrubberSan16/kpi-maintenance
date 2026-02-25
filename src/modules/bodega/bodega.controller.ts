import { Controller } from '@nestjs/common';
import { ApiTags } from '@nestjs/swagger';
import { CrudController } from '../../common/crud/crud.controller';
import { Bodega } from '../entities/bodega.entity';
import { BodegaService } from './bodega.service';

@ApiTags('bodegas')
@Controller('bodegas')
export class BodegaController extends CrudController<Bodega> {
  constructor(protected readonly service: BodegaService) {
    super(service);
  }
}
