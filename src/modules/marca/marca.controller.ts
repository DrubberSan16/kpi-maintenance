import { Controller } from '@nestjs/common';
import { CrudController } from '../../common/crud/crud.controller';
import { Marca } from '../entities/marca.entity';
import { MarcaService } from './marca.service';

@Controller('marcas')
export class MarcaController extends CrudController<Marca> {
  constructor(protected readonly service: MarcaService) {
    super(service);
  }
}
