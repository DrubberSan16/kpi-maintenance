import { Controller } from '@nestjs/common';
import { ApiTags } from '@nestjs/swagger';
import { CrudController } from '../../common/crud/crud.controller';
import { Marca } from '../entities/marca.entity';
import { MarcaService } from './marca.service';

@ApiTags('marcas')
@Controller('marcas')
export class MarcaController extends CrudController<Marca> {
  constructor(protected readonly service: MarcaService) {
    super(service);
  }
}
