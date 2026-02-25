import { Controller } from '@nestjs/common';
import { CrudController } from '../../common/crud/crud.controller';
import { Linea } from '../entities/linea.entity';
import { LineaService } from './linea.service';

@Controller('lineas')
export class LineaController extends CrudController<Linea> {
  constructor(protected readonly service: LineaService) {
    super(service);
  }
}
