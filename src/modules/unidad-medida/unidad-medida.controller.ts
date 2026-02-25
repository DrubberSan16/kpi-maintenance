import { Controller } from '@nestjs/common';
import { CrudController } from '../../common/crud/crud.controller';
import { UnidadMedida } from '../entities/unidad-medida.entity';
import { UnidadMedidaService } from './unidad-medida.service';

@Controller('unidades-medida')
export class UnidadMedidaController extends CrudController<UnidadMedida> {
  constructor(protected readonly service: UnidadMedidaService) {
    super(service);
  }
}
