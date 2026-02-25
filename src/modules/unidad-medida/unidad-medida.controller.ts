import { Controller } from '@nestjs/common';
import { ApiTags } from '@nestjs/swagger';
import { CrudController } from '../../common/crud/crud.controller';
import { UnidadMedida } from '../entities/unidad-medida.entity';
import { UnidadMedidaService } from './unidad-medida.service';

@ApiTags('unidades-medida')
@Controller('unidades-medida')
export class UnidadMedidaController extends CrudController<UnidadMedida> {
  constructor(protected readonly service: UnidadMedidaService) {
    super(service);
  }
}
