import { Controller } from '@nestjs/common';
import { CrudController } from '../../common/crud/crud.controller';
import { Tercero } from '../entities/tercero.entity';
import { TerceroService } from './tercero.service';

@Controller('terceros')
export class TerceroController extends CrudController<Tercero> {
  constructor(protected readonly service: TerceroService) {
    super(service);
  }
}
