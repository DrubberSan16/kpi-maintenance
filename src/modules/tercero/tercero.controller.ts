import { Controller } from '@nestjs/common';
import { ApiTags } from '@nestjs/swagger';
import { CrudController } from '../../common/crud/crud.controller';
import { Tercero } from '../entities/tercero.entity';
import { TerceroService } from './tercero.service';

@ApiTags('terceros')
@Controller('terceros')
export class TerceroController extends CrudController<Tercero> {
  constructor(protected readonly service: TerceroService) {
    super(service);
  }
}
