import { Controller } from '@nestjs/common';
import { CrudController } from '../../common/crud/crud.controller';
import { Kardex } from '../entities/kardex.entity';
import { KardexService } from './kardex.service';

@Controller('kardex')
export class KardexController extends CrudController<Kardex> {
  constructor(protected readonly service: KardexService) {
    super(service);
  }
}
