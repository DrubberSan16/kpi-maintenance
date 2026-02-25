import { Controller } from '@nestjs/common';
import { ApiTags } from '@nestjs/swagger';
import { CrudController } from '../../common/crud/crud.controller';
import { Kardex } from '../entities/kardex.entity';
import { KardexService } from './kardex.service';


@ApiTags('kardex')
@Controller('kardex')
export class KardexController extends CrudController<Kardex> {
  constructor(protected readonly service: KardexService) {
    super(service);
  }
}
