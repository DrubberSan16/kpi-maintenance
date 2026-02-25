import { Controller } from '@nestjs/common';
import { CrudController } from '../../common/crud/crud.controller';
import { StockBodega } from '../entities/stock-bodega.entity';
import { StockBodegaService } from './stock-bodega.service';

@Controller('stock-bodega')
export class StockBodegaController extends CrudController<StockBodega> {
  constructor(protected readonly service: StockBodegaService) {
    super(service);
  }
}
