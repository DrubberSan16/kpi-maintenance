import { Module, OnModuleInit } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { KpiMaintenanceController } from './controllers/kpi-maintenance.controller';
import { KpiMaintenanceService } from './services/kpi-maintenance.service';
import {
  AlertaMantenimientoEntity,
  BitacoraDiariaEntity,
  ConsumoRepuestoEntity,
  EntregaMaterialDetEntity,
  EntregaMaterialEntity,
  EquipoComponenteEntity,
  EquipoEntity,
  EquipoTipoEntity,
  EstadoEquipoCatalogoEntity,
  EstadoEquipoEntity,
  EventoEquipoEntity,
  FallaCatalogoEntity,
  KardexEntity,
  LecturaEquipoEntity,
  LocationEntity,
  LubricacionPuntoEntity,
  MovimientoInventarioDetEntity,
  MovimientoInventarioEntity,
  PlanMantenimientoEntity,
  PlanTareaEntity,
  ProductoEntity,
  ProgramacionPlanEntity,
  ReservaStockEntity,
  StockBodegaEntity,
  WorkOrderAdjuntoEntity,
  WorkOrderEntity,
  WorkOrderTareaEntity,
} from './entities/kpi-maintenance.entity';

@Module({
  imports: [
    TypeOrmModule.forFeature([
      EquipoEntity,
      BitacoraDiariaEntity,
      AlertaMantenimientoEntity,
      EstadoEquipoEntity,
      EstadoEquipoCatalogoEntity,
      EquipoTipoEntity,
      EventoEquipoEntity,
      EquipoComponenteEntity,
      FallaCatalogoEntity,
      LecturaEquipoEntity,
      LubricacionPuntoEntity,
      PlanMantenimientoEntity,
      PlanTareaEntity,
      ProgramacionPlanEntity,
      WorkOrderEntity,
      ConsumoRepuestoEntity,
      StockBodegaEntity,
      ProductoEntity,
      ReservaStockEntity,
      EntregaMaterialEntity,
      EntregaMaterialDetEntity,
      MovimientoInventarioEntity,
      MovimientoInventarioDetEntity,
      KardexEntity,
      WorkOrderTareaEntity,
      WorkOrderAdjuntoEntity,
      LocationEntity,
    ]),
  ],
  controllers: [KpiMaintenanceController],
  providers: [KpiMaintenanceService],
})
export class KpiMaintenanceModule implements OnModuleInit {
  constructor(private readonly service: KpiMaintenanceService) {}
  onModuleInit() {
    void this.service.seedEstadosCatalogo();
  }
}
