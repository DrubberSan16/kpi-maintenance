import { Body, Controller, Delete, Get, Param, Patch, Post, Query } from '@nestjs/common';
import { ApiTags } from '@nestjs/swagger';
import { KpiMaintenanceService } from '../services/kpi-maintenance.service';
import { AlertaQueryDto, ChangeEstadoDto, CreateBitacoraDto, CreateConsumoDto, CreateEquipoDto, CreateEventoDto, CreatePlanDto, CreatePlanTareaDto, CreateProgramacionDto, DateRangeDto, EquipoQueryDto, IssueMaterialsDto, UpdateBitacoraDto, UpdateEquipoDto, UpdatePlanDto, UpdatePlanTareaDto, UpdateProgramacionDto, WorkOrderQueryDto } from '../dto';

@ApiTags('kpi-maintenance')
@Controller()
export class KpiMaintenanceController {
  constructor(private readonly service: KpiMaintenanceService) {}

  @Get('equipos') listEquipos(@Query() query: EquipoQueryDto) { return this.service.listEquipos(query); }
  @Get('equipos/:id') getEquipo(@Param('id') id: string) { return this.service.getEquipo(id); }
  @Post('equipos') createEquipo(@Body() dto: CreateEquipoDto) { return this.service.createEquipo(dto); }
  @Patch('equipos/:id') updateEquipo(@Param('id') id: string, @Body() dto: UpdateEquipoDto) { return this.service.updateEquipo(id, dto); }
  @Delete('equipos/:id') deleteEquipo(@Param('id') id: string) { return this.service.deleteEquipo(id); }

  @Get('equipos/:id/bitacora') listBitacora(@Param('id') id: string, @Query() range: DateRangeDto) { return this.service.listBitacora(id, range); }
  @Post('equipos/:id/bitacora') createBitacora(@Param('id') id: string, @Body() dto: CreateBitacoraDto) { return this.service.createBitacora(id, dto); }
  @Patch('bitacora/:id') updateBitacora(@Param('id') id: string, @Body() dto: UpdateBitacoraDto) { return this.service.updateBitacora(id, dto); }
  @Delete('bitacora/:id') deleteBitacora(@Param('id') id: string) { return this.service.deleteBitacora(id); }

  @Post('equipos/:id/estado') changeEstado(@Param('id') id: string, @Body() dto: ChangeEstadoDto) { return this.service.changeEstado(id, dto); }
  @Get('equipos/:id/estado') listEstados(@Param('id') id: string, @Query() range: DateRangeDto) { return this.service.listEstados(id, range); }

  @Post('equipos/:id/eventos') createEvento(@Param('id') id: string, @Body() dto: CreateEventoDto) { return this.service.createEvento(id, dto); }
  @Get('equipos/:id/eventos') listEventos(@Param('id') id: string, @Query() query: DateRangeDto & { tipo_evento?: string }) { return this.service.listEventos(id, query); }

  @Post('planes') createPlan(@Body() dto: CreatePlanDto) { return this.service.createPlan(dto); }
  @Get('planes') listPlanes() { return this.service.listPlanes(); }
  @Get('planes/:id') getPlan(@Param('id') id: string) { return this.service.getPlan(id); }
  @Patch('planes/:id') updatePlan(@Param('id') id: string, @Body() dto: UpdatePlanDto) { return this.service.updatePlan(id, dto); }
  @Delete('planes/:id') deletePlan(@Param('id') id: string) { return this.service.deletePlan(id); }

  @Post('planes/:id/tareas') createPlanTarea(@Param('id') id: string, @Body() dto: CreatePlanTareaDto) { return this.service.createPlanTarea(id, dto); }
  @Get('planes/:id/tareas') listPlanTareas(@Param('id') id: string) { return this.service.listPlanTareas(id); }
  @Patch('planes/tareas/:id') updatePlanTarea(@Param('id') id: string, @Body() dto: UpdatePlanTareaDto) { return this.service.updatePlanTarea(id, dto); }
  @Delete('planes/tareas/:id') deletePlanTarea(@Param('id') id: string) { return this.service.deletePlanTarea(id); }

  @Post('programaciones') createProgramacion(@Body() dto: CreateProgramacionDto) { return this.service.createProgramacion(dto); }
  @Get('programaciones') listProgramaciones() { return this.service.listProgramaciones(); }
  @Get('programaciones/:id') getProgramacion(@Param('id') id: string) { return this.service.getProgramacion(id); }
  @Patch('programaciones/:id') updateProgramacion(@Param('id') id: string, @Body() dto: UpdateProgramacionDto) { return this.service.updateProgramacion(id, dto); }
  @Delete('programaciones/:id') deleteProgramacion(@Param('id') id: string) { return this.service.deleteProgramacion(id); }

  @Get('alertas') listAlertas(@Query() query: AlertaQueryDto) { return this.service.listAlertas(query); }
  @Post('alertas/recalcular') recalculate() { return this.service.recalculateAlertas(); }

  @Get('work-orders') listWorkOrders(@Query() query: WorkOrderQueryDto) { return this.service.listWorkOrders(query); }
  @Post('work-orders/:id/consumos') createConsumo(@Param('id') id: string, @Body() dto: CreateConsumoDto) { return this.service.createConsumo(id, dto); }
  @Post('work-orders/:id/issue-materials') issueMaterials(@Param('id') id: string, @Body() dto: IssueMaterialsDto) { return this.service.issueMaterials(id, dto); }
}
