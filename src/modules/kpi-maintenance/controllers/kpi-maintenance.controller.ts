import { Body, Controller, Delete, Get, Param, Patch, Post, Query } from '@nestjs/common';
import { ApiTags } from '@nestjs/swagger';
import { KpiMaintenanceService } from '../services/kpi-maintenance.service';
import { AlertaQueryDto, ChangeEstadoDto, CreateBitacoraDto, CreateConsumoDto, CreateEquipoDto, CreateEventoDto, CreatePlanDto, CreatePlanTareaDto, CreateProgramacionDto, DateRangeDto, EquipoQueryDto, IssueMaterialsDto, UpdateBitacoraDto, UpdateEquipoDto, UpdatePlanDto, UpdatePlanTareaDto, UpdateProgramacionDto, WorkOrderQueryDto } from '../dto';

@Controller()
export class KpiMaintenanceController {
  constructor(private readonly service: KpiMaintenanceService) {}

  @ApiTags('Equipos')
  @Get('equipos') listEquipos(@Query() query: EquipoQueryDto) { return this.service.listEquipos(query); }
  @ApiTags('Equipos')
  @Get('equipos/:id') getEquipo(@Param('id') id: string) { return this.service.getEquipo(id); }
  @ApiTags('Equipos')
  @Post('equipos') createEquipo(@Body() dto: CreateEquipoDto) { return this.service.createEquipo(dto); }
  @ApiTags('Equipos')
  @Patch('equipos/:id') updateEquipo(@Param('id') id: string, @Body() dto: UpdateEquipoDto) { return this.service.updateEquipo(id, dto); }
  @ApiTags('Equipos')
  @Delete('equipos/:id') deleteEquipo(@Param('id') id: string) { return this.service.deleteEquipo(id); }

  @ApiTags('Bitácora')
  @Get('equipos/:id/bitacora') listBitacora(@Param('id') id: string, @Query() range: DateRangeDto) { return this.service.listBitacora(id, range); }
  @ApiTags('Bitácora')
  @Post('equipos/:id/bitacora') createBitacora(@Param('id') id: string, @Body() dto: CreateBitacoraDto) { return this.service.createBitacora(id, dto); }
  @ApiTags('Bitácora')
  @Patch('bitacora/:id') updateBitacora(@Param('id') id: string, @Body() dto: UpdateBitacoraDto) { return this.service.updateBitacora(id, dto); }
  @ApiTags('Bitácora')
  @Delete('bitacora/:id') deleteBitacora(@Param('id') id: string) { return this.service.deleteBitacora(id); }

  @ApiTags('Estados')
  @Post('equipos/:id/estado') changeEstado(@Param('id') id: string, @Body() dto: ChangeEstadoDto) { return this.service.changeEstado(id, dto); }
  @ApiTags('Estados')
  @Get('equipos/:id/estado') listEstados(@Param('id') id: string, @Query() range: DateRangeDto) { return this.service.listEstados(id, range); }

  @ApiTags('Eventos')
  @Post('equipos/:id/eventos') createEvento(@Param('id') id: string, @Body() dto: CreateEventoDto) { return this.service.createEvento(id, dto); }
  @ApiTags('Eventos')
  @Get('equipos/:id/eventos') listEventos(@Param('id') id: string, @Query() query: DateRangeDto & { tipo_evento?: string }) { return this.service.listEventos(id, query); }

  @ApiTags('Planes')
  @Post('planes') createPlan(@Body() dto: CreatePlanDto) { return this.service.createPlan(dto); }
  @ApiTags('Planes')
  @Get('planes') listPlanes() { return this.service.listPlanes(); }
  @ApiTags('Planes')
  @Get('planes/:id') getPlan(@Param('id') id: string) { return this.service.getPlan(id); }
  @ApiTags('Planes')
  @Patch('planes/:id') updatePlan(@Param('id') id: string, @Body() dto: UpdatePlanDto) { return this.service.updatePlan(id, dto); }
  @ApiTags('Planes')
  @Delete('planes/:id') deletePlan(@Param('id') id: string) { return this.service.deletePlan(id); }

  @ApiTags('Plan - Tareas')
  @Post('planes/:id/tareas') createPlanTarea(@Param('id') id: string, @Body() dto: CreatePlanTareaDto) { return this.service.createPlanTarea(id, dto); }
  @ApiTags('Plan - Tareas')
  @Get('planes/:id/tareas') listPlanTareas(@Param('id') id: string) { return this.service.listPlanTareas(id); }
  @ApiTags('Plan - Tareas')
  @Patch('planes/tareas/:id') updatePlanTarea(@Param('id') id: string, @Body() dto: UpdatePlanTareaDto) { return this.service.updatePlanTarea(id, dto); }
  @ApiTags('Plan - Tareas')
  @Delete('planes/tareas/:id') deletePlanTarea(@Param('id') id: string) { return this.service.deletePlanTarea(id); }

  @ApiTags('Programaciones')
  @Post('programaciones') createProgramacion(@Body() dto: CreateProgramacionDto) { return this.service.createProgramacion(dto); }
  @ApiTags('Programaciones')
  @Get('programaciones') listProgramaciones() { return this.service.listProgramaciones(); }
  @ApiTags('Programaciones')
  @Get('programaciones/:id') getProgramacion(@Param('id') id: string) { return this.service.getProgramacion(id); }
  @ApiTags('Programaciones')
  @Patch('programaciones/:id') updateProgramacion(@Param('id') id: string, @Body() dto: UpdateProgramacionDto) { return this.service.updateProgramacion(id, dto); }
  @ApiTags('Programaciones')
  @Delete('programaciones/:id') deleteProgramacion(@Param('id') id: string) { return this.service.deleteProgramacion(id); }

  @ApiTags('Alertas')
  @Get('alertas') listAlertas(@Query() query: AlertaQueryDto) { return this.service.listAlertas(query); }
  @ApiTags('Alertas')
  @Post('alertas/recalcular') recalculate() { return this.service.recalculateAlertas(); }

  @ApiTags('Work Orders')
  @Get('work-orders') listWorkOrders(@Query() query: WorkOrderQueryDto) { return this.service.listWorkOrders(query); }
  @ApiTags('Work Orders')
  @Post('work-orders/:id/consumos') createConsumo(@Param('id') id: string, @Body() dto: CreateConsumoDto) { return this.service.createConsumo(id, dto); }
  @ApiTags('Work Orders')
  @Post('work-orders/:id/issue-materials') issueMaterials(@Param('id') id: string, @Body() dto: IssueMaterialsDto) { return this.service.issueMaterials(id, dto); }
}
