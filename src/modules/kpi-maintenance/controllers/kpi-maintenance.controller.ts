import { Body, Controller, Delete, Get, Param, Patch, Post, Query } from '@nestjs/common';
import { ApiBody, ApiOperation, ApiParam, ApiQuery, ApiTags } from '@nestjs/swagger';
import { KpiMaintenanceService } from '../services/kpi-maintenance.service';
import { AlertaQueryDto, ChangeEstadoDto, CreateBitacoraDto, CreateConsumoDto, CreateEquipoDto, CreateEventoDto, CreatePlanDto, CreatePlanTareaDto, CreateProgramacionDto, DateRangeDto, EquipoQueryDto, IssueMaterialsDto, UpdateBitacoraDto, UpdateEquipoDto, UpdatePlanDto, UpdatePlanTareaDto, UpdateProgramacionDto, WorkOrderQueryDto } from '../dto';

@Controller()
export class KpiMaintenanceController {
  constructor(private readonly service: KpiMaintenanceService) {}

  @ApiTags('Equipos')
  @ApiOperation({ summary: 'Listar equipos con filtros opcionales' })
  @Get('equipos') listEquipos(@Query() query: EquipoQueryDto) { return this.service.listEquipos(query); }
  @ApiTags('Equipos')
  @ApiOperation({ summary: 'Obtener equipo por ID' })
  @ApiParam({ name: 'id', description: 'ID del equipo', required: true })
  @Get('equipos/:id') getEquipo(@Param('id') id: string) { return this.service.getEquipo(id); }
  @ApiTags('Equipos')
  @ApiOperation({ summary: 'Crear un equipo' })
  @ApiBody({ type: CreateEquipoDto, required: true })
  @Post('equipos') createEquipo(@Body() dto: CreateEquipoDto) { return this.service.createEquipo(dto); }
  @ApiTags('Equipos')
  @ApiOperation({ summary: 'Actualizar un equipo por ID' })
  @ApiParam({ name: 'id', description: 'ID del equipo', required: true })
  @ApiBody({ type: UpdateEquipoDto, required: true })
  @Patch('equipos/:id') updateEquipo(@Param('id') id: string, @Body() dto: UpdateEquipoDto) { return this.service.updateEquipo(id, dto); }
  @ApiTags('Equipos')
  @ApiOperation({ summary: 'Eliminar un equipo por ID' })
  @ApiParam({ name: 'id', description: 'ID del equipo', required: true })
  @Delete('equipos/:id') deleteEquipo(@Param('id') id: string) { return this.service.deleteEquipo(id); }

  @ApiTags('Bitácora')
  @ApiOperation({ summary: 'Listar bitácora de un equipo por rango de fechas' })
  @ApiParam({ name: 'id', description: 'ID del equipo', required: true })
  @Get('equipos/:id/bitacora') listBitacora(@Param('id') id: string, @Query() range: DateRangeDto) { return this.service.listBitacora(id, range); }
  @ApiTags('Bitácora')
  @ApiOperation({ summary: 'Crear registro de bitácora para un equipo' })
  @ApiParam({ name: 'id', description: 'ID del equipo', required: true })
  @ApiBody({ type: CreateBitacoraDto, required: true })
  @Post('equipos/:id/bitacora') createBitacora(@Param('id') id: string, @Body() dto: CreateBitacoraDto) { return this.service.createBitacora(id, dto); }
  @ApiTags('Bitácora')
  @ApiOperation({ summary: 'Actualizar registro de bitácora por ID' })
  @ApiParam({ name: 'id', description: 'ID de la bitácora', required: true })
  @ApiBody({ type: UpdateBitacoraDto, required: true })
  @Patch('bitacora/:id') updateBitacora(@Param('id') id: string, @Body() dto: UpdateBitacoraDto) { return this.service.updateBitacora(id, dto); }
  @ApiTags('Bitácora')
  @ApiOperation({ summary: 'Eliminar registro de bitácora por ID' })
  @ApiParam({ name: 'id', description: 'ID de la bitácora', required: true })
  @Delete('bitacora/:id') deleteBitacora(@Param('id') id: string) { return this.service.deleteBitacora(id); }

  @ApiTags('Estados')
  @ApiOperation({ summary: 'Cambiar estado de un equipo' })
  @ApiParam({ name: 'id', description: 'ID del equipo', required: true })
  @ApiBody({ type: ChangeEstadoDto, required: true })
  @Post('equipos/:id/estado') changeEstado(@Param('id') id: string, @Body() dto: ChangeEstadoDto) { return this.service.changeEstado(id, dto); }
  @ApiTags('Estados')
  @ApiOperation({ summary: 'Listar historial de estados por equipo' })
  @ApiParam({ name: 'id', description: 'ID del equipo', required: true })
  @Get('equipos/:id/estado') listEstados(@Param('id') id: string, @Query() range: DateRangeDto) { return this.service.listEstados(id, range); }

  @ApiTags('Eventos')
  @ApiOperation({ summary: 'Crear evento para un equipo' })
  @ApiParam({ name: 'id', description: 'ID del equipo', required: true })
  @ApiBody({ type: CreateEventoDto, required: true })
  @Post('equipos/:id/eventos') createEvento(@Param('id') id: string, @Body() dto: CreateEventoDto) { return this.service.createEvento(id, dto); }
  @ApiTags('Eventos')
  @ApiOperation({ summary: 'Listar eventos de un equipo' })
  @ApiParam({ name: 'id', description: 'ID del equipo', required: true })
  @ApiQuery({ name: 'tipo_evento', required: false, description: 'Filtrar por tipo de evento' })
  @Get('equipos/:id/eventos') listEventos(@Param('id') id: string, @Query() query: DateRangeDto & { tipo_evento?: string }) { return this.service.listEventos(id, query); }

  @ApiTags('Planes')
  @ApiOperation({ summary: 'Crear plan de mantenimiento' })
  @ApiBody({ type: CreatePlanDto, required: true })
  @Post('planes') createPlan(@Body() dto: CreatePlanDto) { return this.service.createPlan(dto); }
  @ApiTags('Planes')
  @ApiOperation({ summary: 'Listar planes de mantenimiento' })
  @Get('planes') listPlanes() { return this.service.listPlanes(); }
  @ApiTags('Planes')
  @ApiOperation({ summary: 'Obtener plan por ID' })
  @ApiParam({ name: 'id', description: 'ID del plan', required: true })
  @Get('planes/:id') getPlan(@Param('id') id: string) { return this.service.getPlan(id); }
  @ApiTags('Planes')
  @ApiOperation({ summary: 'Actualizar plan por ID' })
  @ApiParam({ name: 'id', description: 'ID del plan', required: true })
  @ApiBody({ type: UpdatePlanDto, required: true })
  @Patch('planes/:id') updatePlan(@Param('id') id: string, @Body() dto: UpdatePlanDto) { return this.service.updatePlan(id, dto); }
  @ApiTags('Planes')
  @ApiOperation({ summary: 'Eliminar plan por ID' })
  @ApiParam({ name: 'id', description: 'ID del plan', required: true })
  @Delete('planes/:id') deletePlan(@Param('id') id: string) { return this.service.deletePlan(id); }

  @ApiTags('Plan - Tareas')
  @ApiOperation({ summary: 'Crear tarea de un plan' })
  @ApiParam({ name: 'id', description: 'ID del plan', required: true })
  @ApiBody({ type: CreatePlanTareaDto, required: true })
  @Post('planes/:id/tareas') createPlanTarea(@Param('id') id: string, @Body() dto: CreatePlanTareaDto) { return this.service.createPlanTarea(id, dto); }
  @ApiTags('Plan - Tareas')
  @ApiOperation({ summary: 'Listar tareas de un plan' })
  @ApiParam({ name: 'id', description: 'ID del plan', required: true })
  @Get('planes/:id/tareas') listPlanTareas(@Param('id') id: string) { return this.service.listPlanTareas(id); }
  @ApiTags('Plan - Tareas')
  @ApiOperation({ summary: 'Actualizar tarea de plan por ID' })
  @ApiParam({ name: 'id', description: 'ID de la tarea', required: true })
  @ApiBody({ type: UpdatePlanTareaDto, required: true })
  @Patch('planes/tareas/:id') updatePlanTarea(@Param('id') id: string, @Body() dto: UpdatePlanTareaDto) { return this.service.updatePlanTarea(id, dto); }
  @ApiTags('Plan - Tareas')
  @ApiOperation({ summary: 'Eliminar tarea de plan por ID' })
  @ApiParam({ name: 'id', description: 'ID de la tarea', required: true })
  @Delete('planes/tareas/:id') deletePlanTarea(@Param('id') id: string) { return this.service.deletePlanTarea(id); }

  @ApiTags('Programaciones')
  @ApiOperation({ summary: 'Crear programación de mantenimiento' })
  @ApiBody({ type: CreateProgramacionDto, required: true })
  @Post('programaciones') createProgramacion(@Body() dto: CreateProgramacionDto) { return this.service.createProgramacion(dto); }
  @ApiTags('Programaciones')
  @ApiOperation({ summary: 'Listar programaciones de mantenimiento' })
  @Get('programaciones') listProgramaciones() { return this.service.listProgramaciones(); }
  @ApiTags('Programaciones')
  @ApiOperation({ summary: 'Obtener programación por ID' })
  @ApiParam({ name: 'id', description: 'ID de la programación', required: true })
  @Get('programaciones/:id') getProgramacion(@Param('id') id: string) { return this.service.getProgramacion(id); }
  @ApiTags('Programaciones')
  @ApiOperation({ summary: 'Actualizar programación por ID' })
  @ApiParam({ name: 'id', description: 'ID de la programación', required: true })
  @ApiBody({ type: UpdateProgramacionDto, required: true })
  @Patch('programaciones/:id') updateProgramacion(@Param('id') id: string, @Body() dto: UpdateProgramacionDto) { return this.service.updateProgramacion(id, dto); }
  @ApiTags('Programaciones')
  @ApiOperation({ summary: 'Eliminar programación por ID' })
  @ApiParam({ name: 'id', description: 'ID de la programación', required: true })
  @Delete('programaciones/:id') deleteProgramacion(@Param('id') id: string) { return this.service.deleteProgramacion(id); }

  @ApiTags('Alertas')
  @ApiOperation({ summary: 'Listar alertas con filtros opcionales' })
  @Get('alertas') listAlertas(@Query() query: AlertaQueryDto) { return this.service.listAlertas(query); }
  @ApiTags('Alertas')
  @ApiOperation({ summary: 'Recalcular alertas del sistema' })
  @Post('alertas/recalcular') recalculate() { return this.service.recalculateAlertas(); }

  @ApiTags('Work Orders')
  @ApiOperation({ summary: 'Listar órdenes de trabajo con filtros opcionales' })
  @Get('work-orders') listWorkOrders(@Query() query: WorkOrderQueryDto) { return this.service.listWorkOrders(query); }
  @ApiTags('Work Orders')
  @ApiOperation({ summary: 'Registrar consumo en una orden de trabajo' })
  @ApiParam({ name: 'id', description: 'ID de la orden de trabajo', required: true })
  @ApiBody({ type: CreateConsumoDto, required: true })
  @Post('work-orders/:id/consumos') createConsumo(@Param('id') id: string, @Body() dto: CreateConsumoDto) { return this.service.createConsumo(id, dto); }
  @ApiTags('Work Orders')
  @ApiOperation({ summary: 'Emitir materiales en una orden de trabajo' })
  @ApiParam({ name: 'id', description: 'ID de la orden de trabajo', required: true })
  @ApiBody({ type: IssueMaterialsDto, required: true })
  @Post('work-orders/:id/issue-materials') issueMaterials(@Param('id') id: string, @Body() dto: IssueMaterialsDto) { return this.service.issueMaterials(id, dto); }
}
