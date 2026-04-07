import {
  Body,
  Controller,
  Delete,
  Get,
  Param,
  Patch,
  Post,
  Query,
  Res,
  UploadedFile,
  UseInterceptors,
} from '@nestjs/common';
import { FileInterceptor } from '@nestjs/platform-express';
import type { Response } from 'express';
import {
  ApiBody,
  ApiConsumes,
  ApiOperation,
  ApiParam,
  ApiQuery,
  ApiTags,
} from '@nestjs/swagger';
import { KpiMaintenanceService } from '../services/kpi-maintenance.service';
import {
  AlertaQueryDto,
  AnalisisLubricanteCatalogQueryDto,
  AnalisisLubricanteDashboardQueryDto,
  CreateAnalisisLubricanteDto,
  ChangeEstadoDto,
  CreateBitacoraDto,
  CreateConsumoDto,
  CreateCronogramaSemanalDto,
  CreateEquipoDto,
  CreateProgramacionMensualDetalleDto,
  CreateEventoDto,
  CreateFallaCatalogoDto,
  CreateLecturaEquipoDto,
  CreateLubricacionPuntoDto,
  CreateComponenteDto,
  CreateProcedimientoPlantillaDto,
  ComponenteQueryDto,
  CreatePlanDto,
  CreatePlanTareaDto,
  CreateProgramacionDto,
  CreateReporteOperacionDiariaDto,
  DateRangeDto,
  EquipoQueryDto,
  IssueMaterialsDto,
  EventoProcesoQueryDto,
  IntelligencePeriodQueryDto,
  ImportAnalisisLubricanteBatchDto,
  ProgramacionMensualQueryDto,
  PurgeAnalisisLubricanteDto,
  UpdateAnalisisLubricanteDto,
  UpdateBitacoraDto,
  UpdateProgramacionMensualConfigDto,
  UpdateProgramacionMensualDetalleDto,
  UpdateEquipoDto,
  UpdatePlanDto,
  UpdatePlanTareaDto,
  UpdateProgramacionDto,
  UpdateCronogramaSemanalDto,
  WorkOrderQueryDto,
  CreateEquipoTipoDto,
  CreateWorkOrderDto,
  CreateWorkOrderTareaDto,
  UpdateEquipoTipoDto,
  UpdateComponenteDto,
  UpdateFallaCatalogoDto,
  UpdateLecturaEquipoDto,
  UpdateLubricacionPuntoDto,
  UpdateProcedimientoPlantillaDto,
  UpdateReporteOperacionDiariaDto,
  UpdateWorkOrderDto,
  UpdateWorkOrderTareaDto,
  UploadWorkOrderAdjuntoDto,
  WorkOrderAdjuntoQueryDto,
  CreateLocationDto,
  UpdateLocationDto,
  LocationQueryDto,
  EquipoTipoQueryDto,
} from '../dto';

const bodyExamples = {
  createEquipo: {
    codigo: 'EQ-001',
    nombre: 'Excavadora CAT 320',
    equipo_tipo_id: 'd5f4b3e7-668f-44ac-8d59-9e992cbca3d8',
    location_id: 'c1b9cb58-68fd-41f4-8e7f-1c2bc1df51c5',
    marca_id: 'a1b2c3d4-e5f6-7890-abcd-ef1234567890',
    criticidad: 'ALTA',
    estado_operativo: 'OPERATIVO',
    horometro_actual: 1250.5,
  },
  createBitacora: {
    fecha: '2026-03-01T10:30:00.000Z',
    horometro: 1251,
    estado_id: '2dbf65ab-a4ad-4f9b-b9ca-cf9cd5ea2d84',
    observaciones: 'Se realizó inspección visual',
    registrado_por: 'f5c4bc8b-eeb5-4fa6-b7a2-f47f3cf271cb',
  },
  changeEstado: {
    estado_id: '2dbf65ab-a4ad-4f9b-b9ca-cf9cd5ea2d84',
    fecha_inicio: '2026-03-01T11:00:00.000Z',
    motivo: 'Equipo detenido por mantención preventiva',
  },
  createEvento: {
    tipo_evento: 'FALLA',
    work_order_id: 'f7f1f13f-beb9-4471-bdd7-3f73fe3fa6d6',
    fecha_inicio: '2026-03-01T11:00:00.000Z',
    fecha_fin: '2026-03-01T14:30:00.000Z',
    severidad: 3,
    descripcion: 'Falla en sistema hidráulico',
  },
  createPlan: {
    codigo: 'PLAN-250H',
    nombre: 'Mantenimiento cada 250 horas',
    tipo: 'PREVENTIVO',
    frecuencia_tipo: 'HORAS',
    frecuencia_valor: 250,
  },
  createPlanTarea: {
    orden: 1,
    actividad: 'Cambio de aceite de motor',
    field_type: 'CHECKBOX',
  },
  createProgramacion: {
    equipo_id: '1ec0ef12-5fd3-414f-aee4-5f163dd98a8c',
    plan_id: '3a92f88a-0e64-4c58-a0ab-a94f657fcb80',
    ultima_ejecucion_fecha: '2026-02-20T08:00:00.000Z',
    ultima_ejecucion_horas: 1000,
    proxima_fecha: '2026-03-20T08:00:00.000Z',
    proxima_horas: 1250,
    activo: true,
  },
  createConsumo: {
    producto_id: 'ec7be6b7-9ed0-4e15-aa7f-79b3f8f2b84f',
    bodega_id: '6013e2f8-62db-4142-9c40-5cdfb9de09af',
    cantidad: 2.5,
    costo_unitario: 14500,
    observacion: 'Consumo para cambio de filtro',
  },
  issueMaterials: {
    items: [
      {
        producto_id: 'ec7be6b7-9ed0-4e15-aa7f-79b3f8f2b84f',
        bodega_id: '6013e2f8-62db-4142-9c40-5cdfb9de09af',
        cantidad: 1,
      },
      {
        producto_id: 'ac5c35f5-3079-412f-b08f-c140de8f891f',
        bodega_id: '6013e2f8-62db-4142-9c40-5cdfb9de09af',
        cantidad: 4,
      },
    ],
    observacion: 'Salida de materiales para OT-1287',
  },
  createEquipoTipo: {
    codigo: 'EXCAVADORA',
    nombre: 'Excavadora',
    descripcion: 'Tipo de equipo para excavadoras',
  },

  createWorkOrder: {
    code: 'OT-2026-0001',
    type: 'MANTENIMIENTO',
    equipment_id: '1ec0ef12-5fd3-414f-aee4-5f163dd98a8c',
    plan_id: '3a92f88a-0e64-4c58-a0ab-a94f657fcb80',
    title: 'Mantenimiento preventivo 250H',
    description: 'Cambio de aceite y filtros',
    maintenance_kind: 'PREVENTIVO',
    status_workflow: 'PLANNED',
    priority: 3,
    valor_json: {
      accion: '',
      prevencion: '',
      causa: '',
    },
  },
  createWorkOrderTarea: {
    plan_id: '3a92f88a-0e64-4c58-a0ab-a94f657fcb80',
    tarea_id: 'cb44df27-46a4-4dd1-8b56-c69e963f2e93',
    valor_boolean: true,
    observacion: 'Se completó según checklist',
  },
  uploadAdjunto: {
    tipo: 'EVIDENCIA',
    nombre: 'foto-motor.jpg',
    contenido_base64: 'iVBORw0KGgoAAAANSUhEUgAA...',
    mime_type: 'image/jpeg',
  },
  createLocation: {
    codigo: 'BODEGA-PRINCIPAL',
    nombre: 'Bodega Principal',
    descripcion: 'Ubicación principal para almacenamiento de equipos',
  },
} as const;

@Controller()
export class KpiMaintenanceController {
  constructor(private readonly service: KpiMaintenanceService) {}

  @ApiTags('Equipos')
  @ApiOperation({ summary: 'Listar equipos con filtros opcionales' })
  @Get('equipos')
  listEquipos(@Query() query: EquipoQueryDto) {
    return this.service.listEquipos(query);
  }
  @ApiTags('Equipos')
  @ApiOperation({ summary: 'Obtener equipo por ID' })
  @ApiParam({ name: 'id', description: 'ID del equipo', required: true })
  @Get('equipos/:id')
  getEquipo(@Param('id') id: string) {
    return this.service.getEquipo(id);
  }
  @ApiTags('Equipos')
  @ApiOperation({ summary: 'Crear un equipo' })
  @ApiBody({
    type: CreateEquipoDto,
    required: true,
    examples: { ejemplo: { value: bodyExamples.createEquipo } },
  })
  @Post('equipos')
  createEquipo(@Body() dto: CreateEquipoDto) {
    return this.service.createEquipo(dto);
  }
  @ApiTags('Equipos')
  @ApiOperation({ summary: 'Actualizar un equipo por ID' })
  @ApiParam({ name: 'id', description: 'ID del equipo', required: true })
  @ApiBody({
    type: UpdateEquipoDto,
    required: true,
    examples: { ejemplo: { value: bodyExamples.createEquipo } },
  })
  @Patch('equipos/:id')
  updateEquipo(@Param('id') id: string, @Body() dto: UpdateEquipoDto) {
    return this.service.updateEquipo(id, dto);
  }
  @ApiTags('Equipos')
  @ApiOperation({ summary: 'Eliminar un equipo por ID' })
  @ApiParam({ name: 'id', description: 'ID del equipo', required: true })
  @Delete('equipos/:id')
  deleteEquipo(@Param('id') id: string) {
    return this.service.deleteEquipo(id);
  }

  @ApiTags('Tipo Equipo')
  @ApiOperation({ summary: 'Listar tipos de equipos con filtros opcionales' })
  @Get('tipo-equipo')
  listTipoEquipo(@Query() query: EquipoTipoQueryDto) {
    return this.service.listEquipoTipos(query);
  }

  @ApiTags('Tipo Equipo')
  @ApiOperation({ summary: 'Crear tipo de equipo' })
  @ApiBody({
    type: CreateEquipoTipoDto,
    required: true,
    examples: { ejemplo: { value: bodyExamples.createEquipoTipo } },
  })
  @Post('tipo-equipo')
  createTipoEquipo(@Body() dto: CreateEquipoTipoDto) {
    return this.service.createEquipoTipo(dto);
  }

  @ApiTags('Tipo Equipo')
  @ApiOperation({ summary: 'Actualizar tipo de equipo por ID' })
  @ApiParam({
    name: 'id',
    description: 'ID del tipo de equipo',
    required: true,
  })
  @ApiBody({
    type: UpdateEquipoTipoDto,
    required: true,
    examples: { ejemplo: { value: bodyExamples.createEquipoTipo } },
  })
  @Patch('tipo-equipo/:id')
  updateTipoEquipo(@Param('id') id: string, @Body() dto: UpdateEquipoTipoDto) {
    return this.service.updateEquipoTipo(id, dto);
  }

  @ApiTags('Tipo Equipo')
  @ApiOperation({ summary: 'Eliminar tipo de equipo por ID' })
  @ApiParam({
    name: 'id',
    description: 'ID del tipo de equipo',
    required: true,
  })
  @Delete('tipo-equipo/:id')
  deleteTipoEquipo(@Param('id') id: string) {
    return this.service.deleteEquipoTipo(id);
  }

  @ApiTags('Locaciones')
  @ApiOperation({ summary: 'Listar locaciones con filtros opcionales' })
  @Get('locaciones')
  listLocaciones(@Query() query: LocationQueryDto) {
    return this.service.listLocations(query);
  }

  @ApiTags('Locaciones')
  @ApiOperation({ summary: 'Obtener locación por ID' })
  @ApiParam({ name: 'id', description: 'ID de la locación', required: true })
  @Get('locaciones/:id')
  getLocacion(@Param('id') id: string) {
    return this.service.getLocation(id);
  }

  @ApiTags('Locaciones')
  @ApiOperation({ summary: 'Crear locación' })
  @ApiBody({
    type: CreateLocationDto,
    required: true,
    examples: { ejemplo: { value: bodyExamples.createLocation } },
  })
  @Post('locaciones')
  createLocation(@Body() dto: CreateLocationDto) {
    return this.service.createLocation(dto);
  }

  @ApiTags('Locaciones')
  @ApiOperation({ summary: 'Actualizar locación por ID' })
  @ApiParam({ name: 'id', description: 'ID de la locación', required: true })
  @ApiBody({
    type: UpdateLocationDto,
    required: true,
    examples: { ejemplo: { value: bodyExamples.createLocation } },
  })
  @Patch('locaciones/:id')
  updateLocation(@Param('id') id: string, @Body() dto: UpdateLocationDto) {
    return this.service.updateLocation(id, dto);
  }

  @ApiTags('Locaciones')
  @ApiOperation({ summary: 'Eliminar locación por ID' })
  @ApiParam({ name: 'id', description: 'ID de la locación', required: true })
  @Delete('locaciones/:id')
  deleteLocation(@Param('id') id: string) {
    return this.service.deleteLocation(id);
  }

  @ApiTags('Bitácora')
  @ApiOperation({ summary: 'Listar bitácora de un equipo por rango de fechas' })
  @ApiParam({ name: 'id', description: 'ID del equipo', required: true })
  @Get('equipos/:id/bitacora')
  listBitacora(@Param('id') id: string, @Query() range: DateRangeDto) {
    return this.service.listBitacora(id, range);
  }
  @ApiTags('Bitácora')
  @ApiOperation({ summary: 'Crear registro de bitácora para un equipo' })
  @ApiParam({ name: 'id', description: 'ID del equipo', required: true })
  @ApiBody({
    type: CreateBitacoraDto,
    required: true,
    examples: { ejemplo: { value: bodyExamples.createBitacora } },
  })
  @Post('equipos/:id/bitacora')
  createBitacora(@Param('id') id: string, @Body() dto: CreateBitacoraDto) {
    return this.service.createBitacora(id, dto);
  }
  @ApiTags('Bitácora')
  @ApiOperation({ summary: 'Actualizar registro de bitácora por ID' })
  @ApiParam({ name: 'id', description: 'ID de la bitácora', required: true })
  @ApiBody({
    type: UpdateBitacoraDto,
    required: true,
    examples: { ejemplo: { value: bodyExamples.createBitacora } },
  })
  @Patch('bitacora/:id')
  updateBitacora(@Param('id') id: string, @Body() dto: UpdateBitacoraDto) {
    return this.service.updateBitacora(id, dto);
  }
  @ApiTags('Bitácora')
  @ApiOperation({ summary: 'Eliminar registro de bitácora por ID' })
  @ApiParam({ name: 'id', description: 'ID de la bitácora', required: true })
  @Delete('bitacora/:id')
  deleteBitacora(@Param('id') id: string) {
    return this.service.deleteBitacora(id);
  }

  @ApiTags('Estados')
  @ApiOperation({ summary: 'Cambiar estado de un equipo' })
  @ApiParam({ name: 'id', description: 'ID del equipo', required: true })
  @ApiBody({
    type: ChangeEstadoDto,
    required: true,
    examples: { ejemplo: { value: bodyExamples.changeEstado } },
  })
  @Post('equipos/:id/estado')
  changeEstado(@Param('id') id: string, @Body() dto: ChangeEstadoDto) {
    return this.service.changeEstado(id, dto);
  }
  @ApiTags('Estados')
  @ApiOperation({ summary: 'Listar historial de estados por equipo' })
  @ApiParam({ name: 'id', description: 'ID del equipo', required: true })
  @Get('equipos/:id/estado')
  listEstados(@Param('id') id: string, @Query() range: DateRangeDto) {
    return this.service.listEstados(id, range);
  }

  @ApiTags('Eventos')
  @ApiOperation({ summary: 'Crear evento para un equipo' })
  @ApiParam({ name: 'id', description: 'ID del equipo', required: true })
  @ApiBody({
    type: CreateEventoDto,
    required: true,
    examples: { ejemplo: { value: bodyExamples.createEvento } },
  })
  @Post('equipos/:id/eventos')
  createEvento(@Param('id') id: string, @Body() dto: CreateEventoDto) {
    return this.service.createEvento(id, dto);
  }
  @ApiTags('Eventos')
  @ApiOperation({ summary: 'Listar eventos de un equipo' })
  @ApiParam({ name: 'id', description: 'ID del equipo', required: true })
  @ApiQuery({
    name: 'tipo_evento',
    required: false,
    description: 'Filtrar por tipo de evento',
  })
  @Get('equipos/:id/eventos')
  listEventos(
    @Param('id') id: string,
    @Query() query: DateRangeDto & { tipo_evento?: string },
  ) {
    return this.service.listEventos(id, query);
  }

  @ApiTags('Planes')
  @ApiOperation({ summary: 'Crear plan de mantenimiento' })
  @ApiBody({
    type: CreatePlanDto,
    required: true,
    examples: { ejemplo: { value: bodyExamples.createPlan } },
  })
  @Post('planes')
  createPlan(@Body() dto: CreatePlanDto) {
    return this.service.createPlan(dto);
  }
  @ApiTags('Planes')
  @ApiOperation({ summary: 'Listar planes de mantenimiento' })
  @Get('planes')
  listPlanes() {
    return this.service.listPlanes();
  }
  @ApiTags('Planes')
  @ApiOperation({ summary: 'Obtener plan por ID' })
  @ApiParam({ name: 'id', description: 'ID del plan', required: true })
  @Get('planes/:id')
  getPlan(@Param('id') id: string) {
    return this.service.getPlan(id);
  }
  @ApiTags('Planes')
  @ApiOperation({ summary: 'Actualizar plan por ID' })
  @ApiParam({ name: 'id', description: 'ID del plan', required: true })
  @ApiBody({
    type: UpdatePlanDto,
    required: true,
    examples: { ejemplo: { value: bodyExamples.createPlan } },
  })
  @Patch('planes/:id')
  updatePlan(@Param('id') id: string, @Body() dto: UpdatePlanDto) {
    return this.service.updatePlan(id, dto);
  }
  @ApiTags('Planes')
  @ApiOperation({ summary: 'Eliminar plan por ID' })
  @ApiParam({ name: 'id', description: 'ID del plan', required: true })
  @Delete('planes/:id')
  deletePlan(@Param('id') id: string) {
    return this.service.deletePlan(id);
  }

  @ApiTags('Plan - Tareas')
  @ApiOperation({ summary: 'Crear tarea de un plan' })
  @ApiParam({ name: 'id', description: 'ID del plan', required: true })
  @ApiBody({
    type: CreatePlanTareaDto,
    required: true,
    examples: { ejemplo: { value: bodyExamples.createPlanTarea } },
  })
  @Post('planes/:id/tareas')
  createPlanTarea(@Param('id') id: string, @Body() dto: CreatePlanTareaDto) {
    return this.service.createPlanTarea(id, dto);
  }
  @ApiTags('Plan - Tareas')
  @ApiOperation({ summary: 'Listar tareas de un plan' })
  @ApiParam({ name: 'id', description: 'ID del plan', required: true })
  @Get('planes/:id/tareas')
  listPlanTareas(@Param('id') id: string) {
    return this.service.listPlanTareas(id);
  }
  @ApiTags('Plan - Tareas')
  @ApiOperation({ summary: 'Actualizar tarea de plan por ID' })
  @ApiParam({ name: 'id', description: 'ID de la tarea', required: true })
  @ApiBody({
    type: UpdatePlanTareaDto,
    required: true,
    examples: { ejemplo: { value: bodyExamples.createPlanTarea } },
  })
  @Patch('planes/tareas/:id')
  updatePlanTarea(@Param('id') id: string, @Body() dto: UpdatePlanTareaDto) {
    return this.service.updatePlanTarea(id, dto);
  }
  @ApiTags('Plan - Tareas')
  @ApiOperation({ summary: 'Eliminar tarea de plan por ID' })
  @ApiParam({ name: 'id', description: 'ID de la tarea', required: true })
  @Delete('planes/tareas/:id')
  deletePlanTarea(@Param('id') id: string) {
    return this.service.deletePlanTarea(id);
  }

  @ApiTags('Programaciones')
  @ApiOperation({ summary: 'Crear programación de mantenimiento' })
  @ApiBody({
    type: CreateProgramacionDto,
    required: true,
    examples: { ejemplo: { value: bodyExamples.createProgramacion } },
  })
  @Post('programaciones')
  createProgramacion(@Body() dto: CreateProgramacionDto) {
    return this.service.createProgramacion(dto);
  }
  @ApiTags('Programaciones')
  @ApiOperation({ summary: 'Listar programaciones de mantenimiento' })
  @Get('programaciones')
  listProgramaciones() {
    return this.service.listProgramaciones();
  }
  @ApiTags('Programaciones')
  @ApiOperation({
    summary: 'Listar calendarios mensuales importados de programación MPG',
  })
  @Get('programaciones/mensuales')
  listProgramacionesMensuales(@Query() query: ProgramacionMensualQueryDto) {
    return this.service.listProgramacionesMensuales(query);
  }

  @ApiTags('Programaciones')
  @ApiOperation({
    summary: 'Obtener calendario mensual importado de programación MPG por ID',
  })
  @Get('programaciones/mensuales/:id')
  getProgramacionMensual(
    @Param('id') id: string,
    @Query() query: ProgramacionMensualQueryDto,
  ) {
    return this.service.getProgramacionMensual(id, query);
  }

  @ApiTags('Programaciones')
  @ApiOperation({
    summary: 'Crear un detalle manual dentro del calendario mensual importado',
  })
  @Post('programaciones/mensuales/:id/detalles')
  createProgramacionMensualDetalle(
    @Param('id') id: string,
    @Body() dto: CreateProgramacionMensualDetalleDto,
  ) {
    return this.service.createProgramacionMensualDetalle(id, dto);
  }

  @ApiTags('Programaciones')
  @ApiOperation({
    summary: 'Actualizar un detalle manual dentro del calendario mensual importado',
  })
  @Patch('programaciones/mensuales/detalles/:detailId')
  updateProgramacionMensualDetalle(
    @Param('detailId') detailId: string,
    @Body() dto: UpdateProgramacionMensualDetalleDto,
  ) {
    return this.service.updateProgramacionMensualDetalle(detailId, dto);
  }

  @ApiTags('Programaciones')
  @ApiOperation({
    summary: 'Actualizar configuración visual y payload del calendario mensual',
  })
  @Patch('programaciones/mensuales/:id/config')
  updateProgramacionMensualConfig(
    @Param('id') id: string,
    @Body() dto: UpdateProgramacionMensualConfigDto,
  ) {
    return this.service.updateProgramacionMensualConfig(id, dto);
  }

  @ApiTags('Programaciones')
  @ApiOperation({
    summary: 'Importar programación mensual MPG desde Excel',
  })
  @ApiConsumes('multipart/form-data')
  @ApiBody({
    schema: {
      type: 'object',
      required: ['file'],
      properties: {
        file: { type: 'string', format: 'binary' },
      },
    },
  })
  @UseInterceptors(FileInterceptor('file'))
  @Post('programaciones/import/mensual/upload')
  importProgramacionMensualWorkbook(
    @UploadedFile() file: any,
    @Body('requested_by') requestedBy?: string,
    @Body('requested_by_email') requestedByEmail?: string,
    @Body('requested_user_id') requestedUserId?: string,
  ) {
    return this.service.importProgramacionMensualWorkbook(file, {
      requested_by: requestedBy,
      requested_by_email: requestedByEmail,
      requested_user_id: requestedUserId,
    });
  }
  @ApiTags('Programaciones')
  @ApiOperation({ summary: 'Obtener programación por ID' })
  @ApiParam({
    name: 'id',
    description: 'ID de la programación',
    required: true,
  })
  @Get('programaciones/:id')
  getProgramacion(@Param('id') id: string) {
    return this.service.getProgramacion(id);
  }
  @ApiTags('Programaciones')
  @ApiOperation({ summary: 'Actualizar programación por ID' })
  @ApiParam({
    name: 'id',
    description: 'ID de la programación',
    required: true,
  })
  @ApiBody({
    type: UpdateProgramacionDto,
    required: true,
    examples: { ejemplo: { value: bodyExamples.createProgramacion } },
  })
  @Patch('programaciones/:id')
  updateProgramacion(
    @Param('id') id: string,
    @Body() dto: UpdateProgramacionDto,
  ) {
    return this.service.updateProgramacion(id, dto);
  }
  @ApiTags('Programaciones')
  @ApiOperation({ summary: 'Eliminar programación por ID' })
  @ApiParam({
    name: 'id',
    description: 'ID de la programación',
    required: true,
  })
  @Delete('programaciones/:id')
  deleteProgramacion(@Param('id') id: string) {
    return this.service.deleteProgramacion(id);
  }

  @ApiTags('Alertas')
  @ApiOperation({ summary: 'Listar alertas con filtros opcionales' })
  @Get('alertas')
  listAlertas(@Query() query: AlertaQueryDto) {
    return this.service.listAlertas(query);
  }
  @ApiTags('Alertas')
  @ApiOperation({ summary: 'Obtener resumen consolidado de alertas' })
  @Get('alertas/summary')
  getAlertSummary() {
    return this.service.getAlertasSummary();
  }
  @ApiTags('Alertas')
  @ApiOperation({ summary: 'Recalcular alertas del sistema' })
  @Post('alertas/recalcular')
  recalculate(@Body() payload?: Record<string, unknown>) {
    const source = String(payload?.source || 'manual').trim() || 'manual';
    return this.service.recalculateAlertasNow(source);
  }

  @ApiTags('Componentes')
  @ApiOperation({ summary: 'Listar componentes de equipo' })
  @Get('componentes')
  listComponentes(@Query() query: ComponenteQueryDto) {
    return this.service.listComponentes(query);
  }

  @ApiTags('Componentes')
  @ApiOperation({ summary: 'Obtener componente por ID' })
  @Get('componentes/:id')
  getComponente(@Param('id') id: string) {
    return this.service.getComponente(id);
  }

  @ApiTags('Componentes')
  @ApiOperation({ summary: 'Crear componente de equipo' })
  @Post('componentes')
  createComponente(@Body() dto: CreateComponenteDto) {
    return this.service.createComponente(dto);
  }

  @ApiTags('Componentes')
  @ApiOperation({ summary: 'Actualizar componente por ID' })
  @Patch('componentes/:id')
  updateComponente(@Param('id') id: string, @Body() dto: UpdateComponenteDto) {
    return this.service.updateComponente(id, dto);
  }

  @ApiTags('Componentes')
  @ApiOperation({ summary: 'Eliminar componente por ID' })
  @Delete('componentes/:id')
  deleteComponente(@Param('id') id: string) {
    return this.service.deleteComponente(id);
  }

  @ApiTags('Fallas')
  @ApiOperation({ summary: 'Listar catálogo de fallas' })
  @Get('fallas')
  listFallas() {
    return this.service.listFallasCatalogo();
  }

  @ApiTags('Fallas')
  @ApiOperation({ summary: 'Obtener falla por ID' })
  @Get('fallas/:id')
  getFalla(@Param('id') id: string) {
    return this.service.getFallaCatalogo(id);
  }

  @ApiTags('Fallas')
  @ApiOperation({ summary: 'Crear falla de catálogo' })
  @Post('fallas')
  createFalla(@Body() dto: CreateFallaCatalogoDto) {
    return this.service.createFallaCatalogo(dto);
  }

  @ApiTags('Fallas')
  @ApiOperation({ summary: 'Actualizar falla de catálogo' })
  @Patch('fallas/:id')
  updateFalla(@Param('id') id: string, @Body() dto: UpdateFallaCatalogoDto) {
    return this.service.updateFallaCatalogo(id, dto);
  }

  @ApiTags('Fallas')
  @ApiOperation({ summary: 'Eliminar falla de catálogo' })
  @Delete('fallas/:id')
  deleteFalla(@Param('id') id: string) {
    return this.service.deleteFallaCatalogo(id);
  }

  @ApiTags('Lecturas')
  @ApiOperation({ summary: 'Listar lecturas de equipo' })
  @Get('lecturas')
  listLecturas(@Query('equipo_id') equipoId?: string) {
    return this.service.listLecturas(equipoId);
  }

  @ApiTags('Lecturas')
  @ApiOperation({ summary: 'Obtener lectura por ID' })
  @Get('lecturas/:id')
  getLectura(@Param('id') id: string) {
    return this.service.getLectura(id);
  }

  @ApiTags('Lecturas')
  @ApiOperation({ summary: 'Crear lectura de equipo' })
  @Post('lecturas')
  createLectura(@Body() dto: CreateLecturaEquipoDto) {
    return this.service.createLectura(dto);
  }

  @ApiTags('Lecturas')
  @ApiOperation({ summary: 'Actualizar lectura de equipo' })
  @Patch('lecturas/:id')
  updateLectura(@Param('id') id: string, @Body() dto: UpdateLecturaEquipoDto) {
    return this.service.updateLectura(id, dto);
  }

  @ApiTags('Lecturas')
  @ApiOperation({ summary: 'Eliminar lectura de equipo' })
  @Delete('lecturas/:id')
  deleteLectura(@Param('id') id: string) {
    return this.service.deleteLectura(id);
  }

  @ApiTags('Lubricación')
  @ApiOperation({ summary: 'Listar puntos de lubricación' })
  @Get('lubricaciones')
  listLubricaciones(@Query('equipo_id') equipoId?: string) {
    return this.service.listLubricaciones(equipoId);
  }

  @ApiTags('Lubricación')
  @ApiOperation({ summary: 'Obtener punto de lubricación por ID' })
  @Get('lubricaciones/:id')
  getLubricacion(@Param('id') id: string) {
    return this.service.getLubricacion(id);
  }

  @ApiTags('Lubricación')
  @ApiOperation({ summary: 'Crear punto de lubricación' })
  @Post('lubricaciones')
  createLubricacion(@Body() dto: CreateLubricacionPuntoDto) {
    return this.service.createLubricacion(dto);
  }

  @ApiTags('Lubricación')
  @ApiOperation({ summary: 'Actualizar punto de lubricación' })
  @Patch('lubricaciones/:id')
  updateLubricacion(
    @Param('id') id: string,
    @Body() dto: UpdateLubricacionPuntoDto,
  ) {
    return this.service.updateLubricacion(id, dto);
  }

  @ApiTags('Lubricación')
  @ApiOperation({ summary: 'Eliminar punto de lubricación' })
  @Delete('lubricaciones/:id')
  deleteLubricacion(@Param('id') id: string) {
    return this.service.deleteLubricacion(id);
  }

  @ApiTags('Inteligencia Operativa')
  @ApiOperation({ summary: 'Listar plantillas de procedimientos documentales' })
  @Get('inteligencia/procedimientos')
  listProcedimientosPlantilla() {
    return this.service.listProcedimientosPlantilla();
  }

  @ApiTags('Inteligencia Operativa')
  @ApiOperation({ summary: 'Obtener el siguiente código disponible de plantilla MPG' })
  @Get('inteligencia/procedimientos/next-code')
  getNextProcedimientoPlantillaCode() {
    return this.service.getNextProcedimientoPlantillaCode();
  }

  @ApiTags('Inteligencia Operativa')
  @ApiOperation({ summary: 'Obtener plantilla de procedimiento por ID' })
  @Get('inteligencia/procedimientos/:id')
  getProcedimientoPlantilla(@Param('id') id: string) {
    return this.service.getProcedimientoPlantilla(id);
  }

  @ApiTags('Inteligencia Operativa')
  @ApiOperation({ summary: 'Crear plantilla de procedimiento documental' })
  @Post('inteligencia/procedimientos')
  createProcedimientoPlantilla(@Body() dto: CreateProcedimientoPlantillaDto) {
    return this.service.createProcedimientoPlantilla(dto);
  }

  @ApiTags('Inteligencia Operativa')
  @ApiOperation({ summary: 'Actualizar plantilla de procedimiento por ID' })
  @Patch('inteligencia/procedimientos/:id')
  updateProcedimientoPlantilla(
    @Param('id') id: string,
    @Body() dto: UpdateProcedimientoPlantillaDto,
  ) {
    return this.service.updateProcedimientoPlantilla(id, dto);
  }

  @ApiTags('Inteligencia Operativa')
  @ApiOperation({ summary: 'Eliminar plantilla de procedimiento por ID' })
  @Delete('inteligencia/procedimientos/:id')
  deleteProcedimientoPlantilla(@Param('id') id: string) {
    return this.service.deleteProcedimientoPlantilla(id);
  }

  @ApiTags('Inteligencia Operativa')
  @ApiOperation({ summary: 'Listar análisis de lubricante' })
  @Get('inteligencia/analisis-lubricante')
  listAnalisisLubricante() {
    return this.service.listAnalisisLubricante();
  }

  @ApiTags('Inteligencia Operativa')
  @ApiOperation({ summary: 'Obtener el siguiente código disponible de análisis de lubricante' })
  @Get('inteligencia/analisis-lubricante/next-code')
  getNextAnalisisLubricanteCode() {
    return this.service.getNextAnalisisLubricanteCode();
  }

  @ApiTags('Inteligencia Operativa')
  @ApiOperation({ summary: 'Obtener el siguiente código disponible de cronograma semanal' })
  @Get('inteligencia/cronogramas-semanales/next-code')
  getNextCronogramaSemanalCode() {
    return this.service.getNextCronogramaSemanalCode();
  }

  @ApiTags('Inteligencia Operativa')
  @ApiOperation({ summary: 'Catálogo de lubricantes para autocompletado' })
  @Get('inteligencia/analisis-lubricante/catalog')
  listAnalisisLubricanteCatalog(
    @Query() query: AnalisisLubricanteCatalogQueryDto,
  ) {
    return this.service.listAnalisisLubricanteCatalog(query);
  }

  @ApiTags('Inteligencia Operativa')
  @ApiOperation({ summary: 'Dashboard del análisis de lubricante por lubricante o código' })
  @Get('inteligencia/analisis-lubricante/dashboard')
  getAnalisisLubricanteDashboard(
    @Query() query: AnalisisLubricanteDashboardQueryDto,
  ) {
    return this.service.getAnalisisLubricanteDashboard(query);
  }

  @ApiTags('Inteligencia Operativa')
  @ApiOperation({ summary: 'Obtener análisis de lubricante por ID' })
  @Get('inteligencia/analisis-lubricante/:id')
  getAnalisisLubricante(@Param('id') id: string) {
    return this.service.getAnalisisLubricante(id);
  }

  @ApiTags('Inteligencia Operativa')
  @ApiOperation({ summary: 'Crear análisis de lubricante' })
  @Post('inteligencia/analisis-lubricante')
  createAnalisisLubricante(@Body() dto: CreateAnalisisLubricanteDto) {
    return this.service.createAnalisisLubricante(dto);
  }

  @ApiTags('Inteligencia Operativa')
  @ApiOperation({ summary: 'Importar análisis de lubricante en lote desde un Excel ya parseado' })
  @Post('inteligencia/analisis-lubricante/import')
  importAnalisisLubricanteBatch(@Body() dto: ImportAnalisisLubricanteBatchDto) {
    return this.service.importAnalisisLubricanteBatch(dto);
  }

  @ApiTags('Inteligencia Operativa')
  @ApiOperation({
    summary:
      'Subir archivo Excel de análisis de lubricante para procesarlo en segundo plano',
  })
  @ApiConsumes('multipart/form-data')
  @ApiBody({
    schema: {
      type: 'object',
      required: ['file'],
      properties: {
        file: { type: 'string', format: 'binary' },
        upsert_existing: {
          type: 'boolean',
          description:
            'Si es true, actualiza registros existentes que coincidan con la muestra importada',
        },
        requested_by: {
          type: 'string',
          description: 'Usuario responsable de la carga',
        },
      },
    },
  })
  @UseInterceptors(FileInterceptor('file'))
  @Post('inteligencia/analisis-lubricante/import/upload')
  uploadAnalisisLubricanteWorkbook(
    @UploadedFile() file: any,
    @Body('upsert_existing') upsertExisting?: string | boolean,
    @Body('requested_by') requestedBy?: string,
  ) {
    return this.service.startAnalisisLubricanteImport(file, {
      upsert_existing: upsertExisting,
      requested_by: requestedBy,
    });
  }

  @ApiTags('Inteligencia Operativa')
  @ApiOperation({
    summary: 'Descargar formato válido de carga para análisis de lubricante',
  })
  @Get('inteligencia/analisis-lubricante/import/template')
  async downloadAnalisisLubricanteTemplate(@Res() res: Response) {
    const template = await this.service.getAnalisisLubricanteImportTemplate();
    res.setHeader(
      'Content-Type',
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    );
    res.setHeader(
      'Content-Disposition',
      `attachment; filename="${template.filename}"`,
    );
    res.send(template.buffer);
  }

  @ApiTags('Inteligencia Operativa')
  @ApiOperation({
    summary: 'Consultar estado, progreso y logs de una importación de lubricante',
  })
  @Get('inteligencia/analisis-lubricante/import/:jobId')
  getAnalisisLubricanteImportStatus(@Param('jobId') jobId: string) {
    return this.service.getAnalisisLubricanteImportStatus(jobId);
  }

  @ApiTags('Inteligencia Operativa')
  @ApiOperation({
    summary:
      'Eliminar físicamente toda la información de análisis de lubricante, incluyendo detalles, alertas derivadas, eventos y archivos de importación',
  })
  @Post('inteligencia/analisis-lubricante/purge')
  purgeAnalisisLubricante(@Body() dto: PurgeAnalisisLubricanteDto) {
    return this.service.purgeAnalisisLubricante(dto);
  }

  @ApiTags('Inteligencia Operativa')
  @ApiOperation({ summary: 'Actualizar análisis de lubricante por ID' })
  @Patch('inteligencia/analisis-lubricante/:id')
  updateAnalisisLubricante(
    @Param('id') id: string,
    @Body() dto: UpdateAnalisisLubricanteDto,
  ) {
    return this.service.updateAnalisisLubricante(id, dto);
  }

  @ApiTags('Inteligencia Operativa')
  @ApiOperation({ summary: 'Eliminar análisis de lubricante por ID' })
  @Delete('inteligencia/analisis-lubricante/:id')
  deleteAnalisisLubricante(@Param('id') id: string) {
    return this.service.deleteAnalisisLubricante(id);
  }

  @ApiTags('Inteligencia Operativa')
  @ApiOperation({ summary: 'Listar cronogramas semanales' })
  @Get('inteligencia/cronogramas-semanales')
  listCronogramasSemanales() {
    return this.service.listCronogramasSemanales();
  }

  @ApiTags('Inteligencia Operativa')
  @ApiOperation({ summary: 'Obtener cronograma semanal por ID' })
  @Get('inteligencia/cronogramas-semanales/:id')
  getCronogramaSemanal(@Param('id') id: string) {
    return this.service.getCronogramaSemanal(id);
  }

  @ApiTags('Inteligencia Operativa')
  @ApiOperation({ summary: 'Crear cronograma semanal' })
  @Post('inteligencia/cronogramas-semanales')
  createCronogramaSemanal(@Body() dto: CreateCronogramaSemanalDto) {
    return this.service.createCronogramaSemanal(dto);
  }

  @ApiTags('Inteligencia Operativa')
  @ApiOperation({ summary: 'Importar cronograma semanal desde Excel' })
  @ApiConsumes('multipart/form-data')
  @ApiBody({
    schema: {
      type: 'object',
      required: ['file'],
      properties: {
        file: { type: 'string', format: 'binary' },
      },
    },
  })
  @UseInterceptors(FileInterceptor('file'))
  @Post('inteligencia/cronogramas-semanales/import/upload')
  importCronogramaSemanalWorkbook(
    @UploadedFile() file: any,
    @Body('requested_by') requestedBy?: string,
    @Body('requested_by_email') requestedByEmail?: string,
    @Body('requested_user_id') requestedUserId?: string,
  ) {
    return this.service.importCronogramaSemanalWorkbook(file, {
      requested_by: requestedBy,
      requested_by_email: requestedByEmail,
      requested_user_id: requestedUserId,
    });
  }

  @ApiTags('Inteligencia Operativa')
  @ApiOperation({ summary: 'Actualizar cronograma semanal por ID' })
  @Patch('inteligencia/cronogramas-semanales/:id')
  updateCronogramaSemanal(
    @Param('id') id: string,
    @Body() dto: UpdateCronogramaSemanalDto,
  ) {
    return this.service.updateCronogramaSemanal(id, dto);
  }

  @ApiTags('Inteligencia Operativa')
  @ApiOperation({ summary: 'Eliminar cronograma semanal por ID' })
  @Delete('inteligencia/cronogramas-semanales/:id')
  deleteCronogramaSemanal(@Param('id') id: string) {
    return this.service.deleteCronogramaSemanal(id);
  }

  @ApiTags('Inteligencia Operativa')
  @ApiOperation({ summary: 'Listar reportes de operación diaria' })
  @Get('inteligencia/reportes-diarios')
  listReportesOperacionDiaria() {
    return this.service.listReportesOperacionDiaria();
  }

  @ApiTags('Inteligencia Operativa')
  @ApiOperation({ summary: 'Obtener reporte de operación diaria por ID' })
  @Get('inteligencia/reportes-diarios/:id')
  getReporteOperacionDiaria(@Param('id') id: string) {
    return this.service.getReporteOperacionDiaria(id);
  }

  @ApiTags('Inteligencia Operativa')
  @ApiOperation({ summary: 'Crear reporte de operación diaria' })
  @Post('inteligencia/reportes-diarios')
  createReporteOperacionDiaria(@Body() dto: CreateReporteOperacionDiariaDto) {
    return this.service.createReporteOperacionDiaria(dto);
  }

  @ApiTags('Inteligencia Operativa')
  @ApiOperation({ summary: 'Actualizar reporte de operación diaria por ID' })
  @Patch('inteligencia/reportes-diarios/:id')
  updateReporteOperacionDiaria(
    @Param('id') id: string,
    @Body() dto: UpdateReporteOperacionDiariaDto,
  ) {
    return this.service.updateReporteOperacionDiaria(id, dto);
  }

  @ApiTags('Inteligencia Operativa')
  @ApiOperation({ summary: 'Eliminar reporte de operación diaria por ID' })
  @Delete('inteligencia/reportes-diarios/:id')
  deleteReporteOperacionDiaria(@Param('id') id: string) {
    return this.service.deleteReporteOperacionDiaria(id);
  }

  @ApiTags('Inteligencia Operativa')
  @ApiOperation({ summary: 'Listar eventos de proceso' })
  @Get('inteligencia/eventos')
  listEventosProceso(@Query() query: EventoProcesoQueryDto) {
    return this.service.listEventosProceso(query);
  }

  @ApiTags('Inteligencia Operativa')
  @ApiOperation({ summary: 'Listar control de componentes criticos' })
  @Get('inteligencia/control-componentes')
  listControlComponentesCriticos() {
    return this.service.listControlComponentesCriticos();
  }

  @ApiTags('Inteligencia Operativa')
  @ApiOperation({ summary: 'Obtener resumen KPI documental y operativo' })
  @Get('inteligencia/summary')
  getIntelligenceSummary(@Query() query: IntelligencePeriodQueryDto) {
    return this.service.getIntelligenceSummary(query);
  }

  @ApiTags('Work Orders')
  @ApiOperation({ summary: 'Listar órdenes de trabajo con filtros opcionales' })
  @Get('work-orders')
  listWorkOrders(@Query() query: WorkOrderQueryDto) {
    return this.service.listWorkOrders(query);
  }


  @ApiTags('Work Orders')
  @ApiOperation({ summary: 'Obtener el siguiente código disponible de orden de trabajo' })
  @Get('work-orders/next-code')
  getNextWorkOrderCode() {
    return this.service.getNextWorkOrderCode();
  }

  @ApiTags('Work Orders')
  @ApiOperation({
    summary:
      'Listar reservas de stock de un material en una bodega y la OT a la que están ligadas',
  })
  @ApiQuery({
    name: 'producto_id',
    required: true,
    description: 'ID del material/producto',
  })
  @ApiQuery({
    name: 'bodega_id',
    required: true,
    description: 'ID de la bodega',
  })
  @Get('work-orders/material-reservations')
  listMaterialReservations(
    @Query('producto_id') productoId: string,
    @Query('bodega_id') bodegaId: string,
  ) {
    return this.service.listMaterialReservations(productoId, bodegaId);
  }

  @ApiTags('Work Orders')
  @ApiOperation({ summary: 'Obtener orden de trabajo por ID' })
  @ApiParam({
    name: 'id',
    description: 'ID de la orden de trabajo',
    required: true,
  })
  @Get('work-orders/:id')
  getWorkOrder(@Param('id') id: string) {
    return this.service.getWorkOrder(id);
  }

  @ApiTags('Work Orders')
  @ApiOperation({ summary: 'Crear orden de trabajo' })
  @ApiBody({
    type: CreateWorkOrderDto,
    required: true,
    examples: { ejemplo: { value: bodyExamples.createWorkOrder } },
  })
  @Post('work-orders')
  createWorkOrder(@Body() dto: CreateWorkOrderDto) {
    return this.service.createWorkOrder(dto);
  }

  @ApiTags('Work Orders')
  @ApiOperation({ summary: 'Actualizar orden de trabajo por ID' })
  @ApiParam({
    name: 'id',
    description: 'ID de la orden de trabajo',
    required: true,
  })
  @ApiBody({ type: UpdateWorkOrderDto, required: true })
  @Patch('work-orders/:id')
  updateWorkOrder(@Param('id') id: string, @Body() dto: UpdateWorkOrderDto) {
    return this.service.updateWorkOrder(id, dto);
  }

  @ApiTags('Work Orders')
  @ApiOperation({ summary: 'Eliminar orden de trabajo por ID' })
  @ApiParam({
    name: 'id',
    description: 'ID de la orden de trabajo',
    required: true,
  })
  @Delete('work-orders/:id')
  deleteWorkOrder(@Param('id') id: string) {
    return this.service.deleteWorkOrder(id);
  }

  @ApiTags('Work Orders - Tareas')
  @ApiOperation({
    summary: 'Listar tareas registradas de una orden de trabajo',
  })
  @ApiParam({
    name: 'id',
    description: 'ID de la orden de trabajo',
    required: true,
  })
  @Get('work-orders/:id/tareas')
  listWorkOrderTareas(@Param('id') id: string) {
    return this.service.listWorkOrderTareas(id);
  }

  @ApiTags('Work Orders - Tareas')
  @ApiOperation({ summary: 'Crear tarea ejecutada para una orden de trabajo' })
  @ApiParam({
    name: 'id',
    description: 'ID de la orden de trabajo',
    required: true,
  })
  @ApiBody({
    type: CreateWorkOrderTareaDto,
    required: true,
    examples: { ejemplo: { value: bodyExamples.createWorkOrderTarea } },
  })
  @Post('work-orders/:id/tareas')
  createWorkOrderTarea(
    @Param('id') id: string,
    @Body() dto: CreateWorkOrderTareaDto,
  ) {
    return this.service.createWorkOrderTarea(id, dto);
  }

  @ApiTags('Work Orders - Tareas')
  @ApiOperation({ summary: 'Actualizar tarea ejecutada de OT por ID' })
  @ApiParam({ name: 'id', description: 'ID de la tarea de OT', required: true })
  @ApiBody({ type: UpdateWorkOrderTareaDto, required: true })
  @Patch('work-orders/tareas/:id')
  updateWorkOrderTarea(
    @Param('id') id: string,
    @Body() dto: UpdateWorkOrderTareaDto,
  ) {
    return this.service.updateWorkOrderTarea(id, dto);
  }

  @ApiTags('Work Orders - Tareas')
  @ApiOperation({ summary: 'Eliminar tarea ejecutada de OT por ID' })
  @ApiParam({ name: 'id', description: 'ID de la tarea de OT', required: true })
  @Delete('work-orders/tareas/:id')
  deleteWorkOrderTarea(@Param('id') id: string) {
    return this.service.deleteWorkOrderTarea(id);
  }

  @ApiTags('Work Orders - Adjuntos')
  @ApiOperation({ summary: 'Subir archivo adjunto de una OT en base64' })
  @ApiParam({
    name: 'id',
    description: 'ID de la orden de trabajo',
    required: true,
  })
  @ApiBody({
    type: UploadWorkOrderAdjuntoDto,
    required: true,
    examples: { ejemplo: { value: bodyExamples.uploadAdjunto } },
  })
  @Post('work-orders/:id/adjuntos')
  uploadWorkOrderAdjunto(
    @Param('id') id: string,
    @Body() dto: UploadWorkOrderAdjuntoDto,
  ) {
    return this.service.uploadWorkOrderAdjunto(id, dto);
  }

  @ApiTags('Work Orders - Adjuntos')
  @ApiOperation({ summary: 'Listar adjuntos de una OT' })
  @ApiParam({
    name: 'id',
    description: 'ID de la orden de trabajo',
    required: true,
  })
  @Get('work-orders/:id/adjuntos')
  listWorkOrderAdjuntos(
    @Param('id') id: string,
    @Query() query: WorkOrderAdjuntoQueryDto,
  ) {
    return this.service.listWorkOrderAdjuntos(id, query);
  }

  @ApiTags('Work Orders - Adjuntos')
  @ApiOperation({ summary: 'Descargar adjunto de una OT (base64 + data_url)' })
  @ApiParam({
    name: 'id',
    description: 'ID de la orden de trabajo',
    required: true,
  })
  @ApiParam({
    name: 'adjuntoId',
    description: 'ID del adjunto',
    required: true,
  })
  @Get('work-orders/:id/adjuntos/:adjuntoId')
  getWorkOrderAdjunto(
    @Param('id') id: string,
    @Param('adjuntoId') adjuntoId: string,
  ) {
    return this.service.getWorkOrderAdjunto(id, adjuntoId);
  }

  @ApiTags('Work Orders - Adjuntos')
  @ApiOperation({ summary: 'Visualizar adjunto de una OT directamente en el navegador' })
  @ApiParam({
    name: 'id',
    description: 'ID de la orden de trabajo',
    required: true,
  })
  @ApiParam({
    name: 'adjuntoId',
    description: 'ID del adjunto',
    required: true,
  })
  @Get('work-orders/:id/adjuntos/:adjuntoId/view')
  async viewWorkOrderAdjunto(
    @Param('id') id: string,
    @Param('adjuntoId') adjuntoId: string,
    @Res() res: Response,
  ) {
    const file = await this.service.resolveWorkOrderAdjuntoFile(id, adjuntoId);
    res.setHeader('Content-Type', file.mimeType);
    res.setHeader(
      'Content-Disposition',
      `inline; filename="${encodeURIComponent(file.fileName)}"`,
    );
    return res.sendFile(file.filePath);
  }

  @ApiTags('Work Orders - Adjuntos')
  @ApiOperation({ summary: 'Eliminar adjunto de una OT por ID' })
  @ApiParam({
    name: 'id',
    description: 'ID de la orden de trabajo',
    required: true,
  })
  @ApiParam({
    name: 'adjuntoId',
    description: 'ID del adjunto',
    required: true,
  })
  @Delete('work-orders/:id/adjuntos/:adjuntoId')
  deleteWorkOrderAdjunto(
    @Param('id') id: string,
    @Param('adjuntoId') adjuntoId: string,
  ) {
    return this.service.deleteWorkOrderAdjunto(id, adjuntoId);
  }

  @ApiTags('Inventory')
  @ApiOperation({ summary: 'Obtener costo de referencia de un producto por bodega' })
  @ApiQuery({ name: 'producto_id', required: true, description: 'ID del producto' })
  @ApiQuery({ name: 'bodega_id', required: true, description: 'ID de la bodega' })
  @Get('inventory/cost-reference')
  getInventoryCostReference(
    @Query('producto_id') productoId: string,
    @Query('bodega_id') bodegaId: string,
  ) {
    return this.service.getInventoryCostReference(productoId, bodegaId);
  }

  @ApiTags('Work Orders')
  @ApiOperation({ summary: 'Listar consumos de una orden de trabajo' })
  @ApiParam({
    name: 'id',
    description: 'ID de la orden de trabajo',
    required: true,
  })
  @Get('work-orders/:id/consumos')
  listConsumos(@Param('id') id: string) {
    return this.service.listConsumos(id);
  }

  @ApiTags('Work Orders')
  @ApiOperation({ summary: 'Listar historial de una orden de trabajo' })
  @ApiParam({
    name: 'id',
    description: 'ID de la orden de trabajo',
    required: true,
  })
  @Get('work-orders/:id/history')
  listWorkOrderHistory(@Param('id') id: string) {
    return this.service.listWorkOrderHistory(id);
  }

  @ApiTags('Work Orders')
  @ApiOperation({ summary: 'Registrar consumo en una orden de trabajo' })
  @ApiParam({
    name: 'id',
    description: 'ID de la orden de trabajo',
    required: true,
  })
  @ApiBody({
    type: CreateConsumoDto,
    required: true,
    examples: { ejemplo: { value: bodyExamples.createConsumo } },
  })
  @Post('work-orders/:id/consumos')
  createConsumo(@Param('id') id: string, @Body() dto: CreateConsumoDto) {
    return this.service.createConsumo(id, dto);
  }
  @ApiTags('Work Orders')
  @ApiOperation({ summary: 'Listar salidas de materiales de una orden de trabajo' })
  @ApiParam({
    name: 'id',
    description: 'ID de la orden de trabajo',
    required: true,
  })
  @Get('work-orders/:id/issue-materials')
  listIssueMaterials(@Param('id') id: string) {
    return this.service.listIssueMaterials(id);
  }

  @ApiTags('Work Orders')
  @ApiOperation({ summary: 'Emitir materiales en una orden de trabajo' })
  @ApiParam({
    name: 'id',
    description: 'ID de la orden de trabajo',
    required: true,
  })
  @ApiBody({
    type: IssueMaterialsDto,
    required: true,
    examples: { ejemplo: { value: bodyExamples.issueMaterials } },
  })
  @Post('work-orders/:id/issue-materials')
  issueMaterials(@Param('id') id: string, @Body() dto: IssueMaterialsDto) {
    return this.service.issueMaterials(id, dto);
  }
}
