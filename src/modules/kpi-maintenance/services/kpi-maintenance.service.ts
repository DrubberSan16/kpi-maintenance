import {
  BadRequestException,
  ConflictException,
  Inject,
  Injectable,
  Logger,
  NotFoundException,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { createHash } from 'crypto';
import { URLSearchParams } from 'url';
import { mkdir, readFile, unlink, writeFile } from 'fs/promises';
import { basename, extname, join } from 'path';
import { InjectDataSource, InjectRepository } from '@nestjs/typeorm';
import {
  DataSource,
  FindOptionsWhere,
  In,
  IsNull,
  ObjectLiteral,
  Repository,
} from 'typeorm';
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
  BodegaEntity,
  ProgramacionPlanEntity,
  ReservaStockEntity,
  WorkOrderStatusHistoryEntity,
  StockBodegaEntity,
  WorkOrderAdjuntoEntity,
  WorkOrderEntity,
  WorkOrderTareaEntity,
} from '../entities/kpi-maintenance.entity';
import {
  AlertaQueryDto,
  ChangeEstadoDto,
  ComponenteQueryDto,
  CreateBitacoraDto,
  CreateComponenteDto,
  CreateConsumoDto,
  CreateEquipoDto,
  CreateEquipoTipoDto,
  CreateEventoDto,
  CreateFallaCatalogoDto,
  CreateLecturaEquipoDto,
  CreateLubricacionPuntoDto,
  CreateLocationDto,
  CreatePlanDto,
  CreatePlanTareaDto,
  CreateProgramacionDto,
  CreateWorkOrderDto,
  CreateWorkOrderTareaDto,
  DateRangeDto,
  EquipoQueryDto,
  EquipoTipoQueryDto,
  IssueMaterialsDto,
  LocationQueryDto,
  UpdateBitacoraDto,
  UpdateComponenteDto,
  UpdateEquipoDto,
  UpdateEquipoTipoDto,
  UpdateFallaCatalogoDto,
  UpdateLecturaEquipoDto,
  UpdateLocationDto,
  UpdateLubricacionPuntoDto,
  UpdatePlanDto,
  UpdatePlanTareaDto,
  UpdateProgramacionDto,
  UpdateWorkOrderDto,
  UpdateWorkOrderTareaDto,
  UploadWorkOrderAdjuntoDto,
  WorkOrderAdjuntoQueryDto,
  WorkOrderQueryDto,
} from '../dto';

@Injectable()
export class KpiMaintenanceService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(KpiMaintenanceService.name);
  private recalculationInterval: NodeJS.Timeout | null = null;
  private recalculationRunning = false;

  private readonly RECALCULATION_INTERVAL_MS = 2 * 60 * 1000;
  private readonly RECALCULATION_BATCH_SIZE = 100;
  private readonly RECALCULATION_WORKERS = 4;
  private readonly MAX_STORED_ERRORS = 200;

  constructor(
    @InjectRepository(EquipoEntity)
    private readonly equipoRepo: Repository<EquipoEntity>,
    @InjectRepository(EquipoTipoEntity)
    private readonly equipoTipoRepo: Repository<EquipoTipoEntity>,
    @InjectRepository(EquipoComponenteEntity)
    private readonly equipoComponenteRepo: Repository<EquipoComponenteEntity>,
    @InjectRepository(LocationEntity)
    private readonly locationRepo: Repository<LocationEntity>,
    @InjectRepository(BitacoraDiariaEntity)
    private readonly bitacoraRepo: Repository<BitacoraDiariaEntity>,
    @InjectRepository(AlertaMantenimientoEntity)
    private readonly alertaRepo: Repository<AlertaMantenimientoEntity>,
    @InjectRepository(EstadoEquipoEntity)
    private readonly estadoRepo: Repository<EstadoEquipoEntity>,
    @InjectRepository(EstadoEquipoCatalogoEntity)
    private readonly estadoCatalogoRepo: Repository<EstadoEquipoCatalogoEntity>,
    @InjectRepository(EventoEquipoEntity)
    private readonly eventoRepo: Repository<EventoEquipoEntity>,
    @InjectRepository(FallaCatalogoEntity)
    private readonly fallaRepo: Repository<FallaCatalogoEntity>,
    @InjectRepository(LecturaEquipoEntity)
    private readonly lecturaRepo: Repository<LecturaEquipoEntity>,
    @InjectRepository(LubricacionPuntoEntity)
    private readonly lubricacionRepo: Repository<LubricacionPuntoEntity>,
    @InjectRepository(PlanMantenimientoEntity)
    private readonly planRepo: Repository<PlanMantenimientoEntity>,
    @InjectRepository(PlanTareaEntity)
    private readonly planTareaRepo: Repository<PlanTareaEntity>,
    @InjectRepository(ProgramacionPlanEntity)
    private readonly programacionRepo: Repository<ProgramacionPlanEntity>,
    @InjectRepository(WorkOrderEntity)
    private readonly woRepo: Repository<WorkOrderEntity>,
    @InjectRepository(WorkOrderStatusHistoryEntity)
    private readonly woHistoryRepo: Repository<WorkOrderStatusHistoryEntity>,
    @InjectRepository(ConsumoRepuestoEntity)
    private readonly consumoRepo: Repository<ConsumoRepuestoEntity>,
    @InjectRepository(StockBodegaEntity)
    private readonly stockRepo: Repository<StockBodegaEntity>,
    @InjectRepository(ProductoEntity)
    private readonly productoRepo: Repository<ProductoEntity>,
    @InjectRepository(BodegaEntity)
    private readonly bodegaRepo: Repository<BodegaEntity>,
    @InjectRepository(ReservaStockEntity)
    private readonly reservaRepo: Repository<ReservaStockEntity>,
    @InjectRepository(WorkOrderTareaEntity)
    private readonly woTareaRepo: Repository<WorkOrderTareaEntity>,
    @InjectRepository(WorkOrderAdjuntoEntity)
    private readonly woAdjuntoRepo: Repository<WorkOrderAdjuntoEntity>,
    @InjectDataSource() private readonly dataSource: DataSource,
  ) {}

  private readonly uploadRoot =
    process.env.WORK_ORDER_UPLOAD_DIR ||
    join(process.cwd(), 'storage', 'work-orders');

  private buildProductoLabel(producto?: Partial<ProductoEntity> | null) {
    if (!producto) return null;
    const codigo = String(producto.codigo || '').trim();
    const nombre = String(producto.nombre || '').trim();
    if (codigo && nombre) return `${codigo} - ${nombre}`;
    return nombre || codigo || null;
  }

  private buildBodegaLabel(bodega?: Partial<BodegaEntity> | null) {
    if (!bodega) return null;
    const codigo = String(bodega.codigo || '').trim();
    const nombre = String(bodega.nombre || '').trim();
    if (codigo && nombre) return `${codigo} - ${nombre}`;
    return nombre || codigo || null;
  }

  private async buildInventoryCatalogMaps(productIds: string[], warehouseIds: string[]) {
    const uniqueProductIds = [...new Set(productIds.filter(Boolean))];
    const uniqueWarehouseIds = [...new Set(warehouseIds.filter(Boolean))];

    const [productos, bodegas] = await Promise.all([
      uniqueProductIds.length
        ? this.productoRepo.find({ where: { id: In(uniqueProductIds) } })
        : Promise.resolve([] as ProductoEntity[]),
      uniqueWarehouseIds.length
        ? this.bodegaRepo.find({ where: { id: In(uniqueWarehouseIds) } })
        : Promise.resolve([] as BodegaEntity[]),
    ]);

    return {
      productMap: new Map(productos.map((item) => [item.id, item])),
      warehouseMap: new Map(bodegas.map((item) => [item.id, item])),
    };
  }

  private mapConsumoWithCatalogs(
    row: ConsumoRepuestoEntity,
    productMap: Map<string, ProductoEntity>,
    warehouseMap: Map<string, BodegaEntity>,
  ) {
    const producto = productMap.get(row.producto_id);
    const bodega = row.bodega_id ? warehouseMap.get(row.bodega_id) : undefined;
    return {
      ...row,
      producto_codigo: producto?.codigo ?? null,
      producto_nombre: producto?.nombre ?? null,
      producto_label: this.buildProductoLabel(producto) ?? row.producto_id,
      bodega_codigo: bodega?.codigo ?? null,
      bodega_nombre: bodega?.nombre ?? null,
      bodega_label: this.buildBodegaLabel(bodega) ?? row.bodega_id ?? null,
    };
  }

  private mapIssueItemWithCatalogs(
    row: EntregaMaterialDetEntity,
    productMap: Map<string, ProductoEntity>,
    warehouseMap: Map<string, BodegaEntity>,
  ) {
    const producto = productMap.get(row.producto_id);
    const bodega = warehouseMap.get(row.bodega_id);
    return {
      ...row,
      producto_codigo: producto?.codigo ?? null,
      producto_nombre: producto?.nombre ?? null,
      producto_label: this.buildProductoLabel(producto) ?? row.producto_id,
      bodega_codigo: bodega?.codigo ?? null,
      bodega_nombre: bodega?.nombre ?? null,
      bodega_label: this.buildBodegaLabel(bodega) ?? row.bodega_id,
    };
  }

  private async validateProductoEnBodega(productoId: string, bodegaId: string) {
    const [producto, bodega, stock] = await Promise.all([
      this.productoRepo.findOne({ where: { id: productoId } }),
      this.bodegaRepo.findOne({ where: { id: bodegaId } }),
      this.stockRepo.findOne({ where: { producto_id: productoId, bodega_id: bodegaId } }),
    ]);

    if (!producto) {
      throw new NotFoundException('Producto no encontrado');
    }
    if (!bodega) {
      throw new NotFoundException('Bodega no encontrada');
    }
    if (!stock) {
      throw new BadRequestException('El producto seleccionado no pertenece a la bodega indicada.');
    }

    return { producto, bodega, stock };
  }

  onModuleInit() {
    this.scheduleAlertRecalculation();
    this.triggerAlertRecalculation('startup').catch((e: any) => {
      this.logger.error(
        `No se pudo iniciar recálculo de alertas en startup: ${e?.message ?? 'desconocido'}`,
      );
    });
  }

  onModuleDestroy() {
    if (this.recalculationInterval) {
      clearInterval(this.recalculationInterval);
      this.recalculationInterval = null;
    }
  }

  private scheduleAlertRecalculation() {
    if (this.recalculationInterval) return;
    this.recalculationInterval = setInterval(() => {
      this.triggerAlertRecalculation('interval').catch((e: any) => {
        this.logger.error(
          `Error en recálculo automático de alertas: ${e?.message ?? 'desconocido'}`,
        );
      });
    }, this.RECALCULATION_INTERVAL_MS);
  }

  async triggerAlertRecalculation(source = 'manual') {
    if (this.recalculationRunning) {
      return this.wrap(
        { accepted: false, source },
        'Recalculo de alertas ya se encuentra en ejecución',
      );
    }

    this.recalculationRunning = true;
    void this.recalculateAlertas()
      .then((result) => {
        const stats = result.data as { total?: number; skipped?: number };
        this.logger.log(
          `Recalculo de alertas finalizado (${source}): total=${stats.total ?? 0}, skipped=${stats.skipped ?? 0}`,
        );
      })
      .catch((e: any) => {
        this.logger.error(
          `Error ejecutando recálculo de alertas (${source}): ${e?.message ?? 'desconocido'}`,
        );
      })
      .finally(() => {
        this.recalculationRunning = false;
      });

    return this.wrap(
      { accepted: true, source },
      'Recalculo de alertas ejecutándose en segundo plano',
    );
  }

  private wrap(data: unknown, message = 'OK', meta?: unknown) {
    return { data, meta, message };
  }


  private readonly notificationServiceUrl = (
    process.env.NOTIFICATION_SERVICE_URL || process.env.KPI_NOTIFICATION_URL || ''
  ).replace(/\/$/, '');

  private readonly securityServiceUrl = (
    process.env.SECURITY_SERVICE_URL || process.env.KPI_SECURITY_URL || ''
  ).replace(/\/$/, '');

  private readonly publicBaseUrl = (
    process.env.PUBLIC_BASE_URL || process.env.APP_PUBLIC_URL || ''
  ).replace(/\/$/, '');

  private toNumeric(value: unknown, fallback = 0) {
    const num = Number(value);
    return Number.isFinite(num) ? num : fallback;
  }

  private normalizeWorkflowStatus(value: unknown) {
    const raw = String(value || '').trim().toUpperCase();
    if (['PLANNED', 'PLANIFICADA', 'PLANIFICADO', 'CREADA', 'CREADO'].includes(raw)) return 'PLANNED';
    if (['IN_PROGRESS', 'IN PROGRESS', 'EN_PROCESO', 'EN PROCESO', 'PROCESSING'].includes(raw)) return 'IN_PROGRESS';
    if (['CLOSED', 'CERRADA', 'CERRADO', 'DONE', 'COMPLETED'].includes(raw)) return 'CLOSED';
    return raw || 'PLANNED';
  }

  private buildMaintenanceRelativePath(path: string) {
    const basePath = String(process.env.BASE_PATH || '/kpi_maintenance').replace(/\/$/, '');
    return `${basePath}${path.startsWith('/') ? path : `/${path}`}`;
  }

  private buildMaintenancePublicUrl(path: string) {
    const relative = this.buildMaintenanceRelativePath(path);
    return this.publicBaseUrl ? `${this.publicBaseUrl}${relative}` : relative;
  }

  private async postJson(url: string, payload: Record<string, unknown>) {
    const response = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    if (!response.ok) {
      const body = await response.text().catch(() => '');
      throw new Error(`HTTP ${response.status}: ${body || response.statusText}`);
    }
    return response;
  }

  private async publishInAppNotification(payload: {
    title: string;
    body: string;
    module?: string;
    entityType?: string;
    entityId?: string | null;
    level?: string;
    recipients?: string[];
  }) {
    if (!this.notificationServiceUrl) return;
    try {
      await this.postJson(`${this.notificationServiceUrl}/notifications/in-app`, {
        title: payload.title,
        body: payload.body,
        module: payload.module ?? 'maintenance',
        entityType: payload.entityType ?? 'work-order',
        entityId: payload.entityId ?? null,
        level: payload.level ?? 'info',
        recipients: payload.recipients ?? [],
      });
    } catch (error: any) {
      this.logger.warn(`No se pudo publicar notificación: ${error?.message ?? 'desconocido'}`);
    }
  }

  private async writeSecurityLog(payload: {
    description: string;
    status?: string;
    typeLog?: string;
    createdBy?: string | null;
  }) {
    if (!this.securityServiceUrl) return;
    try {
      await this.postJson(`${this.securityServiceUrl}/log-transacts`, {
        moduleMicroservice: 'kpi_maintenance',
        status: payload.status ?? 'SUCCESS',
        typeLog: payload.typeLog ?? 'WORK_ORDER',
        description: payload.description,
        createdBy: payload.createdBy ?? null,
      });
    } catch (error: any) {
      this.logger.warn(`No se pudo registrar log transaccional: ${error?.message ?? 'desconocido'}`);
    }
  }

  private async appendWorkOrderHistory(
    workOrderId: string,
    toStatus: string,
    note: string,
    options?: { fromStatus?: string | null; changedBy?: string | null },
  ) {
    return this.woHistoryRepo.save(
      this.woHistoryRepo.create({
        work_order_id: workOrderId,
        from_status: options?.fromStatus ?? null,
        to_status: toStatus,
        changed_by: options?.changedBy ?? null,
        note,
      }),
    );
  }

  private applyWorkflowDates(
    workOrder: WorkOrderEntity,
    previousStatus: string | null,
    nextStatus: string,
  ) {
    if (nextStatus === 'IN_PROGRESS' && !workOrder.started_at) {
      workOrder.started_at = new Date();
    }
    if (nextStatus === 'CLOSED' && !workOrder.closed_at) {
      workOrder.closed_at = new Date();
      if (!workOrder.started_at) workOrder.started_at = new Date();
    }
    if (previousStatus === 'CLOSED' && nextStatus !== 'CLOSED') {
      workOrder.closed_at = null;
    }
  }

  private addInterval(dateInput: string | Date, frequencyType: string, frequencyValue: number) {
    const date = new Date(dateInput);
    const type = String(frequencyType || 'DIAS').toUpperCase();
    const value = this.toNumeric(frequencyValue, 0);
    if (!value) return date;
    if (type === 'SEMANAS') {
      date.setDate(date.getDate() + value * 7);
      return date;
    }
    if (type === 'MESES') {
      date.setMonth(date.getMonth() + value);
      return date;
    }
    date.setDate(date.getDate() + value);
    return date;
  }

  private async recalculateProgramacionFields(
    programacion: ProgramacionPlanEntity,
    options?: { persist?: boolean },
  ) {
    const equipo = await this.findEquipoOrFail(programacion.equipo_id);
    const plan = await this.findOneOrFail(this.planRepo, {
      id: programacion.plan_id,
      is_deleted: false,
    });

    const freqType = String(plan.frecuencia_tipo || 'HORAS').toUpperCase();
    const freqValue = this.toNumeric(plan.frecuencia_valor, 0);
    const patch: Partial<ProgramacionPlanEntity> = {};

    if (freqType === 'HORAS') {
      const baseHours =
        programacion.ultima_ejecucion_horas != null
          ? this.toNumeric(programacion.ultima_ejecucion_horas)
          : this.toNumeric(equipo.horometro_actual);
      patch.proxima_horas = Number((baseHours + freqValue).toFixed(2));
      if (!programacion.ultima_ejecucion_horas && equipo.horometro_actual != null) {
        patch.ultima_ejecucion_horas = this.toNumeric(equipo.horometro_actual);
      }
    } else {
      const baseDate =
        programacion.ultima_ejecucion_fecha ||
        new Date().toISOString().slice(0, 10);
      patch.proxima_fecha = this
        .addInterval(baseDate, freqType, freqValue)
        .toISOString()
        .slice(0, 10);
      if (!programacion.ultima_ejecucion_fecha) {
        patch.ultima_ejecucion_fecha = baseDate;
      }
    }

    const currentHours = this.toNumeric(equipo.horometro_actual);
    const nextHours = patch.proxima_horas ?? programacion.proxima_horas ?? null;
    const nextDate = patch.proxima_fecha ?? programacion.proxima_fecha ?? null;
    const hoursRemaining = nextHours == null ? null : Number((this.toNumeric(nextHours) - currentHours).toFixed(2));
    const today = new Date();
    today.setHours(0, 0, 0, 0);
    const daysRemaining =
      nextDate == null
        ? null
        : Math.ceil((new Date(nextDate).getTime() - today.getTime()) / 86400000);
    const dueByHours = hoursRemaining != null && hoursRemaining <= 0;
    const dueByDate = daysRemaining != null && daysRemaining <= 0;
    const dueSoon =
      (hoursRemaining != null && hoursRemaining > 0 && hoursRemaining <= Math.max(1, freqValue * 0.1)) ||
      (daysRemaining != null && daysRemaining > 0 && daysRemaining <= 3);

    const enriched = {
      ...programacion,
      ...patch,
      equipo_nombre: equipo.nombre,
      equipo_codigo: equipo.codigo,
      plan_nombre: plan.nombre,
      plan_codigo: plan.codigo,
      frecuencia_tipo: plan.frecuencia_tipo,
      frecuencia_valor: plan.frecuencia_valor,
      horometro_actual: currentHours,
      horas_restantes: hoursRemaining,
      dias_restantes: daysRemaining,
      estado_programacion: dueByHours || dueByDate ? 'VENCIDA' : dueSoon ? 'PROXIMA' : 'PROGRAMADA',
    };

    if (options?.persist !== false) {
      const changed = Object.entries(patch).some(([key, value]) => (programacion as any)[key] !== value);
      if (changed) {
        Object.assign(programacion, patch);
        await this.programacionRepo.save(programacion);
      }
    }

    return enriched;
  }

  private async enrichWorkOrder(workOrder: WorkOrderEntity) {
    const [equipo, plan] = await Promise.all([
      workOrder.equipment_id
        ? this.equipoRepo.findOne({ where: { id: workOrder.equipment_id, is_deleted: false } })
        : Promise.resolve(null),
      workOrder.plan_id
        ? this.planRepo.findOne({ where: { id: workOrder.plan_id, is_deleted: false } })
        : Promise.resolve(null),
    ]);

    return {
      ...workOrder,
      status_workflow: this.normalizeWorkflowStatus(workOrder.status_workflow),
      equipment_nombre: equipo?.nombre ?? null,
      equipment_codigo: equipo?.codigo ?? null,
      plan_nombre: plan?.nombre ?? null,
      plan_codigo: plan?.codigo ?? null,
    };
  }

  async listEquipos(query: EquipoQueryDto) {
    const page = query.page ?? 1;
    const limit = Math.min(query.limit ?? 10, 100);
    const qb = this.equipoRepo
      .createQueryBuilder('e')
      .where('e.is_deleted = false');
    if (query.codigo)
      qb.andWhere('e.codigo ILIKE :codigo', { codigo: `%${query.codigo}%` });
    if (query.marca_id)
      qb.andWhere('e.marca_id = :marca_id', { marca_id: query.marca_id });
    if (query.location_id)
      qb.andWhere('e.location_id = :location_id', {
        location_id: query.location_id,
      });
    if (query.equipo_tipo_id)
      qb.andWhere('e.equipo_tipo_id = :equipo_tipo_id', {
        equipo_tipo_id: query.equipo_tipo_id,
      });
    if (query.estado_operativo)
      qb.andWhere('e.estado_operativo = :estado_operativo', {
        estado_operativo: query.estado_operativo,
      });
    if (query.criticidad)
      qb.andWhere('e.criticidad = :criticidad', {
        criticidad: query.criticidad,
      });
    const [data, total] = await qb
      .orderBy('e.created_at', 'DESC')
      .skip((page - 1) * limit)
      .take(limit)
      .getManyAndCount();
    return this.wrap(data, 'Equipos listados', { page, limit, total });
  }

  async getEquipo(id: string) {
    return this.wrap(await this.findEquipoOrFail(id), 'Equipo obtenido');
  }
  async createEquipo(dto: CreateEquipoDto) {
    return this.wrap(
      await this.equipoRepo.save(
        this.equipoRepo.create({
          ...dto,
          horometro_actual: dto.horometro_actual ?? 0,
        }),
      ),
      'Equipo creado',
    );
  }
  async updateEquipo(id: string, dto: UpdateEquipoDto) {
    const e = await this.findEquipoOrFail(id);
    Object.assign(e, dto);
    return this.wrap(await this.equipoRepo.save(e), 'Equipo actualizado');
  }
  async deleteEquipo(id: string) {
    const e = await this.findEquipoOrFail(id);
    e.is_deleted = true;
    e.deleted_at = new Date();
    await this.equipoRepo.save(e);
    return this.wrap(true, 'Equipo eliminado');
  }

  async listEquipoTipos(query: EquipoTipoQueryDto) {
    const page = query.page ?? 1;
    const limit = Math.min(query.limit ?? 10, 100);
    const qb = this.equipoTipoRepo
      .createQueryBuilder('t')
      .where('t.is_deleted = false');
    if (query.codigo)
      qb.andWhere('t.codigo ILIKE :codigo', { codigo: `%${query.codigo}%` });
    if (query.nombre)
      qb.andWhere('t.nombre ILIKE :nombre', { nombre: `%${query.nombre}%` });
    return this.wrap(
      await qb
        .skip((page - 1) * limit)
        .take(limit)
        .getMany(),
      'Tipos de equipo listados',
    );
  }
  async createEquipoTipo(dto: CreateEquipoTipoDto) {
    return this.wrap(
      await this.equipoTipoRepo.manager.save(
        EquipoTipoEntity,
        this.equipoTipoRepo.manager.create(EquipoTipoEntity, dto),
      ),
      'Tipo de equipo creado',
    );
  }
  async updateEquipoTipo(id: string, dto: UpdateEquipoTipoDto) {
    const t = await this.findOneOrFail(
      this.equipoTipoRepo.manager.getRepository(EquipoTipoEntity),
      {
        id,
        is_deleted: false,
      },
    );
    Object.assign(t, dto);
    return this.wrap(
      await this.equipoTipoRepo.manager.save(EquipoTipoEntity, t),
      'Tipo de equipo actualizado',
    );
  }
  async deleteEquipoTipo(id: string) {
    const t = await this.findOneOrFail(
      this.equipoTipoRepo.manager.getRepository(EquipoTipoEntity),
      {
        id,
        is_deleted: false,
      },
    );
    t.is_deleted = true;
    t.deleted_at = new Date();
    await this.equipoTipoRepo.manager.save(EquipoTipoEntity, t);
    return this.wrap(true, 'Tipo de equipo eliminado');
  }

  async listLocations(query: LocationQueryDto) {
    const page = query.page ?? 1;
    const limit = Math.min(query.limit ?? 10, 100);
    const qb = this.locationRepo
      .createQueryBuilder('l')
      .where('l.is_deleted = false');
    if (query.codigo)
      qb.andWhere('l.codigo ILIKE :codigo', { codigo: `%${query.codigo}%` });
    if (query.nombre)
      qb.andWhere('l.nombre ILIKE :nombre', { nombre: `%${query.nombre}%` });
    return this.wrap(
      await qb
        .skip((page - 1) * limit)
        .take(limit)
        .getMany(),
      'Locations listados',
    );
  }

  async getLocation(id: string) {
    return this.wrap(
      await this.findOneOrFail(
        this.locationRepo.manager.getRepository(LocationEntity),
        {
          id,
          is_deleted: false,
        },
      ),
      'Location obtenida',
    );
  }

  async createLocation(dto: CreateLocationDto) {
    return this.wrap(
      await this.locationRepo.manager.save(
        LocationEntity,
        this.locationRepo.manager.create(LocationEntity, dto),
      ),
      'Location creada',
    );
  }

  async updateLocation(id: string, dto: UpdateLocationDto) {
    const l = await this.findOneOrFail(
      this.locationRepo.manager.getRepository(LocationEntity),
      {
        id,
        is_deleted: false,
      },
    );
    Object.assign(l, dto);
    return this.wrap(
      await this.locationRepo.manager.save(LocationEntity, l),
      'Location actualizada',
    );
  }

  async deleteLocation(id: string) {
    const l = await this.findOneOrFail(
      this.locationRepo.manager.getRepository(LocationEntity),
      {
        id,
        is_deleted: false,
      },
    );
    l.is_deleted = true;
    l.deleted_at = new Date();
    await this.locationRepo.manager.save(LocationEntity, l);
    return this.wrap(true, 'Location eliminada');
  }

  async listBitacora(equipoId: string, range: DateRangeDto) {
    await this.findEquipoOrFail(equipoId);
    const qb = this.bitacoraRepo
      .createQueryBuilder('b')
      .where('b.equipo_id = :equipoId and b.is_deleted = false', { equipoId });
    if (range.from) qb.andWhere('b.fecha >= :from', { from: range.from });
    if (range.to) qb.andWhere('b.fecha <= :to', { to: range.to });
    return this.wrap(
      await qb.orderBy('b.fecha', 'DESC').getMany(),
      'Bitácora listada',
    );
  }

  async createBitacora(equipoId: string, dto: CreateBitacoraDto) {
    const equipo = await this.findEquipoOrFail(equipoId);
    const last = await this.bitacoraRepo.findOne({
      where: { equipo_id: equipoId, is_deleted: false },
      order: { fecha: 'DESC', created_at: 'DESC' },
    });
    if (last && Number(dto.horometro) < Number(last.horometro)) {
      await this.alertaRepo.save(
        this.alertaRepo.create({
          equipo_id: equipoId,
          tipo_alerta: 'ANOMALIA_HOROMETRO',
          detalle: `Horómetro ${dto.horometro} menor al último ${last.horometro}`,
        }),
      );
      throw new ConflictException('El horómetro no puede retroceder');
    }
    const saved = await this.bitacoraRepo.save(
      this.bitacoraRepo.create({ ...dto, equipo_id: equipoId }),
    );
    equipo.horometro_actual = dto.horometro;
    equipo.fecha_ultima_lectura = new Date(dto.fecha);
    await this.equipoRepo.save(equipo);
    await this.triggerAlertRecalculation('bitacora');
    return this.wrap(saved, 'Bitácora creada');
  }

  async updateBitacora(id: string, dto: UpdateBitacoraDto) {
    const b = await this.findOneOrFail(this.bitacoraRepo, {
      id,
      is_deleted: false,
    });
    Object.assign(b, dto);
    const saved = await this.bitacoraRepo.save(b);
    const equipo = await this.findEquipoOrFail(saved.equipo_id);
    if (dto.horometro != null) equipo.horometro_actual = dto.horometro;
    if (dto.fecha) equipo.fecha_ultima_lectura = new Date(dto.fecha);
    await this.equipoRepo.save(equipo);
    await this.triggerAlertRecalculation('bitacora-update');
    return this.wrap(saved, 'Bitácora actualizada');
  }
  async deleteBitacora(id: string) {
    const b = await this.findOneOrFail(this.bitacoraRepo, {
      id,
      is_deleted: false,
    });
    b.is_deleted = true;
    await this.bitacoraRepo.save(b);
    return this.wrap(true, 'Bitácora eliminada');
  }

  async changeEstado(equipoId: string, dto: ChangeEstadoDto) {
    const equipo = await this.findEquipoOrFail(equipoId);
    const estadoCatalogo = await this.findOneOrFail(this.estadoCatalogoRepo, {
      id: dto.estado_id,
      is_deleted: false,
    });
    const current = await this.estadoRepo.findOne({
      where: { equipo_id: equipoId, fecha_fin: IsNull(), is_deleted: false },
      order: { fecha_inicio: 'DESC' },
    });
    if (current) {
      current.fecha_fin = new Date(dto.fecha_inicio);
      await this.estadoRepo.save(current);
    }
    const nuevo = await this.estadoRepo.save(
      this.estadoRepo.create({
        equipo_id: equipoId,
        estado_id: dto.estado_id,
        fecha_inicio: new Date(dto.fecha_inicio),
        motivo: dto.motivo,
      }),
    );
    equipo.estado_operativo = estadoCatalogo.codigo;
    await this.equipoRepo.save(equipo);
    return this.wrap(nuevo, 'Estado actualizado');
  }

  async listEstados(equipoId: string, range: DateRangeDto) {
    const qb = this.estadoRepo
      .createQueryBuilder('e')
      .where('e.equipo_id = :equipoId and e.is_deleted = false', { equipoId });
    if (range.from)
      qb.andWhere('e.fecha_inicio >= :from', { from: range.from });
    if (range.to) qb.andWhere('e.fecha_inicio <= :to', { to: range.to });
    return this.wrap(
      await qb.orderBy('e.fecha_inicio', 'DESC').getMany(),
      'Estados listados',
    );
  }

  createEvento(equipoId: string, dto: CreateEventoDto) {
    return this.wrap(
      this.eventoRepo.save(
        this.eventoRepo.create({ ...dto, equipo_id: equipoId }),
      ),
      'Evento creado',
    );
  }
  async listEventos(
    equipoId: string,
    range: DateRangeDto & { tipo_evento?: string },
  ) {
    const qb = this.eventoRepo
      .createQueryBuilder('e')
      .where('e.equipo_id = :equipoId and e.is_deleted = false', { equipoId });
    if (range.from)
      qb.andWhere('e.fecha_inicio >= :from', { from: range.from });
    if (range.to) qb.andWhere('e.fecha_inicio <= :to', { to: range.to });
    if (range.tipo_evento)
      qb.andWhere('e.tipo_evento = :tipo', { tipo: range.tipo_evento });
    return this.wrap(await qb.getMany(), 'Eventos listados');
  }

  async createPlan(dto: CreatePlanDto) {
    return this.wrap(
      await this.planRepo.save(this.planRepo.create(dto)),
      'Plan creado',
    );
  }
  async listPlanes() {
    return this.wrap(
      await this.planRepo.find({ where: { is_deleted: false } }),
      'Planes listados',
    );
  }
  async getPlan(id: string) {
    return this.wrap(
      await this.findOneOrFail(this.planRepo, { id, is_deleted: false }),
      'Plan obtenido',
    );
  }
  async updatePlan(id: string, dto: UpdatePlanDto) {
    const p = await this.findOneOrFail(this.planRepo, {
      id,
      is_deleted: false,
    });
    Object.assign(p, dto);
    return this.wrap(await this.planRepo.save(p), 'Plan actualizado');
  }
  async deletePlan(id: string) {
    const p = await this.findOneOrFail(this.planRepo, {
      id,
      is_deleted: false,
    });
    p.is_deleted = true;
    await this.planRepo.save(p);
    return this.wrap(true, 'Plan eliminado');
  }
  async createPlanTarea(planId: string, dto: CreatePlanTareaDto) {
    return this.wrap(
      await this.planTareaRepo.save(
        this.planTareaRepo.create({ ...dto, plan_id: planId }),
      ),
      'Tarea creada',
    );
  }
  async listPlanTareas(planId: string) {
    return this.wrap(
      await this.planTareaRepo.find({
        where: { plan_id: planId, is_deleted: false },
        order: { orden: 'ASC' },
      }),
      'Tareas listadas',
    );
  }
  async updatePlanTarea(id: string, dto: UpdatePlanTareaDto) {
    const t = await this.findOneOrFail(this.planTareaRepo, {
      id,
      is_deleted: false,
    });
    Object.assign(t, dto);
    return this.wrap(await this.planTareaRepo.save(t), 'Tarea actualizada');
  }
  async deletePlanTarea(id: string) {
    const t = await this.findOneOrFail(this.planTareaRepo, {
      id,
      is_deleted: false,
    });
    t.is_deleted = true;
    await this.planTareaRepo.save(t);
    return this.wrap(true, 'Tarea eliminada');
  }

  async createProgramacion(dto: CreateProgramacionDto) {
    await this.findEquipoOrFail(dto.equipo_id);
    await this.findOneOrFail(this.planRepo, { id: dto.plan_id, is_deleted: false });
    const entity = this.programacionRepo.create(dto);
    const saved = await this.programacionRepo.save(entity);
    const enriched = await this.recalculateProgramacionFields(saved);
    await this.publishInAppNotification({
      title: 'Nueva programación de mantenimiento',
      body: `${enriched.plan_nombre} programado para ${enriched.equipo_nombre}`,
      module: 'maintenance',
      entityType: 'programacion',
      entityId: saved.id,
      level: 'info',
    });
    await this.writeSecurityLog({
      description: `[PROGRAMACION:${saved.id}] Programación creada para equipo ${saved.equipo_id} y plan ${saved.plan_id}` ,
      typeLog: 'PROGRAMACION',
    });
    return this.wrap(enriched, 'Programación creada');
  }
  async listProgramaciones() {
    const rows = await this.programacionRepo.find({ where: { is_deleted: false, activo: true } });
    const data = await Promise.all(rows.map((row) => this.recalculateProgramacionFields(row, { persist: false })));
    data.sort((a: any, b: any) => {
      const left = a.proxima_fecha || '';
      const right = b.proxima_fecha || '';
      if (left && right) return String(left).localeCompare(String(right));
      return this.toNumeric(a.proxima_horas, 99999999) - this.toNumeric(b.proxima_horas, 99999999);
    });
    return this.wrap(data, 'Programaciones listadas');
  }
  async getProgramacion(id: string) {
    const row = await this.findOneOrFail(this.programacionRepo, {
      id,
      is_deleted: false,
    });
    return this.wrap(
      await this.recalculateProgramacionFields(row, { persist: false }),
      'Programación obtenida',
    );
  }
  async updateProgramacion(id: string, dto: UpdateProgramacionDto) {
    const p = await this.findOneOrFail(this.programacionRepo, {
      id,
      is_deleted: false,
    });
    Object.assign(p, dto);
    const saved = await this.programacionRepo.save(p);
    const enriched = await this.recalculateProgramacionFields(saved);
    await this.publishInAppNotification({
      title: 'Programación actualizada',
      body: `${enriched.plan_nombre} actualizado para ${enriched.equipo_nombre}`,
      module: 'maintenance',
      entityType: 'programacion',
      entityId: saved.id,
      level: 'info',
    });
    await this.writeSecurityLog({
      description: `[PROGRAMACION:${saved.id}] Programación actualizada`,
      typeLog: 'PROGRAMACION',
    });
    return this.wrap(enriched, 'Programación actualizada');
  }
  async deleteProgramacion(id: string) {
    const p = await this.findOneOrFail(this.programacionRepo, {
      id,
      is_deleted: false,
    });
    p.is_deleted = true;
    await this.programacionRepo.save(p);
    return this.wrap(true, 'Programación eliminada');
  }

  async listComponentes(query: ComponenteQueryDto) {
    const where: FindOptionsWhere<EquipoComponenteEntity> = {
      is_deleted: false,
    };
    if (query.equipo_id) where.equipo_id = query.equipo_id;
    return this.wrap(
      await this.equipoComponenteRepo.find({ where }),
      'Componentes listados',
    );
  }

  async getComponente(id: string) {
    return this.wrap(
      await this.findOneOrFail(this.equipoComponenteRepo, {
        id,
        is_deleted: false,
      }),
      'Componente obtenido',
    );
  }
  async createComponente(dto: CreateComponenteDto) {
    await this.findEquipoOrFail(dto.equipo_id);
    if (dto.parent_id)
      await this.findOneOrFail(this.equipoComponenteRepo, {
        id: dto.parent_id,
        is_deleted: false,
      });
    return this.wrap(
      await this.equipoComponenteRepo.save(
        this.equipoComponenteRepo.create(dto),
      ),
      'Componente creado',
    );
  }
  async updateComponente(id: string, dto: UpdateComponenteDto) {
    const item = await this.findOneOrFail(this.equipoComponenteRepo, {
      id,
      is_deleted: false,
    });
    Object.assign(item, dto);
    return this.wrap(
      await this.equipoComponenteRepo.save(item),
      'Componente actualizado',
    );
  }
  async deleteComponente(id: string) {
    const item = await this.findOneOrFail(this.equipoComponenteRepo, {
      id,
      is_deleted: false,
    });
    item.is_deleted = true;
    await this.equipoComponenteRepo.save(item);
    return this.wrap(true, 'Componente eliminado');
  }

  async listFallasCatalogo() {
    return this.wrap(
      await this.fallaRepo.find({ where: { is_deleted: false } }),
      'Fallas listadas',
    );
  }
  async getFallaCatalogo(id: string) {
    return this.wrap(
      await this.findOneOrFail(this.fallaRepo, { id, is_deleted: false }),
      'Falla obtenida',
    );
  }
  async createFallaCatalogo(dto: CreateFallaCatalogoDto) {
    return this.wrap(
      await this.fallaRepo.save(this.fallaRepo.create(dto)),
      'Falla creada',
    );
  }
  async updateFallaCatalogo(id: string, dto: UpdateFallaCatalogoDto) {
    const item = await this.findOneOrFail(this.fallaRepo, {
      id,
      is_deleted: false,
    });
    Object.assign(item, dto);
    return this.wrap(await this.fallaRepo.save(item), 'Falla actualizada');
  }
  async deleteFallaCatalogo(id: string) {
    const item = await this.findOneOrFail(this.fallaRepo, {
      id,
      is_deleted: false,
    });
    item.is_deleted = true;
    await this.fallaRepo.save(item);
    return this.wrap(true, 'Falla eliminada');
  }

  async listLecturas(equipoId?: string) {
    const where: FindOptionsWhere<LecturaEquipoEntity> = { is_deleted: false };
    if (equipoId) where.equipo_id = equipoId;
    return this.wrap(
      await this.lecturaRepo.find({ where }),
      'Lecturas listadas',
    );
  }
  async getLectura(id: string) {
    return this.wrap(
      await this.findOneOrFail(this.lecturaRepo, { id, is_deleted: false }),
      'Lectura obtenida',
    );
  }
  async createLectura(dto: CreateLecturaEquipoDto) {
    const equipo = await this.findEquipoOrFail(dto.equipo_id);
    const created = await this.lecturaRepo.save(
      this.lecturaRepo.create({
        ...dto,
        fecha: dto.fecha ? new Date(dto.fecha) : new Date(),
      }),
    );
    const tipo = String(dto.tipo || '').toUpperCase();
    if ((tipo.includes('HORO') || tipo.includes('HORA')) && dto.valor != null) {
      equipo.horometro_actual = dto.valor;
      equipo.fecha_ultima_lectura = created.fecha;
      await this.equipoRepo.save(equipo);
      await this.triggerAlertRecalculation('lectura');
    }
    return this.wrap(created, 'Lectura creada');
  }
  async updateLectura(id: string, dto: UpdateLecturaEquipoDto) {
    const item = await this.findOneOrFail(this.lecturaRepo, {
      id,
      is_deleted: false,
    });
    Object.assign(item, {
      ...dto,
      fecha: dto.fecha ? new Date(dto.fecha) : item.fecha,
    });
    const saved = await this.lecturaRepo.save(item);
    const tipo = String(saved.tipo || '').toUpperCase();
    if ((tipo.includes('HORO') || tipo.includes('HORA')) && saved.valor != null) {
      const equipo = await this.findEquipoOrFail(saved.equipo_id);
      equipo.horometro_actual = this.toNumeric(saved.valor);
      equipo.fecha_ultima_lectura = saved.fecha;
      await this.equipoRepo.save(equipo);
      await this.triggerAlertRecalculation('lectura-update');
    }
    return this.wrap(saved, 'Lectura actualizada');
  }
  async deleteLectura(id: string) {
    const item = await this.findOneOrFail(this.lecturaRepo, {
      id,
      is_deleted: false,
    });
    item.is_deleted = true;
    await this.lecturaRepo.save(item);
    return this.wrap(true, 'Lectura eliminada');
  }

  async listLubricaciones(equipoId?: string) {
    const where: FindOptionsWhere<LubricacionPuntoEntity> = {
      is_deleted: false,
    };
    if (equipoId) where.equipo_id = equipoId;
    return this.wrap(
      await this.lubricacionRepo.find({ where }),
      'Puntos de lubricación listados',
    );
  }
  async getLubricacion(id: string) {
    return this.wrap(
      await this.findOneOrFail(this.lubricacionRepo, { id, is_deleted: false }),
      'Punto de lubricación obtenido',
    );
  }
  async createLubricacion(dto: CreateLubricacionPuntoDto) {
    await this.findEquipoOrFail(dto.equipo_id);
    if (dto.componente_id)
      await this.findOneOrFail(this.equipoComponenteRepo, {
        id: dto.componente_id,
        is_deleted: false,
      });
    return this.wrap(
      await this.lubricacionRepo.save(this.lubricacionRepo.create(dto)),
      'Punto de lubricación creado',
    );
  }
  async updateLubricacion(id: string, dto: UpdateLubricacionPuntoDto) {
    const item = await this.findOneOrFail(this.lubricacionRepo, {
      id,
      is_deleted: false,
    });
    Object.assign(item, dto);
    return this.wrap(
      await this.lubricacionRepo.save(item),
      'Punto de lubricación actualizado',
    );
  }
  async deleteLubricacion(id: string) {
    const item = await this.findOneOrFail(this.lubricacionRepo, {
      id,
      is_deleted: false,
    });
    item.is_deleted = true;
    await this.lubricacionRepo.save(item);
    return this.wrap(true, 'Punto de lubricación eliminado');
  }

  async listAlertas(q: AlertaQueryDto) {
    const where: FindOptionsWhere<AlertaMantenimientoEntity> = {
      is_deleted: false,
    };
    if (q.estado) where.estado = q.estado;
    if (q.tipo_alerta) where.tipo_alerta = q.tipo_alerta;
    if (q.equipo_id) where.equipo_id = q.equipo_id;
    return this.wrap(await this.alertaRepo.find({ where }), 'Alertas listadas');
  }

  async recalculateAlertas() {
    let upserts = 0;
    let skipped = 0;
    let offset = 0;
    let omittedErrors = 0;
    const errors: string[] = [];

    while (true) {
      const programaciones = await this.programacionRepo.find({
        where: { is_deleted: false, activo: true },
        skip: offset,
        take: this.RECALCULATION_BATCH_SIZE,
      });
      if (!programaciones.length) break;

      const batchStats =
        await this.processProgramacionesInWorkers(programaciones);
      upserts += batchStats.upserts;
      skipped += batchStats.skipped;

      for (const error of batchStats.errors) {
        if (errors.length < this.MAX_STORED_ERRORS) errors.push(error);
        else omittedErrors++;
      }

      offset += programaciones.length;
    }

    return this.wrap(
      { total: upserts, skipped, errors, omitted_errors: omittedErrors },
      'Alertas recalculadas',
    );
  }

  private async processProgramacionesInWorkers(
    programaciones: ProgramacionPlanEntity[],
  ) {
    let cursor = 0;
    let upserts = 0;
    let skipped = 0;
    const errors: string[] = [];
    const workers = Math.max(
      1,
      Math.min(this.RECALCULATION_WORKERS, programaciones.length),
    );

    const runWorker = async () => {
      while (cursor < programaciones.length) {
        const index = cursor;
        cursor++;
        const outcome = await this.processProgramacion(programaciones[index]);
        if (outcome.upserted) upserts++;
        if (outcome.skipped) skipped++;
        if (outcome.error) errors.push(outcome.error);
      }
    };

    await Promise.all(Array.from({ length: workers }, () => runWorker()));
    return { upserts, skipped, errors };
  }

  private async processProgramacion(prog: ProgramacionPlanEntity) {
    try {
      if (!prog.plan_id || !prog.equipo_id) {
        return {
          upserted: false,
          skipped: true,
          error: `Programación inválida sin plan/equipo: ${prog.id ?? 'sin-id'}`,
        };
      }

      const equipo = await this.equipoRepo.findOne({
        where: { id: prog.equipo_id, is_deleted: false },
      });
      if (!equipo) {
        return {
          upserted: false,
          skipped: true,
          error: `Equipo no encontrado para programación ${prog.id ?? prog.plan_id}`,
        };
      }

      const h = Number(equipo.horometro_actual ?? 0);
      if (!Number.isFinite(h)) {
        return {
          upserted: false,
          skipped: true,
          error: `Horómetro inválido en equipo ${equipo.id}`,
        };
      }

      const pHRaw = prog.proxima_horas;
      const pH = pHRaw !== null && pHRaw !== undefined ? Number(pHRaw) : null;
      if (pHRaw !== null && pHRaw !== undefined && !Number.isFinite(pH)) {
        return {
          upserted: false,
          skipped: true,
          error: `proxima_horas inválida en programación ${prog.id ?? prog.plan_id}`,
        };
      }

      const pF = prog.proxima_fecha ? new Date(prog.proxima_fecha) : null;
      if (pF && Number.isNaN(pF.getTime())) {
        return {
          upserted: false,
          skipped: true,
          error: `proxima_fecha inválida en programación ${prog.id ?? prog.plan_id}`,
        };
      }

      const today = new Date();
      let tipo: string | null = null;
      let detalle = `Recalculada plan ${prog.plan_id}`;

      if (pH !== null && h >= pH) tipo = 'OVERDUE';
      else if (pF && today > pF) tipo = 'OVERDUE';
      else if (pH !== null) {
        const diff = pH - h;
        if (diff <= 325) tipo = 'MPG_325';
        else if (diff <= 650) tipo = 'MPG_650';
        else if (diff <= 975) tipo = 'MPG_975';
        else tipo = 'MPG_1300';
      } else if (pF) {
        tipo = 'MPG_1300';
      }

      if (!tipo) {
        return {
          upserted: false,
          skipped: true,
          error: `Sin criterio de alerta para programación ${prog.id ?? prog.plan_id}`,
        };
      }

      if (pH !== null && h < pH && tipo === 'MPG_1300') {
        detalle = `Programación activa fuera de umbrales para plan ${prog.plan_id}`;
      }

      const reference = `PLAN:${prog.plan_id}`;
      const existing = await this.alertaRepo.findOne({
        where: {
          equipo_id: prog.equipo_id,
          tipo_alerta: tipo,
          referencia: reference,
          estado: 'ABIERTA',
          is_deleted: false,
        },
      });
      if (!existing) {
        await this.alertaRepo.save(
          this.alertaRepo.create({
            equipo_id: prog.equipo_id,
            tipo_alerta: tipo,
            referencia: reference,
            detalle,
          }),
        );
        return { upserted: true, skipped: false, error: null as string | null };
      }

      return { upserted: false, skipped: false, error: null as string | null };
    } catch (e: any) {
      return {
        upserted: false,
        skipped: true,
        error: `Error procesando programación ${prog.id ?? prog.plan_id}: ${e?.message ?? 'desconocido'}`,
      };
    }
  }

  async listWorkOrders(q: WorkOrderQueryDto) {
    const qb = this.woRepo
      .createQueryBuilder('wo')
      .where('wo.is_deleted = false');
    if (q.equipo_id)
      qb.andWhere('wo.equipment_id = :equipo', { equipo: q.equipo_id });
    if (q.status_workflow)
      qb.andWhere('UPPER(wo.status_workflow) = :estado', {
        estado: this.normalizeWorkflowStatus(q.status_workflow),
      });
    if (q.maintenance_kind)
      qb.andWhere('wo.maintenance_kind = :kind', { kind: q.maintenance_kind });
    qb.orderBy('wo.created_at', 'DESC');
    const rows = await qb.getMany();
    return this.wrap(await Promise.all(rows.map((row) => this.enrichWorkOrder(row))), 'Work orders listadas');
  }

  async getWorkOrder(id: string) {
    const row = await this.findOneOrFail(this.woRepo, { id, is_deleted: false });
    return this.wrap(await this.enrichWorkOrder(row), 'Work order obtenida');
  }

  async createWorkOrder(dto: CreateWorkOrderDto) {
    if (dto.equipment_id) await this.findEquipoOrFail(dto.equipment_id);
    if (dto.plan_id)
      await this.findOneOrFail(this.planRepo, {
        id: dto.plan_id,
        is_deleted: false,
      });
    const normalizedStatus = this.normalizeWorkflowStatus(dto.status_workflow ?? 'PLANNED');
    const entity = this.woRepo.create({
      code: dto.code,
      type: dto.type,
      equipment_id: dto.equipment_id ?? null,
      plan_id: dto.plan_id ?? null,
      valor_json: dto.valor_json ?? null,
      title: dto.title,
      description: dto.description ?? null,
      status_workflow: normalizedStatus,
      priority: dto.priority ?? 5,
      provider_type: dto.provider_type ?? 'INTERNO',
      maintenance_kind: dto.maintenance_kind ?? 'CORRECTIVO',
      safety_permit_required: dto.safety_permit_required ?? false,
      safety_permit_code: dto.safety_permit_code ?? null,
      vendor_id: dto.vendor_id ?? null,
      purchase_request_id: dto.purchase_request_id ?? null,
    });
    this.applyWorkflowDates(entity, null, normalizedStatus);
    const created = await this.woRepo.save(entity);
    await this.appendWorkOrderHistory(created.id, normalizedStatus, 'Orden de trabajo creada');
    if (dto.alerta_id) {
      const alerta = await this.findOneOrFail(this.alertaRepo, {
        id: dto.alerta_id,
        is_deleted: false,
      });
      alerta.work_order_id = created.id;
      alerta.estado = normalizedStatus === 'CLOSED' ? 'CERRADA' : 'EN_PROCESO';
      await this.alertaRepo.save(alerta);
    }
    const enriched = await this.enrichWorkOrder(created);
    await this.publishInAppNotification({
      title: 'Nueva orden de trabajo',
      body: `${enriched.code} - ${enriched.title}`,
      module: 'maintenance',
      entityType: 'work-order',
      entityId: created.id,
      level: normalizedStatus === 'CLOSED' ? 'success' : 'info',
    });
    await this.writeSecurityLog({
      description: `[WO:${created.id}] Creación de OT ${created.code}`,
      typeLog: 'WORK_ORDER',
    });
    return this.wrap(enriched, 'Work order creada');
  }

  async updateWorkOrder(id: string, dto: UpdateWorkOrderDto) {
    const wo = await this.findOneOrFail(this.woRepo, { id, is_deleted: false });
    const previousStatus = this.normalizeWorkflowStatus(wo.status_workflow);
    Object.assign(wo, dto);
    wo.status_workflow = this.normalizeWorkflowStatus(dto.status_workflow ?? wo.status_workflow);
    this.applyWorkflowDates(wo, previousStatus, wo.status_workflow);
    const saved = await this.woRepo.save(wo);
    if (previousStatus !== saved.status_workflow) {
      await this.appendWorkOrderHistory(saved.id, saved.status_workflow, `Cambio de estado ${previousStatus} → ${saved.status_workflow}`, { fromStatus: previousStatus });
    } else {
      await this.appendWorkOrderHistory(saved.id, saved.status_workflow, 'Cabecera de OT actualizada', { fromStatus: previousStatus });
    }
    const alertas = await this.alertaRepo.find({ where: { work_order_id: saved.id, is_deleted: false } });
    if (alertas.length) {
      const nextAlertStatus = saved.status_workflow === 'CLOSED' ? 'CERRADA' : 'EN_PROCESO';
      for (const alerta of alertas) alerta.estado = nextAlertStatus;
      await this.alertaRepo.save(alertas);
    }
    const enriched = await this.enrichWorkOrder(saved);
    await this.publishInAppNotification({
      title: previousStatus !== saved.status_workflow ? 'Estado de OT actualizado' : 'Orden de trabajo actualizada',
      body: `${enriched.code} - ${enriched.title} (${saved.status_workflow})`,
      module: 'maintenance',
      entityType: 'work-order',
      entityId: saved.id,
      level: saved.status_workflow === 'CLOSED' ? 'success' : 'info',
    });
    await this.writeSecurityLog({
      description: `[WO:${saved.id}] Actualización de OT ${saved.code} (${previousStatus} -> ${saved.status_workflow})`,
      typeLog: 'WORK_ORDER',
    });
    return this.wrap(enriched, 'Work order actualizada');
  }

  async deleteWorkOrder(id: string) {
    const wo = await this.findOneOrFail(this.woRepo, { id, is_deleted: false });
    wo.is_deleted = true;
    await this.woRepo.save(wo);
    await this.appendWorkOrderHistory(wo.id, this.normalizeWorkflowStatus(wo.status_workflow), 'Orden de trabajo eliminada lógicamente', { fromStatus: wo.status_workflow });
    await this.writeSecurityLog({
      description: `[WO:${wo.id}] Eliminación lógica de OT ${wo.code}`,
      typeLog: 'WORK_ORDER',
    });
    return this.wrap(true, 'Work order eliminada');
  }

  async listWorkOrderTareas(workOrderId: string) {
    await this.findOneOrFail(this.woRepo, {
      id: workOrderId,
      is_deleted: false,
    });
    return this.wrap(
      await this.woTareaRepo.find({
        where: { work_order_id: workOrderId, is_deleted: false },
      }),
      'Tareas de OT listadas',
    );
  }

  async createWorkOrderTarea(
    workOrderId: string,
    dto: CreateWorkOrderTareaDto,
  ) {
    const workOrder = await this.findOneOrFail(this.woRepo, {
      id: workOrderId,
      is_deleted: false,
    });
    await this.findOneOrFail(this.planRepo, {
      id: dto.plan_id,
      is_deleted: false,
    });
    await this.findOneOrFail(this.planTareaRepo, {
      id: dto.tarea_id,
      plan_id: dto.plan_id,
      is_deleted: false,
    });
    const created = await this.woTareaRepo.save(
      this.woTareaRepo.create({ ...dto, work_order_id: workOrderId }),
    );
    await this.appendWorkOrderHistory(workOrderId, this.normalizeWorkflowStatus(workOrder.status_workflow), `Tarea registrada: ${dto.tarea_id}`, { fromStatus: workOrder.status_workflow });
    return this.wrap(created, 'Tarea de OT creada');
  }

  async updateWorkOrderTarea(id: string, dto: UpdateWorkOrderTareaDto) {
    const tarea = await this.findOneOrFail(this.woTareaRepo, {
      id,
      is_deleted: false,
    });
    Object.assign(tarea, dto);
    const saved = await this.woTareaRepo.save(tarea);
    const workOrder = await this.findOneOrFail(this.woRepo, { id: tarea.work_order_id, is_deleted: false });
    await this.appendWorkOrderHistory(tarea.work_order_id, this.normalizeWorkflowStatus(workOrder.status_workflow), `Tarea actualizada: ${tarea.tarea_id}`, { fromStatus: workOrder.status_workflow });
    return this.wrap(
      saved,
      'Tarea de OT actualizada',
    );
  }

  async deleteWorkOrderTarea(id: string) {
    const tarea = await this.findOneOrFail(this.woTareaRepo, {
      id,
      is_deleted: false,
    });
    tarea.is_deleted = true;
    await this.woTareaRepo.save(tarea);
    const workOrder = await this.findOneOrFail(this.woRepo, { id: tarea.work_order_id, is_deleted: false });
    await this.appendWorkOrderHistory(tarea.work_order_id, this.normalizeWorkflowStatus(workOrder.status_workflow), `Tarea eliminada: ${tarea.tarea_id}`, { fromStatus: workOrder.status_workflow });
    return this.wrap(true, 'Tarea de OT eliminada');
  }

  async uploadWorkOrderAdjunto(
    workOrderId: string,
    dto: UploadWorkOrderAdjuntoDto,
  ) {
    const workOrder = await this.findOneOrFail(this.woRepo, {
      id: workOrderId,
      is_deleted: false,
    });
    let buffer: Buffer;
    try {
      buffer = Buffer.from(dto.contenido_base64, 'base64');
    } catch {
      throw new BadRequestException('contenido_base64 inválido');
    }
    if (!buffer.length) throw new BadRequestException('Archivo vacío');
    const folder = join(this.uploadRoot, workOrderId);
    await mkdir(folder, { recursive: true });
    const originalName = basename(dto.nombre);
    const filePath = join(folder, originalName);
    await writeFile(filePath, buffer);
    const hash = createHash('sha256').update(buffer).digest('hex');
    const created = await this.woAdjuntoRepo.save(
      this.woAdjuntoRepo.create({
        work_order_id: workOrderId,
        tipo: dto.tipo ?? 'EVIDENCIA',
        nombre: originalName,
        url: filePath,
        hash_sha256: hash,
        meta: {
          ...(dto.meta ?? {}),
          mime_type: dto.mime_type ?? null,
          extension: extname(originalName) || null,
          size_bytes: buffer.length,
        },
      }),
    );
    await this.appendWorkOrderHistory(workOrderId, this.normalizeWorkflowStatus(workOrder.status_workflow), `Adjunto agregado: ${originalName}`, { fromStatus: workOrder.status_workflow });
    await this.writeSecurityLog({
      description: `[WO:${workOrderId}] Adjunto agregado ${originalName}`,
      typeLog: 'ADJUNTO',
    });
    return this.wrap(
      {
        ...created,
        view_url: this.buildMaintenancePublicUrl(`/work-orders/${workOrderId}/adjuntos/${created.id}/view`),
      },
      'Adjunto cargado',
    );
  }

  async listWorkOrderAdjuntos(
    workOrderId: string,
    query: WorkOrderAdjuntoQueryDto,
  ) {
    await this.findOneOrFail(this.woRepo, {
      id: workOrderId,
      is_deleted: false,
    });
    const where: FindOptionsWhere<WorkOrderAdjuntoEntity> = {
      work_order_id: workOrderId,
      is_deleted: false,
    };
    if (query.tipo) where.tipo = query.tipo;
    const rows = await this.woAdjuntoRepo.find({ where });
    return this.wrap(
      rows.map((row) => ({
        ...row,
        view_url: this.buildMaintenancePublicUrl(`/work-orders/${workOrderId}/adjuntos/${row.id}/view`),
      })),
      'Adjuntos listados',
    );
  }

  async resolveWorkOrderAdjuntoFile(workOrderId: string, adjuntoId: string) {
    const adjunto = await this.findOneOrFail(this.woAdjuntoRepo, {
      id: adjuntoId,
      work_order_id: workOrderId,
      is_deleted: false,
    });
    const meta = (adjunto.meta ?? {}) as Record<string, unknown>;
    const mimeType =
      typeof meta.mime_type === 'string'
        ? meta.mime_type
        : 'application/octet-stream';
    return {
      filePath: adjunto.url,
      fileName: adjunto.nombre || `adjunto-${adjunto.id}`,
      mimeType,
      meta,
    };
  }

  async getWorkOrderAdjunto(workOrderId: string, adjuntoId: string) {
    const adjunto = await this.findOneOrFail(this.woAdjuntoRepo, {
      id: adjuntoId,
      work_order_id: workOrderId,
      is_deleted: false,
    });
    const file = await this.resolveWorkOrderAdjuntoFile(workOrderId, adjuntoId);
    return this.wrap(
      {
        id: adjunto.id,
        work_order_id: adjunto.work_order_id,
        tipo: adjunto.tipo,
        nombre: adjunto.nombre,
        hash_sha256: adjunto.hash_sha256,
        content_type: file.mimeType,
        view_url: this.buildMaintenancePublicUrl(`/work-orders/${workOrderId}/adjuntos/${adjuntoId}/view`),
        meta: adjunto.meta,
      },
      'Adjunto obtenido',
    );
  }

  async deleteWorkOrderAdjunto(workOrderId: string, adjuntoId: string) {
    const workOrder = await this.findOneOrFail(this.woRepo, { id: workOrderId, is_deleted: false });
    const adjunto = await this.findOneOrFail(this.woAdjuntoRepo, {
      id: adjuntoId,
      work_order_id: workOrderId,
      is_deleted: false,
    });
    adjunto.is_deleted = true;
    await this.woAdjuntoRepo.save(adjunto);
    try {
      await unlink(adjunto.url);
    } catch {
      /* ignore */
    }
    await this.appendWorkOrderHistory(workOrderId, this.normalizeWorkflowStatus(workOrder.status_workflow), `Adjunto eliminado: ${adjunto.nombre ?? adjunto.id}`, { fromStatus: workOrder.status_workflow });
    await this.writeSecurityLog({
      description: `[WO:${workOrderId}] Adjunto eliminado ${adjunto.id}`,
      typeLog: 'ADJUNTO',
    });
    return this.wrap(true, 'Adjunto eliminado');
  }

  async listConsumos(workOrderId: string) {
    await this.findOneOrFail(this.woRepo, { id: workOrderId, is_deleted: false });
    const rows = await this.consumoRepo.find({
      where: { work_order_id: workOrderId, is_deleted: false },
      order: { id: 'DESC' },
    });
    const { productMap, warehouseMap } = await this.buildInventoryCatalogMaps(
      rows.map((row) => row.producto_id),
      rows.map((row) => row.bodega_id || '').filter(Boolean),
    );
    return this.wrap(
      rows.map((row) => this.mapConsumoWithCatalogs(row, productMap, warehouseMap)),
      'Consumos listados',
    );
  }

  async listWorkOrderHistory(workOrderId: string) {
    await this.findOneOrFail(this.woRepo, { id: workOrderId, is_deleted: false });
    const rows = await this.woHistoryRepo.find({
      where: { work_order_id: workOrderId },
      order: { changed_at: 'DESC' },
    });
    return this.wrap(rows, 'Historial de la OT listado');
  }

  async createConsumo(workOrderId: string, dto: CreateConsumoDto) {
    const workOrder = await this.findOneOrFail(this.woRepo, {
      id: workOrderId,
      is_deleted: false,
    });
    if (!dto.bodega_id) {
      throw new BadRequestException('La bodega es obligatoria para registrar el consumo.');
    }

    const { producto, bodega } = await this.validateProductoEnBodega(dto.producto_id, dto.bodega_id);
    const subtotal = dto.cantidad * dto.costo_unitario;
    const saved = await this.consumoRepo.save(
      this.consumoRepo.create({
        ...dto,
        work_order_id: workOrderId,
        subtotal,
      }),
    );
    await this.appendWorkOrderHistory(workOrderId, this.normalizeWorkflowStatus(workOrder.status_workflow), `Consumo registrado para producto ${dto.producto_id} por ${dto.cantidad}`, { fromStatus: workOrder.status_workflow });
    await this.writeSecurityLog({
      description: `[WO:${workOrderId}] Consumo registrado producto ${dto.producto_id} cantidad ${dto.cantidad}`,
      typeLog: 'CONSUMO',
    });
    return this.wrap(
      this.mapConsumoWithCatalogs(saved, new Map([[producto.id, producto]]), new Map([[bodega.id, bodega]])),
      'Consumo registrado',
    );
  }

  async listIssueMaterials(workOrderId: string) {
    await this.findOneOrFail(this.woRepo, { id: workOrderId, is_deleted: false });
    const entregas = await this.dataSource.getRepository(EntregaMaterialEntity).find({
      where: { work_order_id: workOrderId, is_deleted: false },
      order: { fecha: 'DESC' },
    });
    const entregaIds = entregas.map((item) => item.id);
    const detalles = entregaIds.length
      ? await this.dataSource.getRepository(EntregaMaterialDetEntity).find({ where: { entrega_id: In(entregaIds) } })
      : [];

    const { productMap, warehouseMap } = await this.buildInventoryCatalogMaps(
      detalles.map((item) => item.producto_id),
      detalles.map((item) => item.bodega_id),
    );

    return this.wrap(
      entregas.map((entrega) => {
        const items = detalles
          .filter((detalle) => detalle.entrega_id === entrega.id)
          .map((detalle) => this.mapIssueItemWithCatalogs(detalle, productMap, warehouseMap));
        return {
          ...entrega,
          items,
          total: items.reduce(
            (acc, item) => acc + Number(item.costo_unitario || 0) * Number(item.cantidad || 0),
            0,
          ),
        };
      }),
      'Salidas de materiales listadas',
    );
  }

  async issueMaterials(workOrderId: string, dto: IssueMaterialsDto) {
    const qr = this.dataSource.createQueryRunner();
    await qr.connect();
    await qr.startTransaction();
    try {
      const workOrder = await this.findOneOrFail(this.woRepo, {
        id: workOrderId,
        is_deleted: false,
      });
      const em = await qr.manager.save(
        EntregaMaterialEntity,
        qr.manager.create(EntregaMaterialEntity, {
          work_order_id: workOrderId,
          code: `EM-${Date.now()}`,
          observacion: dto.observacion,
        }),
      );
      const mov = await qr.manager.save(
        MovimientoInventarioEntity,
        qr.manager.create(MovimientoInventarioEntity, {
          tipo_movimiento: 'SALIDA',
          work_order_id: workOrderId,
          total_costos: 0,
        }),
      );
      let total = 0;
      for (const item of dto.items) {
        const reserva = await qr.manager.findOne(ReservaStockEntity, {
          where: {
            work_order_id: workOrderId,
            producto_id: item.producto_id,
            bodega_id: item.bodega_id,
            estado: 'RESERVADO',
            is_deleted: false,
          },
        });
        if (!reserva || Number(reserva.cantidad) < item.cantidad)
          throw new ConflictException('Reserva insuficiente');
        const stock = await qr.manager.findOne(StockBodegaEntity, {
          where: { producto_id: item.producto_id, bodega_id: item.bodega_id },
        });
        if (!stock || Number(stock.stock_actual) < item.cantidad)
          throw new ConflictException('Stock insuficiente');
        const producto = await qr.manager.findOne(ProductoEntity, {
          where: { id: item.producto_id },
        });
        if (!producto) throw new NotFoundException('Producto no encontrado');
        const costo = Number(producto.ultimo_costo);
        const subtotal = item.cantidad * costo;
        total += subtotal;
        stock.stock_actual = Number(stock.stock_actual) - item.cantidad;
        await qr.manager.save(stock);
        reserva.estado = 'CONSUMIDO';
        await qr.manager.save(reserva);
        await qr.manager.save(
          EntregaMaterialDetEntity,
          qr.manager.create(EntregaMaterialDetEntity, {
            entrega_id: em.id,
            producto_id: item.producto_id,
            bodega_id: item.bodega_id,
            cantidad: item.cantidad,
            costo_unitario: costo,
          }),
        );
        const movDet = await qr.manager.save(
          MovimientoInventarioDetEntity,
          qr.manager.create(MovimientoInventarioDetEntity, {
            movimiento_id: mov.id,
            producto_id: item.producto_id,
            cantidad: item.cantidad,
            costo_unitario: costo,
            subtotal_costo: subtotal,
          }),
        );
        await qr.manager.save(
          KardexEntity,
          qr.manager.create(KardexEntity, {
            bodega_id: item.bodega_id,
            producto_id: item.producto_id,
            movimiento_id: mov.id,
            movimiento_det_id: movDet.id,
            tipo_movimiento: 'SALIDA',
            salida_cantidad: item.cantidad,
            costo_unitario: costo,
            costo_total: subtotal,
            saldo_cantidad: stock.stock_actual,
            saldo_costo_promedio: costo,
            saldo_valorizado: Number(stock.stock_actual) * costo,
          }),
        );
        await qr.manager.save(
          ConsumoRepuestoEntity,
          qr.manager.create(ConsumoRepuestoEntity, {
            work_order_id: workOrderId,
            producto_id: item.producto_id,
            bodega_id: item.bodega_id,
            cantidad: item.cantidad,
            costo_unitario: costo,
            subtotal,
          }),
        );
      }
      mov.total_costos = total;
      await qr.manager.save(mov);
      await qr.commitTransaction();
      await this.appendWorkOrderHistory(workOrderId, this.normalizeWorkflowStatus(workOrder.status_workflow), `Salida de materiales registrada (${dto.items.length} items)`, { fromStatus: workOrder.status_workflow });
      await this.writeSecurityLog({
        description: `[WO:${workOrderId}] Emisión de materiales por total ${total}`,
        typeLog: 'MATERIALES',
      });
      return this.wrap(
        { entrega_id: em.id, movimiento_id: mov.id, total },
        'Materiales entregados',
      );
    } catch (e) {
      await qr.rollbackTransaction();
      throw e;
    } finally {
      await qr.release();
    }
  }

  private async findEquipoOrFail(id: string) {
    return this.findOneOrFail(this.equipoRepo, { id, is_deleted: false });
  }
  private async findOneOrFail<T extends ObjectLiteral>(
    repo: Repository<T>,
    where: FindOptionsWhere<T>,
  ) {
    const item = await repo.findOne({ where });
    if (!item) throw new NotFoundException('Registro no encontrado');
    return item;
  }

  async seedEstadosCatalogo() {
    const codigos = ['OPERATIVO', 'RESERVA', 'MPG', 'CORRECTIVO', 'BLOQUEADA'];
    for (const codigo of codigos) {
      const existing = await this.estadoCatalogoRepo.findOne({
        where: { codigo, is_deleted: false },
      });
      if (!existing)
        await this.estadoCatalogoRepo.save(
          this.estadoCatalogoRepo.create({ codigo, descripcion: codigo }),
        );
    }
  }
}
