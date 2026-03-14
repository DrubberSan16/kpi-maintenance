import { BadRequestException, ConflictException, Inject, Injectable, NotFoundException } from '@nestjs/common';
import { createHash } from 'crypto';
import { mkdir, readFile, unlink, writeFile } from 'fs/promises';
import { basename, extname, join } from 'path';
import { InjectDataSource, InjectRepository } from '@nestjs/typeorm';
import { DataSource, FindOptionsWhere, In, IsNull, ObjectLiteral, Repository } from 'typeorm';
import { AlertaMantenimientoEntity, BitacoraDiariaEntity, ConsumoRepuestoEntity, EntregaMaterialDetEntity, EntregaMaterialEntity, EquipoEntity, EquipoTipoEntity, EstadoEquipoCatalogoEntity, EstadoEquipoEntity, EventoEquipoEntity, KardexEntity, LocationEntity, MovimientoInventarioDetEntity, MovimientoInventarioEntity, PlanMantenimientoEntity, PlanTareaEntity, ProductoEntity, ProgramacionPlanEntity, ReservaStockEntity, StockBodegaEntity, WorkOrderAdjuntoEntity, WorkOrderEntity, WorkOrderTareaEntity } from '../entities/kpi-maintenance.entity';
import { AlertaQueryDto, ChangeEstadoDto, CreateBitacoraDto, CreateConsumoDto, CreateEquipoDto, CreateEquipoTipoDto, CreateEventoDto, CreatePlanDto, CreatePlanTareaDto, CreateProgramacionDto, CreateWorkOrderDto, CreateWorkOrderTareaDto, DateRangeDto, EquipoQueryDto, IssueMaterialsDto, UpdateBitacoraDto, UpdateEquipoDto, UpdateEquipoTipoDto, UpdatePlanDto, UpdatePlanTareaDto, UpdateProgramacionDto, UpdateWorkOrderDto, UpdateWorkOrderTareaDto, UploadWorkOrderAdjuntoDto, WorkOrderAdjuntoQueryDto, WorkOrderQueryDto } from '../dto';

@Injectable()
export class KpiMaintenanceService {
  constructor(
    @InjectRepository(EquipoEntity) private readonly equipoRepo: Repository<EquipoEntity>,
    @InjectRepository(EquipoTipoEntity) private readonly equipoTipoRepo: Repository<EquipoTipoEntity>,
    @InjectRepository(LocationEntity) private readonly locationRepo: Repository<LocationEntity>,
    @InjectRepository(BitacoraDiariaEntity) private readonly bitacoraRepo: Repository<BitacoraDiariaEntity>,
    @InjectRepository(AlertaMantenimientoEntity) private readonly alertaRepo: Repository<AlertaMantenimientoEntity>,
    @InjectRepository(EstadoEquipoEntity) private readonly estadoRepo: Repository<EstadoEquipoEntity>,
    @InjectRepository(EstadoEquipoCatalogoEntity) private readonly estadoCatalogoRepo: Repository<EstadoEquipoCatalogoEntity>,
    @InjectRepository(EventoEquipoEntity) private readonly eventoRepo: Repository<EventoEquipoEntity>,
    @InjectRepository(PlanMantenimientoEntity) private readonly planRepo: Repository<PlanMantenimientoEntity>,
    @InjectRepository(PlanTareaEntity) private readonly planTareaRepo: Repository<PlanTareaEntity>,
    @InjectRepository(ProgramacionPlanEntity) private readonly programacionRepo: Repository<ProgramacionPlanEntity>,
    @InjectRepository(WorkOrderEntity) private readonly woRepo: Repository<WorkOrderEntity>,
    @InjectRepository(ConsumoRepuestoEntity) private readonly consumoRepo: Repository<ConsumoRepuestoEntity>,
    @InjectRepository(StockBodegaEntity) private readonly stockRepo: Repository<StockBodegaEntity>,
    @InjectRepository(ProductoEntity) private readonly productoRepo: Repository<ProductoEntity>,
    @InjectRepository(ReservaStockEntity) private readonly reservaRepo: Repository<ReservaStockEntity>,
    @InjectRepository(WorkOrderTareaEntity) private readonly woTareaRepo: Repository<WorkOrderTareaEntity>,
    @InjectRepository(WorkOrderAdjuntoEntity) private readonly woAdjuntoRepo: Repository<WorkOrderAdjuntoEntity>,
    @InjectDataSource() private readonly dataSource: DataSource,
  ) { }

  private readonly uploadRoot = process.env.WORK_ORDER_UPLOAD_DIR || join(process.cwd(), 'storage', 'work-orders');

  private wrap(data: unknown, message = 'OK', meta?: unknown) { return { data, meta, message }; }

  async listEquipos(query: EquipoQueryDto) {
    const page = query.page ?? 1; const limit = Math.min(query.limit ?? 10, 100);
    const qb = this.equipoRepo.createQueryBuilder('e').where('e.is_deleted = false');
    if (query.codigo) qb.andWhere('e.codigo ILIKE :codigo', { codigo: `%${query.codigo}%` });
    if (query.marca_id) qb.andWhere('e.marca_id = :marca_id', { marca_id: query.marca_id });
    if (query.location_id) qb.andWhere('e.location_id = :location_id', { location_id: query.location_id });
    if (query.equipo_tipo_id) qb.andWhere('e.equipo_tipo_id = :equipo_tipo_id', { equipo_tipo_id: query.equipo_tipo_id });
    if (query.estado_operativo) qb.andWhere('e.estado_operativo = :estado_operativo', { estado_operativo: query.estado_operativo });
    if (query.criticidad) qb.andWhere('e.criticidad = :criticidad', { criticidad: query.criticidad });
    const [data, total] = await qb.orderBy('e.created_at', 'DESC').skip((page - 1) * limit).take(limit).getManyAndCount();
    return this.wrap(data, 'Equipos listados', { page, limit, total });
  }

  async getEquipo(id: string) { return this.wrap(await this.findEquipoOrFail(id), 'Equipo obtenido'); }
  async createEquipo(dto: CreateEquipoDto) { return this.wrap(await this.equipoRepo.save(this.equipoRepo.create({ ...dto, horometro_actual: dto.horometro_actual ?? 0 })), 'Equipo creado'); }
  async updateEquipo(id: string, dto: UpdateEquipoDto) { const e = await this.findEquipoOrFail(id); Object.assign(e, dto); return this.wrap(await this.equipoRepo.save(e), 'Equipo actualizado'); }
  async deleteEquipo(id: string) { const e = await this.findEquipoOrFail(id); e.is_deleted = true; e.deleted_at = new Date(); await this.equipoRepo.save(e); return this.wrap(true, 'Equipo eliminado'); }

  async listEquipoTipos() {
    return this.wrap(await this.equipoTipoRepo.manager.find(EquipoTipoEntity, {
      where: { is_deleted: false }
    }), 'Tipos de equipo listados');
  }
  async createEquipoTipo(dto: CreateEquipoTipoDto) {
    return this.wrap(await this.equipoTipoRepo.manager.save(EquipoTipoEntity, this.equipoTipoRepo.manager.create(EquipoTipoEntity, dto)), 'Tipo de equipo creado');
  }
  async updateEquipoTipo(id: string, dto: UpdateEquipoTipoDto) {
    const t = await this.findOneOrFail(this.equipoTipoRepo.manager.getRepository(EquipoTipoEntity), {
      id, is_deleted: false
    }); Object.assign(t, dto); return this.wrap(await this.equipoTipoRepo.manager.save(EquipoTipoEntity, t), 'Tipo de equipo actualizado');
  }
  async deleteEquipoTipo(id: string) {
    const t = await this.findOneOrFail(this.equipoTipoRepo.manager.getRepository(EquipoTipoEntity), {
      id, is_deleted: false
    }); t.is_deleted = true; t.deleted_at = new Date(); await this.equipoTipoRepo.manager.save(EquipoTipoEntity, t); return this.wrap(true, 'Tipo de equipo eliminado');
  }

  async listLocations() {
    return this.wrap(await this.locationRepo.manager.find(LocationEntity, {
      where: { is_deleted: false }
    }), 'Locations listados');
  }

  async getLocation(id: string) {
    return this.wrap(await this.findOneOrFail(this.locationRepo.manager.getRepository(LocationEntity), {
      id, is_deleted: false
    }), 'Location obtenida');
  }

  async createLocation(dto: CreateEquipoTipoDto) {
    return this.wrap(await this.locationRepo.manager.save(LocationEntity, this.locationRepo.manager.create(LocationEntity, dto)), 'Location creada');
  }

  async updateLocation(id: string, dto: UpdateEquipoTipoDto) {
    const l = await this.findOneOrFail(this.locationRepo.manager.getRepository(LocationEntity), {
      id, is_deleted: false
    }); Object.assign(l, dto); return this.wrap(await this.locationRepo.manager.save(LocationEntity, l), 'Location actualizada');
  }

  async deleteLocation(id: string) {
    const l = await this.findOneOrFail(this.locationRepo.manager.getRepository(LocationEntity), {
      id, is_deleted: false
    }); l.is_deleted = true; l.deleted_at = new Date(); await this.locationRepo.manager.save(LocationEntity, l); return this.wrap(true, 'Location eliminada');
  }

  async listBitacora(equipoId: string, range: DateRangeDto) {
    await this.findEquipoOrFail(equipoId);
    const qb = this.bitacoraRepo.createQueryBuilder('b').where('b.equipo_id = :equipoId and b.is_deleted = false', { equipoId });
    if (range.from) qb.andWhere('b.fecha >= :from', { from: range.from });
    if (range.to) qb.andWhere('b.fecha <= :to', { to: range.to });
    return this.wrap(await qb.orderBy('b.fecha', 'DESC').getMany(), 'Bitácora listada');
  }

  async createBitacora(equipoId: string, dto: CreateBitacoraDto) {
    const equipo = await this.findEquipoOrFail(equipoId);
    const last = await this.bitacoraRepo.findOne({ where: { equipo_id: equipoId, is_deleted: false }, order: { fecha: 'DESC', created_at: 'DESC' } });
    if (last && Number(dto.horometro) < Number(last.horometro)) {
      await this.alertaRepo.save(this.alertaRepo.create({ equipo_id: equipoId, tipo_alerta: 'ANOMALIA_HOROMETRO', detalle: `Horómetro ${dto.horometro} menor al último ${last.horometro}` }));
      throw new ConflictException('El horómetro no puede retroceder');
    }
    const saved = await this.bitacoraRepo.save(this.bitacoraRepo.create({ ...dto, equipo_id: equipoId }));
    equipo.horometro_actual = dto.horometro;
    equipo.fecha_ultima_lectura = new Date(dto.fecha);
    await this.equipoRepo.save(equipo);
    return this.wrap(saved, 'Bitácora creada');
  }

  async updateBitacora(id: string, dto: UpdateBitacoraDto) { const b = await this.findOneOrFail(this.bitacoraRepo, { id, is_deleted: false }); Object.assign(b, dto); return this.wrap(await this.bitacoraRepo.save(b), 'Bitácora actualizada'); }
  async deleteBitacora(id: string) { const b = await this.findOneOrFail(this.bitacoraRepo, { id, is_deleted: false }); b.is_deleted = true; await this.bitacoraRepo.save(b); return this.wrap(true, 'Bitácora eliminada'); }

  async changeEstado(equipoId: string, dto: ChangeEstadoDto) {
    const equipo = await this.findEquipoOrFail(equipoId);
    const estadoCatalogo = await this.findOneOrFail(this.estadoCatalogoRepo, { id: dto.estado_id, is_deleted: false });
    const current = await this.estadoRepo.findOne({ where: { equipo_id: equipoId, fecha_fin: IsNull(), is_deleted: false }, order: { fecha_inicio: 'DESC' } });
    if (current) { current.fecha_fin = new Date(dto.fecha_inicio); await this.estadoRepo.save(current); }
    const nuevo = await this.estadoRepo.save(this.estadoRepo.create({ equipo_id: equipoId, estado_id: dto.estado_id, fecha_inicio: new Date(dto.fecha_inicio), motivo: dto.motivo }));
    equipo.estado_operativo = estadoCatalogo.codigo;
    await this.equipoRepo.save(equipo);
    return this.wrap(nuevo, 'Estado actualizado');
  }

  async listEstados(equipoId: string, range: DateRangeDto) {
    const qb = this.estadoRepo.createQueryBuilder('e').where('e.equipo_id = :equipoId and e.is_deleted = false', { equipoId });
    if (range.from) qb.andWhere('e.fecha_inicio >= :from', { from: range.from });
    if (range.to) qb.andWhere('e.fecha_inicio <= :to', { to: range.to });
    return this.wrap(await qb.orderBy('e.fecha_inicio', 'DESC').getMany(), 'Estados listados');
  }

  createEvento(equipoId: string, dto: CreateEventoDto) { return this.wrap(this.eventoRepo.save(this.eventoRepo.create({ ...dto, equipo_id: equipoId })), 'Evento creado'); }
  async listEventos(equipoId: string, range: DateRangeDto & { tipo_evento?: string }) {
    const qb = this.eventoRepo.createQueryBuilder('e').where('e.equipo_id = :equipoId and e.is_deleted = false', { equipoId });
    if (range.from) qb.andWhere('e.fecha_inicio >= :from', { from: range.from }); if (range.to) qb.andWhere('e.fecha_inicio <= :to', { to: range.to }); if (range.tipo_evento) qb.andWhere('e.tipo_evento = :tipo', { tipo: range.tipo_evento });
    return this.wrap(await qb.getMany(), 'Eventos listados');
  }

  async createPlan(dto: CreatePlanDto) { return this.wrap(await this.planRepo.save(this.planRepo.create(dto)), 'Plan creado'); }
  async listPlanes() { return this.wrap(await this.planRepo.find({ where: { is_deleted: false } }), 'Planes listados'); }
  async getPlan(id: string) { return this.wrap(await this.findOneOrFail(this.planRepo, { id, is_deleted: false }), 'Plan obtenido'); }
  async updatePlan(id: string, dto: UpdatePlanDto) { const p = await this.findOneOrFail(this.planRepo, { id, is_deleted: false }); Object.assign(p, dto); return this.wrap(await this.planRepo.save(p), 'Plan actualizado'); }
  async deletePlan(id: string) { const p = await this.findOneOrFail(this.planRepo, { id, is_deleted: false }); p.is_deleted = true; await this.planRepo.save(p); return this.wrap(true, 'Plan eliminado'); }
  async createPlanTarea(planId: string, dto: CreatePlanTareaDto) { return this.wrap(await this.planTareaRepo.save(this.planTareaRepo.create({ ...dto, plan_id: planId })), 'Tarea creada'); }
  async listPlanTareas(planId: string) { return this.wrap(await this.planTareaRepo.find({ where: { plan_id: planId, is_deleted: false }, order: { orden: 'ASC' } }), 'Tareas listadas'); }
  async updatePlanTarea(id: string, dto: UpdatePlanTareaDto) { const t = await this.findOneOrFail(this.planTareaRepo, { id, is_deleted: false }); Object.assign(t, dto); return this.wrap(await this.planTareaRepo.save(t), 'Tarea actualizada'); }
  async deletePlanTarea(id: string) { const t = await this.findOneOrFail(this.planTareaRepo, { id, is_deleted: false }); t.is_deleted = true; await this.planTareaRepo.save(t); return this.wrap(true, 'Tarea eliminada'); }

  async createProgramacion(dto: CreateProgramacionDto) { return this.wrap(await this.programacionRepo.save(this.programacionRepo.create(dto)), 'Programación creada'); }
  async listProgramaciones() { return this.wrap(await this.programacionRepo.find({ where: { is_deleted: false } }), 'Programaciones listadas'); }
  async getProgramacion(id: string) { return this.wrap(await this.findOneOrFail(this.programacionRepo, { id, is_deleted: false }), 'Programación obtenida'); }
  async updateProgramacion(id: string, dto: UpdateProgramacionDto) { const p = await this.findOneOrFail(this.programacionRepo, { id, is_deleted: false }); Object.assign(p, dto); return this.wrap(await this.programacionRepo.save(p), 'Programación actualizada'); }
  async deleteProgramacion(id: string) { const p = await this.findOneOrFail(this.programacionRepo, { id, is_deleted: false }); p.is_deleted = true; await this.programacionRepo.save(p); return this.wrap(true, 'Programación eliminada'); }

  async listAlertas(q: AlertaQueryDto) {
    const where: FindOptionsWhere<AlertaMantenimientoEntity> = { is_deleted: false };
    if (q.estado) where.estado = q.estado;
    if (q.tipo_alerta) where.tipo_alerta = q.tipo_alerta;
    if (q.equipo_id) where.equipo_id = q.equipo_id;
    return this.wrap(await this.alertaRepo.find({ where }), 'Alertas listadas');
  }

  async recalculateAlertas() {
    const programaciones = await this.programacionRepo.find({ where: { is_deleted: false, activo: true } });
    let upserts = 0;
    for (const prog of programaciones) {
      const equipo = await this.equipoRepo.findOne({ where: { id: prog.equipo_id, is_deleted: false } });
      if (!equipo) continue;
      const h = Number(equipo.horometro_actual ?? 0);
      const pH = prog.proxima_horas ? Number(prog.proxima_horas) : null;
      const pF = prog.proxima_fecha ? new Date(prog.proxima_fecha) : null;
      const today = new Date();
      let tipo: string | null = null;
      if (pH !== null && h >= pH) tipo = 'OVERDUE';
      else if (pF && today > pF) tipo = 'OVERDUE';
      else if (pH !== null) {
        const diff = pH - h;
        if (diff <= 325) tipo = 'MPG_325'; else if (diff <= 650) tipo = 'MPG_650'; else if (diff <= 975) tipo = 'MPG_975'; else if (diff <= 1300) tipo = 'MPG_1300';
      }
      if (!tipo) continue;
      const existing = await this.alertaRepo.findOne({ where: { equipo_id: prog.equipo_id, tipo_alerta: tipo, estado: 'ABIERTA', is_deleted: false } });
      if (!existing) { await this.alertaRepo.save(this.alertaRepo.create({ equipo_id: prog.equipo_id, tipo_alerta: tipo, detalle: `Recalculada plan ${prog.plan_id}` })); upserts++; }
    }
    return this.wrap({ total: upserts }, 'Alertas recalculadas');
  }

  async listWorkOrders(q: WorkOrderQueryDto) {
    const qb = this.woRepo.createQueryBuilder('wo').where('wo.is_deleted = false');
    if (q.equipo_id) qb.andWhere('wo.equipment_id = :equipo', { equipo: q.equipo_id });
    if (q.estado) qb.andWhere('wo.status_workflow = :estado', { estado: q.estado });
    if (q.maintenance_kind) qb.andWhere('wo.maintenance_kind = :kind', { kind: q.maintenance_kind });
    return this.wrap(await qb.getMany(), 'Work orders listadas');
  }


  async createWorkOrder(dto: CreateWorkOrderDto) {
    await this.findEquipoOrFail(dto.equipment_id);
    if (dto.plan_id) await this.findOneOrFail(this.planRepo, { id: dto.plan_id, is_deleted: false });
    const created = await this.woRepo.save(this.woRepo.create({
      equipment_id: dto.equipment_id,
      maintenance_kind: dto.maintenance_kind ?? dto.plan_id ?? null,
      status_workflow: dto.status_workflow ?? 'PENDIENTE',
    }));
    if (dto.alerta_id) {
      const alerta = await this.findOneOrFail(this.alertaRepo, { id: dto.alerta_id, is_deleted: false });
      alerta.work_order_id = created.id;
      alerta.estado = 'EN_PROCESO';
      await this.alertaRepo.save(alerta);
    }
    return this.wrap(created, 'Work order creada');
  }

  async updateWorkOrder(id: string, dto: UpdateWorkOrderDto) {
    const wo = await this.findOneOrFail(this.woRepo, { id, is_deleted: false });
    Object.assign(wo, dto);
    return this.wrap(await this.woRepo.save(wo), 'Work order actualizada');
  }

  async deleteWorkOrder(id: string) {
    const wo = await this.findOneOrFail(this.woRepo, { id, is_deleted: false });
    wo.is_deleted = true;
    await this.woRepo.save(wo);
    return this.wrap(true, 'Work order eliminada');
  }

  async listWorkOrderTareas(workOrderId: string) {
    await this.findOneOrFail(this.woRepo, { id: workOrderId, is_deleted: false });
    return this.wrap(await this.woTareaRepo.find({ where: { work_order_id: workOrderId, is_deleted: false } }), 'Tareas de OT listadas');
  }

  async createWorkOrderTarea(workOrderId: string, dto: CreateWorkOrderTareaDto) {
    await this.findOneOrFail(this.woRepo, { id: workOrderId, is_deleted: false });
    await this.findOneOrFail(this.planRepo, { id: dto.plan_id, is_deleted: false });
    await this.findOneOrFail(this.planTareaRepo, { id: dto.tarea_id, plan_id: dto.plan_id, is_deleted: false });
    const created = await this.woTareaRepo.save(this.woTareaRepo.create({ ...dto, work_order_id: workOrderId }));
    return this.wrap(created, 'Tarea de OT creada');
  }

  async updateWorkOrderTarea(id: string, dto: UpdateWorkOrderTareaDto) {
    const tarea = await this.findOneOrFail(this.woTareaRepo, { id, is_deleted: false });
    Object.assign(tarea, dto);
    return this.wrap(await this.woTareaRepo.save(tarea), 'Tarea de OT actualizada');
  }

  async deleteWorkOrderTarea(id: string) {
    const tarea = await this.findOneOrFail(this.woTareaRepo, { id, is_deleted: false });
    tarea.is_deleted = true;
    await this.woTareaRepo.save(tarea);
    return this.wrap(true, 'Tarea de OT eliminada');
  }

  async uploadWorkOrderAdjunto(workOrderId: string, dto: UploadWorkOrderAdjuntoDto) {
    await this.findOneOrFail(this.woRepo, { id: workOrderId, is_deleted: false });
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
    const created = await this.woAdjuntoRepo.save(this.woAdjuntoRepo.create({
      work_order_id: workOrderId,
      tipo: dto.tipo ?? 'EVIDENCIA',
      nombre: originalName,
      url: filePath,
      hash_sha256: hash,
      meta: { ...(dto.meta ?? {}), mime_type: dto.mime_type ?? null, extension: extname(originalName) || null, size_bytes: buffer.length },
    }));
    return this.wrap(created, 'Adjunto cargado');
  }

  async listWorkOrderAdjuntos(workOrderId: string, query: WorkOrderAdjuntoQueryDto) {
    await this.findOneOrFail(this.woRepo, { id: workOrderId, is_deleted: false });
    const where: FindOptionsWhere<WorkOrderAdjuntoEntity> = { work_order_id: workOrderId, is_deleted: false };
    if (query.tipo) where.tipo = query.tipo;
    return this.wrap(await this.woAdjuntoRepo.find({ where }), 'Adjuntos listados');
  }

  async getWorkOrderAdjunto(workOrderId: string, adjuntoId: string) {
    const adjunto = await this.findOneOrFail(this.woAdjuntoRepo, { id: adjuntoId, work_order_id: workOrderId, is_deleted: false });
    const buffer = await readFile(adjunto.url);
    const base64 = buffer.toString('base64');
    const meta = (adjunto.meta ?? {}) as Record<string, unknown>;
    const mimeType = typeof meta.mime_type === 'string' ? meta.mime_type : 'application/octet-stream';
    return this.wrap({
      id: adjunto.id,
      work_order_id: adjunto.work_order_id,
      tipo: adjunto.tipo,
      nombre: adjunto.nombre,
      hash_sha256: adjunto.hash_sha256,
      contenido_base64: base64,
      content_type: mimeType,
      data_url: `data:${mimeType};base64,${base64}`,
      meta: adjunto.meta,
    }, 'Adjunto obtenido');
  }

  async deleteWorkOrderAdjunto(workOrderId: string, adjuntoId: string) {
    const adjunto = await this.findOneOrFail(this.woAdjuntoRepo, { id: adjuntoId, work_order_id: workOrderId, is_deleted: false });
    adjunto.is_deleted = true;
    await this.woAdjuntoRepo.save(adjunto);
    try { await unlink(adjunto.url); } catch { /* ignore */ }
    return this.wrap(true, 'Adjunto eliminado');
  }

  async createConsumo(workOrderId: string, dto: CreateConsumoDto) {
    await this.findOneOrFail(this.woRepo, { id: workOrderId, is_deleted: false });
    const subtotal = dto.cantidad * dto.costo_unitario;
    return this.wrap(await this.consumoRepo.save(this.consumoRepo.create({ ...dto, work_order_id: workOrderId, subtotal })), 'Consumo registrado');
  }

  async issueMaterials(workOrderId: string, dto: IssueMaterialsDto) {
    const qr = this.dataSource.createQueryRunner();
    await qr.connect(); await qr.startTransaction();
    try {
      await this.findOneOrFail(this.woRepo, { id: workOrderId, is_deleted: false });
      const em = await qr.manager.save(EntregaMaterialEntity, qr.manager.create(EntregaMaterialEntity, { work_order_id: workOrderId, code: `EM-${Date.now()}`, observacion: dto.observacion }));
      const mov = await qr.manager.save(MovimientoInventarioEntity, qr.manager.create(MovimientoInventarioEntity, { tipo_movimiento: 'SALIDA', work_order_id: workOrderId, total_costos: 0 }));
      let total = 0;
      for (const item of dto.items) {
        const reserva = await qr.manager.findOne(ReservaStockEntity, { where: { work_order_id: workOrderId, producto_id: item.producto_id, bodega_id: item.bodega_id, estado: 'RESERVADO', is_deleted: false } });
        if (!reserva || Number(reserva.cantidad) < item.cantidad) throw new ConflictException('Reserva insuficiente');
        const stock = await qr.manager.findOne(StockBodegaEntity, { where: { producto_id: item.producto_id, bodega_id: item.bodega_id } });
        if (!stock || Number(stock.stock_actual) < item.cantidad) throw new ConflictException('Stock insuficiente');
        const producto = await qr.manager.findOne(ProductoEntity, { where: { id: item.producto_id } });
        if (!producto) throw new NotFoundException('Producto no encontrado');
        const costo = Number(producto.ultimo_costo);
        const subtotal = item.cantidad * costo;
        total += subtotal;
        stock.stock_actual = Number(stock.stock_actual) - item.cantidad;
        await qr.manager.save(stock);
        reserva.estado = 'CONSUMIDO';
        await qr.manager.save(reserva);
        await qr.manager.save(EntregaMaterialDetEntity, qr.manager.create(EntregaMaterialDetEntity, { entrega_id: em.id, producto_id: item.producto_id, bodega_id: item.bodega_id, cantidad: item.cantidad, costo_unitario: costo }));
        const movDet = await qr.manager.save(MovimientoInventarioDetEntity, qr.manager.create(MovimientoInventarioDetEntity, { movimiento_id: mov.id, producto_id: item.producto_id, cantidad: item.cantidad, costo_unitario: costo, subtotal_costo: subtotal }));
        await qr.manager.save(KardexEntity, qr.manager.create(KardexEntity, { bodega_id: item.bodega_id, producto_id: item.producto_id, movimiento_id: mov.id, movimiento_det_id: movDet.id, tipo_movimiento: 'SALIDA', salida_cantidad: item.cantidad, costo_unitario: costo, costo_total: subtotal, saldo_cantidad: stock.stock_actual, saldo_costo_promedio: costo, saldo_valorizado: Number(stock.stock_actual) * costo }));
        await qr.manager.save(ConsumoRepuestoEntity, qr.manager.create(ConsumoRepuestoEntity, { work_order_id: workOrderId, producto_id: item.producto_id, bodega_id: item.bodega_id, cantidad: item.cantidad, costo_unitario: costo, subtotal }));
      }
      mov.total_costos = total;
      await qr.manager.save(mov);
      await qr.commitTransaction();
      return this.wrap({ entrega_id: em.id, movimiento_id: mov.id, total }, 'Materiales entregados');
    } catch (e) {
      await qr.rollbackTransaction();
      throw e;
    } finally {
      await qr.release();
    }
  }

  private async findEquipoOrFail(id: string) { return this.findOneOrFail(this.equipoRepo, { id, is_deleted: false }); }
  private async findOneOrFail<T extends ObjectLiteral>(repo: Repository<T>, where: FindOptionsWhere<T>) {
    const item = await repo.findOne({ where });
    if (!item) throw new NotFoundException('Registro no encontrado');
    return item;
  }

  async seedEstadosCatalogo() {
    const codigos = ['OPERATIVO', 'RESERVA', 'MPG', 'CORRECTIVO', 'BLOQUEADA'];
    for (const codigo of codigos) {
      const existing = await this.estadoCatalogoRepo.findOne({ where: { codigo, is_deleted: false } });
      if (!existing) await this.estadoCatalogoRepo.save(this.estadoCatalogoRepo.create({ codigo, descripcion: codigo }));
    }
  }
}
