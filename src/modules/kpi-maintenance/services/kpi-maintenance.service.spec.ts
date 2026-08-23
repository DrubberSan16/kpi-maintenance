import {
  BadRequestException,
  ConflictException,
  ForbiddenException,
  NotFoundException,
} from '@nestjs/common';
import { DataSource } from 'typeorm';
import { KpiMaintenanceService } from './kpi-maintenance.service';

const createRepo = () => ({
  findOne: jest.fn(),
  find: jest.fn().mockResolvedValue([]),
  save: jest.fn(async (value) => value),
  create: jest.fn((value) => value),
  createQueryBuilder: jest.fn(),
  delete: jest.fn(),
  softDelete: jest.fn(),
  update: jest.fn(),
  count: jest.fn(),
});

const createRepos = () => ({
  equipoRepo: createRepo(),
  equipoTipoRepo: createRepo(),
  equipoComponenteRepo: createRepo(),
  locationRepo: createRepo(),
  marcaRepo: createRepo(),
  sucursalRepo: createRepo(),
  bitacoraRepo: createRepo(),
  alertaRepo: createRepo(),
  estadoRepo: createRepo(),
  estadoCatalogoRepo: createRepo(),
  eventoRepo: createRepo(),
  fallaRepo: createRepo(),
  lecturaRepo: createRepo(),
  lubricacionRepo: createRepo(),
  procedimientoRepo: createRepo(),
  procedimientoActividadRepo: createRepo(),
  analisisLubricanteRepo: createRepo(),
  analisisLubricanteDetRepo: createRepo(),
  cronogramaSemanalRepo: createRepo(),
  cronogramaSemanalDetRepo: createRepo(),
  programacionMensualRepo: createRepo(),
  programacionMensualDetRepo: createRepo(),
  reporteDiarioRepo: createRepo(),
  reporteDiarioUnidadRepo: createRepo(),
  reporteCombustibleRepo: createRepo(),
  controlComponenteRepo: createRepo(),
  eventoProcesoRepo: createRepo(),
  planRepo: createRepo(),
  planTareaRepo: createRepo(),
  programacionRepo: createRepo(),
  woRepo: createRepo(),
  woHistoryRepo: createRepo(),
  consumoRepo: createRepo(),
  stockRepo: createRepo(),
  kardexRepo: createRepo(),
  productoRepo: createRepo(),
  unidadMedidaRepo: createRepo(),
  bodegaRepo: createRepo(),
  reservaRepo: createRepo(),
  woTareaRepo: createRepo(),
  woAdjuntoRepo: createRepo(),
});

type RepoBag = ReturnType<typeof createRepos>;

const createDataSourceMock = () =>
  ({
    query: jest.fn().mockResolvedValue([]),
    createQueryRunner: jest.fn(() => ({
      connect: jest.fn(),
      startTransaction: jest.fn(),
      commitTransaction: jest.fn(),
      rollbackTransaction: jest.fn(),
      release: jest.fn(),
      manager: {
        save: jest.fn(async (_entity, value) => value),
        create: jest.fn((_entity, value) => value),
        findOne: jest.fn(),
      },
    })),
  }) as unknown as DataSource;

const createService = (repos: RepoBag, ds: DataSource) =>
  new KpiMaintenanceService(
    repos.equipoRepo as any,
    repos.equipoTipoRepo as any,
    repos.equipoComponenteRepo as any,
    repos.locationRepo as any,
    repos.marcaRepo as any,
    repos.sucursalRepo as any,
    repos.bitacoraRepo as any,
    repos.alertaRepo as any,
    repos.estadoRepo as any,
    repos.estadoCatalogoRepo as any,
    repos.eventoRepo as any,
    repos.fallaRepo as any,
    repos.lecturaRepo as any,
    repos.lubricacionRepo as any,
    repos.procedimientoRepo as any,
    repos.procedimientoActividadRepo as any,
    repos.analisisLubricanteRepo as any,
    repos.analisisLubricanteDetRepo as any,
    repos.cronogramaSemanalRepo as any,
    repos.cronogramaSemanalDetRepo as any,
    repos.programacionMensualRepo as any,
    repos.programacionMensualDetRepo as any,
    repos.reporteDiarioRepo as any,
    repos.reporteDiarioUnidadRepo as any,
    repos.reporteCombustibleRepo as any,
    repos.controlComponenteRepo as any,
    repos.eventoProcesoRepo as any,
    repos.planRepo as any,
    repos.planTareaRepo as any,
    repos.programacionRepo as any,
    repos.woRepo as any,
    repos.woHistoryRepo as any,
    repos.consumoRepo as any,
    repos.stockRepo as any,
    repos.kardexRepo as any,
    repos.productoRepo as any,
    repos.unidadMedidaRepo as any,
    repos.bodegaRepo as any,
    repos.reservaRepo as any,
    repos.woTareaRepo as any,
    repos.woAdjuntoRepo as any,
    ds,
  );

describe('KpiMaintenanceService alerts', () => {
  let repos: RepoBag;
  let service: KpiMaintenanceService;

  beforeEach(() => {
    jest.clearAllMocks();
    repos = createRepos();
    service = createService(repos, createDataSourceMock());
  });

  it('envia correos cuando se dispara una alerta nueva', async () => {
    const sendMail = jest.fn().mockResolvedValue(undefined);
    jest
      .spyOn(service as any, 'resolveAlertNotificationRecipients')
      .mockResolvedValue([
        {
          type: 'TRANSACTION_OWNER',
          email: 'operador@example.com',
          userId: 'u-1',
          username: 'operador',
          displayName: 'Operador',
          roleName: 'SUPERVISOR',
        },
        {
          type: 'GENERAL_MANAGER',
          email: 'gerencia@example.com',
          userId: 'u-2',
          username: 'gerencia',
          displayName: 'Gerencia',
          roleName: 'GERENTE GENERAL',
        },
        {
          type: 'ADMINISTRATOR',
          email: 'admin@example.com',
          userId: 'u-3',
          username: 'admin',
          displayName: 'Administrador',
          roleName: 'ADMINISTRADOR',
        },
      ]);
    jest
      .spyOn(service as any, 'getAlertMailTransporter')
      .mockResolvedValue({ sendMail } as any);

    const result = await (service as any).sendAlertTriggerEmails({
      id: 'alert-1',
      categoria: 'INVENTARIO',
      nivel: 'WARNING',
      estado: 'ABIERTA',
      origen: 'INVENTARIO',
      tipo_alerta: 'STOCK_BAJO_BODEGA',
      detalle: 'Stock bajo',
      referencia: 'STOCK_BODEGA:1',
      referencia_tipo: 'STOCK_BODEGA',
      fecha_generada: new Date('2026-03-29T10:00:00Z'),
      payload_json: { equipo_codigo: 'UGN-03' },
    });

    expect(sendMail).toHaveBeenCalledTimes(3);
    expect(sendMail).toHaveBeenCalledWith(
      expect.objectContaining({
        replyTo: 'operador@example.com',
      }),
    );
    expect(result.sent).toEqual([
      'operador@example.com',
      'gerencia@example.com',
      'admin@example.com',
    ]);
  });

  it('resuelve el correo del usuario transaccionante desde usuarios y expande la lista de gerencia general', async () => {
    jest.spyOn(service as any, 'fetchSecurityUsers').mockResolvedValue([
      {
        id: 'u-actor',
        nameUser: 'operador',
        nameSurname: 'Operador Uno',
        email: 'operador.real@example.com',
        roleName: 'SUPERVISOR',
        roleNames: ['SUPERVISOR'],
        status: 'ACTIVE',
        isDeleted: false,
      },
      {
        id: 'u-g1',
        nameUser: 'gerencia1',
        nameSurname: 'Gerente Uno',
        email: 'gerente1@example.com',
        roleName: 'GERENTE GENERAL',
        roleNames: ['GERENTE GENERAL'],
        status: 'ACTIVE',
        isDeleted: false,
      },
      {
        id: 'u-g2',
        nameUser: 'gerencia2',
        nameSurname: 'Gerente Dos',
        email: 'gerente2@example.com',
        roleName: 'GERENCIA GENERAL',
        roleNames: ['GERENCIA GENERAL'],
        status: 'ACTIVE',
        isDeleted: false,
      },
      {
        id: 'u-admin',
        nameUser: 'admin',
        nameSurname: 'Admin Uno',
        email: 'admin@example.com',
        roleName: 'ADMINISTRADOR',
        roleNames: ['ADMINISTRADOR'],
        status: 'ACTIVE',
        isDeleted: false,
      },
    ]);

    const recipients = await (service as any).resolveAlertNotificationRecipients({
      actor_user_id: 'u-actor',
      actor_username: 'operador',
      actor_email: 'correo-stale@example.com',
    });

    expect(recipients).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          type: 'TRANSACTION_OWNER',
          email: 'operador.real@example.com',
          userId: 'u-actor',
        }),
        expect.objectContaining({
          type: 'GENERAL_MANAGER',
          email: 'gerente1@example.com',
          userId: 'u-g1',
        }),
        expect.objectContaining({
          type: 'GENERAL_MANAGER',
          email: 'gerente2@example.com',
          userId: 'u-g2',
        }),
        expect.objectContaining({
          type: 'ADMINISTRATOR',
          email: 'admin@example.com',
          userId: 'u-admin',
        }),
      ]),
    );
  });

  it('crea una alerta nueva y dispara notificaciones en el recálculo', async () => {
    repos.alertaRepo.find.mockResolvedValue([]);
    repos.alertaRepo.save.mockImplementation(async (value) => ({
      id: 'alert-1',
      ...value,
    }));
    const dispatchSpy = jest
      .spyOn(service as any, 'dispatchAlertTriggeredNotifications')
      .mockResolvedValue(undefined);

    const stats = await (service as any).syncAlertCandidates([
      {
        equipo_id: 'equipo-1',
        tipo_alerta: 'STOCK_BAJO_BODEGA',
        categoria: 'INVENTARIO',
        nivel: 'WARNING',
        origen: 'INVENTARIO',
        referencia_tipo: 'STOCK_BODEGA',
        referencia: 'STOCK_BODEGA:1',
        detalle: 'Stock 10 / minimo 20',
        payload_json: { producto_id: 'producto-1' },
      },
    ]);

    expect(stats.created).toBe(1);
    expect(repos.alertaRepo.save).toHaveBeenCalled();
    expect(dispatchSpy).toHaveBeenCalledTimes(1);
  });

  it('no dispara el correo global de inventario al sincronizar su alerta', async () => {
    repos.alertaRepo.find.mockResolvedValue([]);
    repos.alertaRepo.save.mockImplementation(async (value) => ({
      id: 'alert-inventory',
      ...value,
    }));
    const dispatchSpy = jest
      .spyOn(service as any, 'dispatchAlertTriggeredNotifications')
      .mockResolvedValue(undefined);

    await (service as any).syncAlertCandidates(
      [
        {
          tipo_alerta: 'STOCK_BAJO_BODEGA',
          categoria: 'INVENTARIO',
          nivel: 'WARNING',
          origen: 'INVENTARIO',
          referencia_tipo: 'INVENTARIO_RESUMEN',
          referencia: 'INVENTARIO:RESUMEN_GENERAL',
          detalle: '1 material en alerta',
          payload_json: { inventory_items: [] },
        },
      ],
      {
        shouldNotifyCandidate: (candidate: any) =>
          candidate.origen !== 'INVENTARIO',
      },
    );

    expect(dispatchSpy).not.toHaveBeenCalled();
  });

  it('calcula el stock minimo sin incluir la reserva critica', async () => {
    repos.stockRepo.find.mockResolvedValue([
      {
        id: 'stock-1',
        producto_id: 'producto-1',
        bodega_id: 'bodega-1',
        stock_actual: 10,
        stock_critico: 4,
        stock_min_bodega: 7,
        stock_max_bodega: 20,
        costo_promedio_bodega: 1,
        created_by: 'operador',
        is_deleted: false,
      },
    ]);
    repos.productoRepo.find.mockResolvedValue([
      { id: 'producto-1', codigo: 'MAT-1', nombre: 'Material uno' },
    ]);
    repos.bodegaRepo.find.mockResolvedValue([
      {
        id: 'bodega-1',
        codigo: 'BOD-001',
        nombre: 'Principal',
        sucursal_id: 'sucursal-1',
      },
    ]);

    const candidates = await (service as any).buildInventoryAlertCandidates();
    const item = candidates[0].payload_json.inventory_items[0];

    expect(item).toMatchObject({
      stock_actual: 10,
      stock_critico: 4,
      stock_disponible_minimo: 6,
      stock_min_bodega: 7,
      sucursal_id: 'sucursal-1',
    });
  });

  it('secciona destinatarios por sucursal y deja alcance global al super administrador', async () => {
    jest.spyOn(service as any, 'fetchSecurityUsers').mockResolvedValue([
      {
        id: 'warehouse-a',
        nameUser: 'bodega-a',
        nameSurname: 'Bodega A',
        email: 'bodega-a@example.com',
        roleName: 'PERSONAL DE BODEGA',
        roleNames: ['PERSONAL DE BODEGA'],
        status: 'ACTIVE',
        isDeleted: false,
        sucursalIds: ['sucursal-a'],
        allSucursales: false,
      },
      {
        id: 'admin-b',
        nameUser: 'admin-b',
        nameSurname: 'Admin B',
        email: 'admin-b@example.com',
        roleName: 'ADMINISTRATIVO',
        roleNames: ['ADMINISTRATIVO'],
        status: 'ACTIVE',
        isDeleted: false,
        sucursalIds: ['sucursal-b'],
        allSucursales: false,
      },
      {
        id: 'super',
        nameUser: 'super',
        nameSurname: 'Super Administrador',
        email: 'super@example.com',
        roleName: 'SUPER ADMINISTRADOR',
        roleNames: ['SUPER ADMINISTRADOR'],
        status: 'ACTIVE',
        isDeleted: false,
        sucursalIds: [],
        allSucursales: true,
      },
    ]);
    const items = [
      { sucursal_id: 'sucursal-a', bodega_id: 'bodega-a' },
      { sucursal_id: 'sucursal-b', bodega_id: 'bodega-b' },
    ];

    const scoped = await (service as any).resolveScopedInventoryRecipients(items);
    const byEmail = new Map(
      scoped.map((entry: any) => [entry.recipient.email, entry.items]),
    );

    expect(byEmail.get('bodega-a@example.com')).toHaveLength(1);
    expect(byEmail.get('admin-b@example.com')).toHaveLength(1);
    expect(byEmail.get('super@example.com')).toHaveLength(2);
  });

  it('el correo por movimiento incluye solo el stock afectado', async () => {
    const sendSpy = jest
      .spyOn(service as any, 'sendScopedInventoryStockEmails')
      .mockResolvedValue({ sent: 1, failed: 0, recipients: 1 });
    const buildItem = (stockId: string) => ({
      stock_id: stockId,
      producto_id: `producto-${stockId}`,
      producto_label: `Material ${stockId}`,
      bodega_id: 'bodega-1',
      bodega_label: 'BOD-001',
      sucursal_id: 'sucursal-1',
    });

    await (service as any).dispatchInventoryRecalculationEmails(
      [
        {
          origen: 'INVENTARIO',
          payload_json: {
            inventory_items: [buildItem('stock-1'), buildItem('stock-2')],
          },
        },
      ],
      'inventory-kardex-document',
      { stock_id: 'stock-2', movement_direction: 'decrease' },
    );

    expect(sendSpy).toHaveBeenCalledWith(
      [expect.objectContaining({ stock_id: 'stock-2' })],
      'movement',
    );
  });

  it('cierra alertas gestionadas cuando la condición desaparece', async () => {
    repos.alertaRepo.find.mockResolvedValue([
      {
        id: 'alert-1',
        equipo_id: 'equipo-1',
        tipo_alerta: 'STOCK_BAJO_BODEGA',
        categoria: 'INVENTARIO',
        nivel: 'WARNING',
        origen: 'INVENTARIO',
        referencia_tipo: 'STOCK_BODEGA',
        referencia: 'STOCK_BODEGA:1',
        detalle: 'Stock bajo',
        fecha_generada: new Date('2026-03-29T10:00:00Z'),
        ultima_evaluacion_at: new Date('2026-03-29T10:00:00Z'),
        estado: 'ABIERTA',
        payload_json: {},
        is_deleted: false,
      },
    ]);
    repos.alertaRepo.save.mockImplementation(async (value) => value);

    const stats = await (service as any).syncAlertCandidates([]);

    expect(stats.resolved).toBe(1);
    expect(repos.alertaRepo.save).toHaveBeenCalledWith(
      expect.objectContaining({
        id: 'alert-1',
        estado: 'CERRADA',
      }),
    );
  });

  it('bitácora con horómetro retrocedido crea alerta, notifica y rechaza la operación', async () => {
    repos.equipoRepo.findOne.mockResolvedValue({
      id: 'equipo-1',
      is_deleted: false,
    });
    repos.bitacoraRepo.findOne.mockResolvedValue({
      horometro: 100,
    });
    repos.alertaRepo.save.mockImplementation(async (value) => ({
      id: 'alert-1',
      ...value,
    }));
    const dispatchSpy = jest
      .spyOn(service as any, 'dispatchAlertTriggeredNotifications')
      .mockResolvedValue(undefined);

    await expect(
      service.createBitacora('equipo-1', {
        fecha: '2026-03-29',
        horometro: 90,
        registrado_por: 'operador',
      } as any),
    ).rejects.toBeInstanceOf(ConflictException);

    expect(repos.alertaRepo.save).toHaveBeenCalledTimes(1);
    expect(dispatchSpy).toHaveBeenCalledWith(
      expect.objectContaining({
        id: 'alert-1',
        tipo_alerta: 'ANOMALIA_HOROMETRO',
      }),
    );
  });
  it('marca la alerta de programación como informativa cuando la OT vinculada culmina', async () => {
    const recalcSpy = jest
      .spyOn(service as any, 'recalculateProgramacionFields')
      .mockResolvedValue({});

    repos.alertaRepo.find.mockResolvedValue([
      {
        id: 'alert-1',
        equipo_id: 'equipo-1',
        tipo_alerta: 'MANTENIMIENTO_VENCIDO',
        categoria: 'MANTENIMIENTO',
        nivel: 'CRITICAL',
        origen: 'PROGRAMACION',
        referencia_tipo: 'PROGRAMACION',
        referencia: 'PROGRAMACION:prog-1',
        detalle: 'UGN - 03 · MPG 325H · atrasada 0 d',
        fecha_generada: new Date('2026-04-02T10:00:00Z'),
        ultima_evaluacion_at: new Date('2026-04-02T10:00:00Z'),
        estado: 'EN_PROCESO',
        payload_json: {
          programacion_id: 'prog-1',
          plan_nombre: 'MPG 325H',
          work_orders: [
            {
              id: 'wo-1',
              code: 'OT-A00005',
              title: 'Cambio de aceite',
              status_workflow: 'IN_PROGRESS',
            },
          ],
        },
        work_order_id: 'wo-1',
        is_deleted: false,
      },
    ]);
    repos.alertaRepo.save.mockImplementation(async (value) => value);
    repos.programacionRepo.findOne.mockResolvedValue({
      id: 'prog-1',
      equipo_id: 'equipo-1',
      plan_id: 'plan-1',
      is_deleted: false,
      payload_json: {},
      ultima_ejecucion_fecha: null,
      ultima_ejecucion_horas: null,
    });
    repos.equipoRepo.findOne.mockResolvedValue({
      id: 'equipo-1',
      is_deleted: false,
      horometro_actual: 15432,
    });

    await (service as any).syncAlertsForWorkOrder({
      id: 'wo-1',
      code: 'OT-A00005',
      title: 'Cambio de aceite',
      status_workflow: 'CLOSED',
      equipment_id: 'equipo-1',
      closed_at: new Date('2026-04-02T16:30:00Z'),
    });

    expect(recalcSpy).toHaveBeenCalledWith(
      expect.objectContaining({
        id: 'prog-1',
        ultima_ejecucion_fecha: '2026-04-02',
        ultima_ejecucion_horas: 15432,
      }),
    );
    expect(repos.alertaRepo.save).toHaveBeenCalledWith(
      expect.objectContaining({
        id: 'alert-1',
        estado: 'CERRADA',
        nivel: 'INFO',
        detalle: expect.stringContaining('OT-A00005'),
      }),
    );
  });
});

describe('KpiMaintenanceService work orders', () => {
  let repos: RepoBag;
  let service: KpiMaintenanceService;

  beforeEach(() => {
    jest.clearAllMocks();
    repos = createRepos();
    service = createService(repos, createDataSourceMock());

    jest
      .spyOn(service as any, 'appendWorkOrderHistory')
      .mockResolvedValue(undefined);
    jest
      .spyOn(service as any, 'syncAlertWorkOrderLink')
      .mockResolvedValue(undefined);
    jest
      .spyOn(service as any, 'syncAlertsForWorkOrder')
      .mockResolvedValue(undefined);
    jest
      .spyOn(service as any, 'publishInAppNotification')
      .mockResolvedValue(undefined);
    jest
      .spyOn(service as any, 'writeSecurityLog')
      .mockResolvedValue(undefined);
    jest
      .spyOn(service as any, 'registerProcessEvent')
      .mockResolvedValue(undefined);
    jest
      .spyOn(service as any, 'ensureAutomaticWorkOrderAlertWithManager')
      .mockResolvedValue({ created: false, alert: { id: null } });
  });

  it('rechaza cambios en una OT bloqueada por una anexada activa', async () => {
    repos.woRepo.findOne.mockResolvedValue({
      id: 'wo-blocker',
      code: 'OT-A00002',
      status_workflow: 'IN_PROGRESS',
      is_deleted: false,
    });

    await expect(
      (service as any).assertWorkOrderNotBlockedByActiveAnnex(
        {
          id: 'wo-1',
          code: 'OT-A00001',
          status_workflow: 'BLOCKED',
          blocked_by_work_order_id: 'wo-blocker',
        },
        undefined,
        'cambiar el estado de la orden',
      ),
    ).rejects.toBeInstanceOf(ConflictException);
  });

  it('rechaza una OT en estado bloqueado sin OT anexada', async () => {
    await expect(
      (service as any).assertWorkOrderNotBlockedByActiveAnnex(
        {
          id: 'wo-1',
          code: 'OT-A00001',
          status_workflow: 'BLOCKED',
          blocked_by_work_order_id: null,
        },
        undefined,
        'guardar la orden',
      ),
    ).rejects.toBeInstanceOf(ConflictException);
  });

  it('crea la OT sincronizando plantilla y guardando la cabecera correctamente', async () => {
    repos.equipoRepo.findOne.mockResolvedValue({
      id: 'equipo-1',
      nombre: 'UG 03',
      codigo: 'UG03',
      is_deleted: false,
    });
    repos.planRepo.findOne.mockResolvedValue({
      id: 'plan-1',
      nombre: 'Plan 325H',
      codigo: '325H',
      is_deleted: false,
    });
    repos.woRepo.save.mockImplementation(async (value) => ({
      id: 'wo-1',
      ...value,
    }));

    jest
      .spyOn(service as any, 'syncPlanFromProcedimiento')
      .mockResolvedValue({ plan: { id: 'plan-1' } });
    jest
      .spyOn(service as any, 'enrichWorkOrder')
      .mockResolvedValue({
        id: 'wo-1',
        code: 'OT-A00001',
        title: 'Orden (PMP-A00001)',
        status_workflow: 'PLANNED',
        plan_id: 'plan-1',
        procedimiento_id: 'proc-1',
      });

    await service.createWorkOrder({
      code: 'OT-A00001',
      type: 'MANTENIMIENTO',
      title: 'Orden (PMP-A00001)',
      equipment_id: 'equipo-1',
      maintenance_kind: 'CORRECTIVO',
      status_workflow: 'PLANNED',
      procedimiento_id: 'proc-1',
      valor_json: {
        causa: 'Fuga detectada',
        accion: 'Cambio de componente',
        prevencion: 'Revisión semanal',
      },
    } as any);

    expect(repos.woRepo.save).toHaveBeenCalledWith(
      expect.objectContaining({
        code: 'OT-A00001',
        type: 'MANTENIMIENTO',
        equipment_id: 'equipo-1',
        plan_id: 'plan-1',
        maintenance_kind: 'CORRECTIVO',
        status_workflow: 'PLANNED',
        valor_json: expect.objectContaining({
          causa: 'Fuga detectada',
          procedimiento_id: 'proc-1',
        }),
      }),
    );
    expect((service as any).appendWorkOrderHistory).toHaveBeenCalledWith(
      'wo-1',
      'PLANNED',
      'Orden de trabajo creada',
      { changedBy: null },
    );
  });

  it('actualiza la OT preservando el estado y mezclando valor_json de forma correcta', async () => {
    repos.equipoRepo.findOne.mockResolvedValue({
      id: 'equipo-1',
      nombre: 'UG 03',
      codigo: 'UG03',
      is_deleted: false,
    });
    repos.planRepo.findOne.mockResolvedValue({
      id: 'plan-new',
      nombre: 'Plan nuevo',
      codigo: 'PLAN-NUEVO',
      is_deleted: false,
    });
    repos.woRepo.findOne.mockResolvedValue({
      id: 'wo-1',
      code: 'OT-A00001',
      type: 'MANTENIMIENTO',
      equipment_id: 'equipo-1',
      plan_id: 'plan-old',
      title: 'Orden (PMP-A00001)',
      maintenance_kind: 'CORRECTIVO',
      status_workflow: 'PLANNED',
      valor_json: {
        causa: 'Fuga detectada',
        prevencion: 'Revisión semanal',
      },
      is_deleted: false,
    });
    repos.woRepo.save.mockImplementation(async (value) => value);

    jest
      .spyOn(service as any, 'syncPlanFromProcedimiento')
      .mockResolvedValue({ plan: { id: 'plan-new' } });
    jest
      .spyOn(service as any, 'enrichWorkOrder')
      .mockResolvedValue({
        id: 'wo-1',
        code: 'OT-A00001',
        title: 'Orden (PMP-A00001)',
        status_workflow: 'PLANNED',
        plan_id: 'plan-new',
        procedimiento_id: 'proc-2',
      });

    await service.updateWorkOrder('wo-1', {
      maintenance_kind: 'PREVENTIVO',
      status_workflow: 'PLANNED',
      procedimiento_id: 'proc-2',
      valor_json: {
        accion: 'Cambio de componente',
      },
    } as any);

    expect(repos.woRepo.save).toHaveBeenCalledWith(
      expect.objectContaining({
        id: 'wo-1',
        maintenance_kind: 'PREVENTIVO',
        plan_id: 'plan-new',
        status_workflow: 'PLANNED',
        valor_json: expect.objectContaining({
          causa: 'Fuga detectada',
          prevencion: 'Revisión semanal',
          accion: 'Cambio de componente',
          procedimiento_id: 'proc-2',
        }),
      }),
    );
    expect((service as any).appendWorkOrderHistory).toHaveBeenCalledWith(
      'wo-1',
      'PLANNED',
      'Cabecera de OT actualizada',
      { fromStatus: 'PLANNED', changedBy: null },
    );
  });

  it('rechaza crear una OT de Cebado sin equipo aunque tenga plantilla y fecha', async () => {
    repos.planRepo.findOne.mockResolvedValue({
      id: 'plan-1',
      nombre: 'Plan 325H',
      codigo: '325H',
      is_deleted: false,
    });

    await expect(
      service.createWorkOrder({
        code: 'OT-A00002',
        type: 'MANTENIMIENTO',
        title: 'Cebado UG 03',
        maintenance_kind: 'CEBADO',
        plan_id: 'plan-1',
        status_workflow: 'PLANNED',
        valor_json: { fecha_programacion: '2026-09-04' },
      } as any),
    ).rejects.toBeInstanceOf(BadRequestException);
    expect(repos.woRepo.save).not.toHaveBeenCalled();
    expect(repos.programacionRepo.save).not.toHaveBeenCalled();
  });

  it('rechaza crear una OT de Cebado sin plantilla aunque tenga equipo y fecha', async () => {
    repos.equipoRepo.findOne.mockResolvedValue({
      id: 'equipo-1',
      nombre: 'UG 03',
      codigo: 'UG03',
      is_deleted: false,
    });

    await expect(
      service.createWorkOrder({
        code: 'OT-A00002',
        type: 'MANTENIMIENTO',
        title: 'Cebado UG 03',
        maintenance_kind: 'CEBADO',
        equipment_id: 'equipo-1',
        status_workflow: 'PLANNED',
        valor_json: { fecha_programacion: '2026-09-04' },
      } as any),
    ).rejects.toBeInstanceOf(BadRequestException);
    expect(repos.woRepo.save).not.toHaveBeenCalled();
    expect(repos.programacionRepo.save).not.toHaveBeenCalled();
  });

  it('crea la OT de Cebado y su programacion cuando tiene equipo, plantilla y fecha', async () => {
    repos.equipoRepo.findOne.mockResolvedValue({
      id: 'equipo-1',
      nombre: 'UG 03',
      codigo: 'UG03',
      is_deleted: false,
    });
    repos.planRepo.findOne.mockResolvedValue({
      id: 'plan-1',
      nombre: 'Plan 325H',
      codigo: '325H',
      is_deleted: false,
    });
    repos.woRepo.save.mockImplementation(async (value) => ({
      id: 'wo-1',
      ...value,
    }));
    repos.programacionRepo.find.mockResolvedValue([]);
    repos.programacionRepo.create.mockImplementation((value: any) => ({
      id: 'prog-1',
      ...value,
    }));
    repos.programacionRepo.save.mockImplementation(async (value: any) => value);
    jest
      .spyOn(service as any, 'enrichWorkOrder')
      .mockResolvedValue({
        id: 'wo-1',
        code: 'OT-A00002',
        title: 'Cebado UG 03',
        status_workflow: 'PLANNED',
        plan_id: 'plan-1',
      });

    await service.createWorkOrder({
      code: 'OT-A00002',
      type: 'MANTENIMIENTO',
      title: 'Cebado UG 03',
      maintenance_kind: 'CEBADO',
      equipment_id: 'equipo-1',
      plan_id: 'plan-1',
      status_workflow: 'PLANNED',
      valor_json: {
        fecha_programacion: '2026-09-04',
        causa: 'Cebado programado',
        accion: 'Cebado de graseras',
        prevencion: 'Cumplir plan de lubricacion',
      },
    } as any);

    expect(repos.woRepo.save).toHaveBeenCalledWith(
      expect.objectContaining({ maintenance_kind: 'CEBADO' }),
    );
    expect(repos.programacionRepo.save).toHaveBeenCalledWith(
      expect.objectContaining({
        equipo_id: 'equipo-1',
        plan_id: 'plan-1',
        work_order_id: 'wo-1',
        proxima_fecha: '2026-09-04',
      }),
    );
  });

  it('rechaza actualizar una OT a Cebado si no tiene equipo asignado', async () => {
    repos.woRepo.findOne.mockResolvedValue({
      id: 'wo-1',
      code: 'OT-A00001',
      type: 'MANTENIMIENTO',
      equipment_id: null,
      plan_id: 'plan-1',
      title: 'Orden sin equipo',
      maintenance_kind: 'CORRECTIVO',
      status_workflow: 'PLANNED',
      valor_json: {},
      is_deleted: false,
    });
    repos.planRepo.findOne.mockResolvedValue({
      id: 'plan-1',
      nombre: 'Plan 325H',
      codigo: '325H',
      is_deleted: false,
    });

    await expect(
      service.updateWorkOrder('wo-1', {
        maintenance_kind: 'CEBADO',
        valor_json: {
          fecha_programacion: '2026-09-04',
          causa: 'Cebado programado',
          accion: 'Cebado de graseras',
          prevencion: 'Cumplir plan de lubricacion',
        },
      } as any),
    ).rejects.toBeInstanceOf(BadRequestException);
    expect(repos.woRepo.save).not.toHaveBeenCalled();
    expect(repos.programacionRepo.save).not.toHaveBeenCalled();
  });

  it('rechaza actualizar una OT a Cebado si no tiene plantilla asignada', async () => {
    repos.woRepo.findOne.mockResolvedValue({
      id: 'wo-1',
      code: 'OT-A00001',
      type: 'MANTENIMIENTO',
      equipment_id: 'equipo-1',
      plan_id: null,
      title: 'Orden sin plantilla',
      maintenance_kind: 'CORRECTIVO',
      status_workflow: 'PLANNED',
      valor_json: {},
      is_deleted: false,
    });
    repos.equipoRepo.findOne.mockResolvedValue({
      id: 'equipo-1',
      nombre: 'UG 03',
      codigo: 'UG03',
      is_deleted: false,
    });

    await expect(
      service.updateWorkOrder('wo-1', {
        maintenance_kind: 'CEBADO',
        valor_json: {
          fecha_programacion: '2026-09-04',
          causa: 'Cebado programado',
          accion: 'Cebado de graseras',
          prevencion: 'Cumplir plan de lubricacion',
        },
      } as any),
    ).rejects.toBeInstanceOf(BadRequestException);
    expect(repos.woRepo.save).not.toHaveBeenCalled();
    expect(repos.programacionRepo.save).not.toHaveBeenCalled();
  });

  it('registrar consumo crea o incrementa la reserva de stock para la OT', async () => {
    repos.woRepo.findOne.mockResolvedValue({
      id: 'wo-1',
      status_workflow: 'IN_PROGRESS',
      is_deleted: false,
    });
    repos.productoRepo.findOne.mockResolvedValue({
      id: 'producto-1',
      codigo: '175',
      nombre: 'PROBADOR DE TIERRA DIGITAL',
      ultimo_costo: 20,
    });
    repos.bodegaRepo.findOne.mockResolvedValue({
      id: 'bodega-1',
      codigo: 'TPBD',
      nombre: 'BODEGA',
    });
    repos.stockRepo.findOne.mockResolvedValue({
      producto_id: 'producto-1',
      bodega_id: 'bodega-1',
      stock_actual: 100,
    });
    repos.kardexRepo.findOne.mockResolvedValue(null);
    repos.consumoRepo.save.mockImplementation(async (value) => ({
      id: 'consumo-1',
      ...value,
    }));
    repos.reservaRepo.findOne.mockResolvedValue(null);
    repos.reservaRepo.save.mockImplementation(async (value) => value);
    jest
      .spyOn(service as any, 'getActiveReservedQuantity')
      .mockResolvedValue(0);

    await service.createConsumo('wo-1', {
      producto_id: 'producto-1',
      bodega_id: 'bodega-1',
      cantidad: 15,
    } as any);

    expect(repos.reservaRepo.save).toHaveBeenCalledWith(
      expect.objectContaining({
        work_order_id: 'wo-1',
        producto_id: 'producto-1',
        bodega_id: 'bodega-1',
        cantidad: 15,
        estado: 'RESERVADO',
      }),
    );
  });

  it('consume stock crítico cuando no existe stock nuevo ni usado', () => {
    const stock = {
      stock_actual: 5,
      stock_nuevo: 0,
      stock_usado: 0,
      stock_critico: 5,
      es_usado: true,
    } as any;

    const condition = (service as any).resolveIssueMaterialCondition(
      stock,
      undefined,
    );
    const total = (service as any).applyIssuedStockByCondition(
      stock,
      2,
      condition,
      'MAT-1',
    );

    expect(condition).toBe('CRITICO');
    expect(total).toBe(3);
    expect(stock).toMatchObject({
      stock_actual: 3,
      stock_nuevo: 0,
      stock_usado: 0,
      stock_critico: 3,
    });
  });

  it('bloquea el stock crítico mientras exista stock nuevo o usado', () => {
    const stock = {
      stock_actual: 6,
      stock_nuevo: 1,
      stock_usado: 0,
      stock_critico: 5,
      es_usado: true,
    } as any;

    expect(() =>
      (service as any).applyIssuedStockByCondition(
        stock,
        1,
        'CRITICO',
        'MAT-1',
      ),
    ).toThrow(ConflictException);
  });
});

describe('KpiMaintenanceService anulacion de ordenes de trabajo', () => {
  let repos: RepoBag;
  let service: KpiMaintenanceService;

  type ManagerStub = {
    manager: any;
    rows: Record<string, any[]>;
    saved: Record<string, any[]>;
    updates: Array<{ entity: string; values: any; where: string[] }>;
  };

  const createManagerStub = (rows: Record<string, any[]>): ManagerStub => {
    let sequence = 0;
    const saved: Record<string, any[]> = {};
    const updates: Array<{ entity: string; values: any; where: string[] }> = [];
    const nameOf = (entity: any) =>
      typeof entity === 'function' ? entity.name : String(entity);

    const manager: any = {
      create: (entity: any, value: any) => ({
        id: value?.id ?? `${nameOf(entity)}-${++sequence}`,
        ...value,
      }),
      save: async (entity: any, value: any) => {
        const key = nameOf(entity);
        const list = Array.isArray(value) ? value : [value];
        saved[key] = [...(saved[key] ?? []), ...list];
        return value;
      },
      find: async (entity: any) => rows[nameOf(entity)] ?? [],
      findOne: async (entity: any) => (rows[nameOf(entity)] ?? [])[0] ?? null,
      createQueryBuilder: () => {
        const state: { entity: string; values: any; where: string[] } = {
          entity: '',
          values: null,
          where: [],
        };
        const builder: any = {
          update: (entity: any) => {
            state.entity = nameOf(entity);
            return builder;
          },
          set: (values: any) => {
            state.values = values;
            return builder;
          },
          where: (clause: string) => {
            state.where.push(clause);
            return builder;
          },
          andWhere: (clause: string) => {
            state.where.push(clause);
            return builder;
          },
          execute: async () => {
            updates.push({ ...state });
            return { affected: 1 };
          },
        };
        return builder;
      },
    };

    return { manager, rows, saved, updates };
  };

  const previousSecurityUrl = process.env.SECURITY_SERVICE_URL;

  beforeEach(() => {
    jest.clearAllMocks();
    // la verificacion de permisos consulta el arbol de menus de kpi-security
    process.env.SECURITY_SERVICE_URL = 'http://127.0.0.1:3015';
    repos = createRepos();
    service = createService(repos, createDataSourceMock());
  });

  afterAll(() => {
    if (previousSecurityUrl === undefined) delete process.env.SECURITY_SERVICE_URL;
    else process.env.SECURITY_SERVICE_URL = previousSecurityUrl;
  });

  it('devuelve el stock al bucket original sin exigir que nuevo y usado esten en cero', () => {
    const stock = {
      stock_actual: 4,
      stock_nuevo: 3,
      stock_usado: 0,
      stock_critico: 1,
      es_usado: false,
    } as any;

    const total = (service as any).applyStockDeltaByConditionForMaintenance(
      stock,
      2,
      'CRITICO',
      'MAT-1',
    );

    expect(total).toBe(6);
    expect(stock).toMatchObject({
      stock_actual: 6,
      stock_nuevo: 3,
      stock_usado: 0,
      stock_critico: 3,
    });
  });

  it('rechaza el reverso cuando el material ya no esta disponible en la bodega', () => {
    const stock = {
      stock_actual: 1,
      stock_nuevo: 1,
      stock_usado: 0,
      stock_critico: 0,
    } as any;

    expect(() =>
      (service as any).applyStockDeltaByConditionForMaintenance(
        stock,
        -5,
        'NUEVO',
        'MAT-1',
      ),
    ).toThrow(ConflictException);
  });

  it('reingresa a bodega los materiales entregados y anula el kardex de egreso', async () => {
    const stock = {
      id: 'stock-1',
      bodega_id: 'bodega-1',
      producto_id: 'producto-1',
      stock_actual: 5,
      stock_nuevo: 5,
      stock_usado: 0,
      stock_critico: 0,
      stock_fisico: 5,
      is_deleted: false,
    };
    const stub = createManagerStub({
      EntregaMaterialEntity: [
        { id: 'entrega-1', work_order_id: 'wo-1', is_deleted: false },
      ],
      EntregaMaterialDetEntity: [
        {
          id: 'entrega-det-1',
          entrega_id: 'entrega-1',
          producto_id: 'producto-1',
          bodega_id: 'bodega-1',
          cantidad: 3,
          costo_unitario: 10,
          condicion_material: 'NUEVO',
        },
      ],
      MovimientoInventarioEntity: [
        {
          id: 'mov-egreso-1',
          work_order_id: 'wo-1',
          tipo_documento: 'EGRESO_BODEGA',
          numero_documento: 'EB-00000004',
          is_deleted: false,
        },
      ],
      ProductoEntity: [
        { id: 'producto-1', codigo: 'MAT', nombre: 'MATERIAL', ultimo_costo: 10 },
      ],
      StockBodegaEntity: [stock],
    });

    const result = await (service as any).reverseWorkOrderMaterialIssues(
      stub.manager,
      { id: 'wo-1', code: 'OT-A00001' },
      { actorName: 'tester', fecha: new Date('2026-08-22T10:00:00Z'), motivo: null },
    );

    expect(result).toMatchObject({ entregas: 1, items: 1, total: 30 });
    expect(stock).toMatchObject({
      stock_actual: 8,
      stock_nuevo: 8,
      stock_fisico: 8,
    });

    const movimiento = stub.saved.MovimientoInventarioEntity?.[0];
    expect(movimiento).toMatchObject({
      tipo_movimiento: 'INGRESO',
      tipo_documento: 'ANULACION_ORDEN_TRABAJO',
      bodega_destino_id: 'bodega-1',
      work_order_id: 'wo-1',
    });
    expect(stub.saved.KardexEntity?.[0]).toMatchObject({
      bodega_id: 'bodega-1',
      producto_id: 'producto-1',
      tipo_movimiento: 'INGRESO',
      entrada_cantidad: 3,
      salida_cantidad: 0,
      saldo_cantidad: 8,
    });

    const annulledEntities = stub.updates.map((item) => item.entity);
    expect(annulledEntities).toEqual(
      expect.arrayContaining([
        'MovimientoInventarioEntity',
        'MovimientoInventarioDetEntity',
        'KardexEntity',
        'EntregaMaterialEntity',
      ]),
    );
    expect(
      stub.updates.find((item) => item.entity === 'KardexEntity')?.values,
    ).toMatchObject({ is_deleted: true, status: 'INACTIVE' });
    expect(result.movimientos).toEqual(
      expect.arrayContaining(['mov-egreso-1']),
    );
  });

  it('retorna desde chatarra a la bodega origen el material desechado por la OT', async () => {
    const scrapStock = {
      id: 'stock-chatarra',
      bodega_id: 'bodega-chatarra',
      producto_id: 'producto-1',
      stock_actual: 4,
      stock_nuevo: 4,
      stock_usado: 0,
      stock_critico: 0,
      stock_fisico: 4,
      is_deleted: false,
    };
    const sourceStock = {
      id: 'stock-origen',
      bodega_id: 'bodega-1',
      producto_id: 'producto-1',
      stock_actual: 1,
      stock_nuevo: 1,
      stock_usado: 0,
      stock_critico: 0,
      stock_fisico: 1,
      is_deleted: false,
    };
    const stub = createManagerStub({
      WorkOrderDesechoEntity: [
        {
          id: 'desecho-1',
          work_order_id: 'wo-1',
          bodega_origen_id: 'bodega-1',
          bodega_chatarra_id: 'bodega-chatarra',
          transferencia_bodega_id: 'transfer-1',
          is_deleted: false,
        },
      ],
      WorkOrderDesechoDetEntity: [
        {
          id: 'desecho-det-1',
          work_order_desecho_id: 'desecho-1',
          producto_id: 'producto-1',
          cantidad: 2,
          costo_unitario: 7,
          transferencia_bodega_det_id: 'transfer-det-1',
          is_deleted: false,
        },
      ],
      TransferenciaBodegaEntity: [
        {
          id: 'transfer-1',
          movimiento_salida_id: 'mov-salida-1',
          movimiento_ingreso_id: 'mov-ingreso-1',
        },
      ],
      TransferenciaBodegaDetEntity: [
        { id: 'transfer-det-1', kardex_salida_id: 'kardex-salida-1' },
      ],
      KardexEntity: [{ id: 'kardex-salida-1', condicion_material: 'NUEVO' }],
      ProductoEntity: [
        { id: 'producto-1', codigo: 'MAT', nombre: 'MATERIAL', ultimo_costo: 7 },
      ],
      MovimientoInventarioEntity: [],
    });
    jest
      .spyOn(service as any, 'getOrCreateStockRowForMaintenance')
      .mockImplementation(async (_manager: any, args: any) =>
        args.bodegaId === 'bodega-chatarra' ? scrapStock : sourceStock,
      );

    const result = await (service as any).reverseWorkOrderScrapTransfers(
      stub.manager,
      { id: 'wo-1', code: 'OT-A00001' },
      { actorName: 'tester', fecha: new Date('2026-08-22T10:00:00Z'), motivo: null },
    );

    expect(result).toMatchObject({ desechos: 1, items: 1, total: 14 });
    expect(scrapStock).toMatchObject({ stock_actual: 2, stock_nuevo: 2 });
    expect(sourceStock).toMatchObject({ stock_actual: 3, stock_nuevo: 3 });
    expect(result.movimientos).toEqual(
      expect.arrayContaining(['mov-salida-1', 'mov-ingreso-1']),
    );
    const kardexDirections = (stub.saved.KardexEntity ?? []).map(
      (row: any) => row.tipo_movimiento,
    );
    expect(kardexDirections).toEqual(['SALIDA', 'INGRESO']);
  });

  it('anula logicamente (activo=false, status=ANULADA) la programacion activa vinculada a la OT sin desvincularla', async () => {
    const programacion = {
      id: 'prog-1',
      work_order_id: 'wo-1',
      is_deleted: false,
      activo: true,
      status: 'ACTIVE',
      payload_json: { work_order_id: 'wo-1', work_order_code: 'OT-A00005' },
    };
    const stub = createManagerStub({
      ProgramacionPlanEntity: [programacion],
    });

    const count = await (service as any).annulProgramacionesForWorkOrder(
      stub.manager,
      'wo-1',
      {
        actorName: 'tester',
        annulledAt: new Date('2026-08-22T10:00:00Z'),
        motivo: 'Anulacion de prueba',
      },
    );

    expect(count).toBe(1);
    expect(stub.saved.ProgramacionPlanEntity).toHaveLength(1);
    expect(stub.saved.ProgramacionPlanEntity[0]).toMatchObject({
      id: 'prog-1',
      work_order_id: 'wo-1',
      activo: false,
      status: 'ANULADA',
    });
    expect(
      (stub.saved.ProgramacionPlanEntity[0].payload_json as any)
        .work_order_annulment,
    ).toMatchObject({
      annulled_by: 'tester',
      motivo: 'Anulacion de prueba',
    });
    // preserva el vinculo original para trazabilidad
    expect(
      (stub.saved.ProgramacionPlanEntity[0].payload_json as any)
        .work_order_id,
    ).toBe('wo-1');
  });

  it('no anula programaciones sin vinculo activo a la OT (ninguna coincidencia)', async () => {
    const stub = createManagerStub({ ProgramacionPlanEntity: [] });

    const count = await (service as any).annulProgramacionesForWorkOrder(
      stub.manager,
      'wo-1',
      { actorName: 'tester', annulledAt: new Date('2026-08-22T10:00:00Z') },
    );

    expect(count).toBe(0);
    expect(stub.saved.ProgramacionPlanEntity).toBeUndefined();
  });

  it('al anular una OT, anula en la misma transaccion la programacion vinculada y reporta el conteo', async () => {
    jest
      .spyOn(service as any, 'assertWorkOrderAnnulmentAllowed')
      .mockResolvedValue(undefined);
    jest
      .spyOn(service as any, 'assertWorkOrderNotBlockedByActiveAnnex')
      .mockResolvedValue(undefined);
    jest
      .spyOn(service as any, 'releaseBlockedWorkOrdersFor')
      .mockResolvedValue(undefined);
    jest
      .spyOn(service as any, 'appendWorkOrderHistory')
      .mockResolvedValue(undefined);
    jest.spyOn(service as any, 'writeSecurityLog').mockResolvedValue(undefined);
    jest
      .spyOn(service as any, 'recalculateAlertasNow')
      .mockResolvedValue(undefined);
    jest
      .spyOn(service as any, 'enrichWorkOrder')
      .mockResolvedValue({ id: 'wo-1', code: 'OT-A00005' });
    jest
      .spyOn(service as any, 'reverseWorkOrderScrapTransfers')
      .mockResolvedValue({
        desechos: 0,
        items: 0,
        total: 0,
        affectedPairs: [],
        movimientos: [],
      });
    jest
      .spyOn(service as any, 'reverseWorkOrderMaterialIssues')
      .mockResolvedValue({
        entregas: 0,
        items: 0,
        total: 0,
        affectedPairs: [],
        movimientos: [],
      });
    jest
      .spyOn(service as any, 'releaseOpenReservationsForWorkOrder')
      .mockResolvedValue(0);
    jest
      .spyOn(service as any, 'annulWorkOrderConsumosWithManager')
      .mockResolvedValue(0);

    const workOrder = {
      id: 'wo-1',
      code: 'OT-A00005',
      status: 'CERRADA',
      status_workflow: 'CLOSED',
      valor_json: {},
      is_deleted: false,
    };
    repos.woRepo.findOne.mockResolvedValue(workOrder);
    repos.stockRepo.find.mockResolvedValue([]);

    const programacion = {
      id: 'prog-1',
      work_order_id: 'wo-1',
      is_deleted: false,
      activo: true,
      status: 'ACTIVE',
      payload_json: { work_order_id: 'wo-1', work_order_code: 'OT-A00005' },
    };
    const stub = createManagerStub({
      WorkOrderEntity: [workOrder],
      ProgramacionPlanEntity: [programacion],
    });
    (service as any).dataSource = {
      transaction: async (cb: any) => cb(stub.manager),
    };

    const result = await service.annulWorkOrder('wo-1', {
      username: 'tester',
    } as any);

    expect(stub.saved.ProgramacionPlanEntity).toHaveLength(1);
    expect(stub.saved.ProgramacionPlanEntity[0]).toMatchObject({
      id: 'prog-1',
      work_order_id: 'wo-1',
      activo: false,
      status: 'ANULADA',
    });
    expect((result as any).data.anulacion.programaciones_anuladas).toBe(1);
  });

  it('permite anular con el permiso de eliminacion del menu aunque el rol no sea administrativo', async () => {
    jest
      .spyOn(service as any, 'getJson')
      .mockResolvedValue([
        {
          nombre: 'Mantenimiento',
          urlComponent: 'mantenimiento',
          permissions: { permitDeleted: false },
          children: [
            {
              nombre: 'Ordenes de trabajo',
              urlComponent: 'work-orders',
              permissions: { permitDeleted: true },
              children: [],
            },
          ],
        },
      ]);

    await expect(
      (service as any).hasWorkOrderAnnulmentPermission({
        userId: '2c2f5e02-2f0a-4c4d-9f2a-9c9d1f3a5b21',
        roleName: 'JEFE DE TALLER',
      }),
    ).resolves.toBe(true);
  });

  it('rechaza la anulacion cuando el usuario no tiene rol ni permiso de eliminacion', async () => {
    jest
      .spyOn(service as any, 'getJson')
      .mockResolvedValue([
        {
          nombre: 'Ordenes de trabajo',
          urlComponent: 'work-orders',
          permissions: { permitDeleted: false },
          children: [],
        },
      ]);

    await expect(
      service.annulWorkOrder('wo-1', {
        userId: '2c2f5e02-2f0a-4c4d-9f2a-9c9d1f3a5b21',
        roleName: 'OPERADOR',
      } as any),
    ).rejects.toBeInstanceOf(ForbiddenException);
    expect(repos.woRepo.findOne).not.toHaveBeenCalled();
  });
});

describe('KpiMaintenanceService programacion automatica de OT de Cebado', () => {
  let repos: RepoBag;
  let service: KpiMaintenanceService;

  beforeEach(() => {
    jest.clearAllMocks();
    repos = createRepos();
    service = createService(repos, createDataSourceMock());
    jest
      .spyOn(service as any, 'appendWorkOrderHistory')
      .mockResolvedValue(undefined);
    jest
      .spyOn(service as any, 'publishInAppNotification')
      .mockResolvedValue(undefined);
    jest.spyOn(service as any, 'writeSecurityLog').mockResolvedValue(undefined);
  });

  it('exige la fecha de programacion en la cabecera de una OT de Cebado', () => {
    expect(() =>
      (service as any).applyCebadoProgramacionDate('CEBADO', {
        causa: 'x',
        accion: 'y',
        prevencion: 'z',
      }),
    ).toThrow(BadRequestException);
  });

  it('no exige la fecha de programacion en OT de otro tipo', () => {
    expect(
      (service as any).applyCebadoProgramacionDate('CORRECTIVO', {}),
    ).toBeNull();
  });

  it('normaliza la fecha dentro del payload de la cabecera', () => {
    const payload: Record<string, unknown> = {
      fecha_programacion: '2026-09-04T05:00:00.000Z',
    };
    const resolved = (service as any).applyCebadoProgramacionDate(
      'CEBADO',
      payload,
    );
    expect(resolved).toBe('2026-09-04');
    expect(payload.fecha_programacion).toBe('2026-09-04');
  });

  it('rechaza una fecha inexistente', () => {
    expect(() =>
      (service as any).applyCebadoProgramacionDate('CEBADO', {
        fecha_programacion: '2026-02-30',
      }),
    ).toThrow(BadRequestException);
  });

  it('crea la programacion en modo CALENDARIO cuando la OT de Cebado aun no tiene una', async () => {
    repos.programacionRepo.find.mockResolvedValue([]);
    repos.programacionRepo.create.mockImplementation((value: any) => ({
      id: 'prog-1',
      ...value,
    }));
    repos.programacionRepo.save.mockImplementation(async (value: any) => value);

    const result = await (service as any).syncCebadoProgramacionFromWorkOrder(
      {
        id: 'wo-1',
        code: 'OT-A00001',
        title: 'Cebado UG 03',
        equipment_id: 'equipo-1',
        plan_id: 'plan-1',
        maintenance_kind: 'CEBADO',
        status_workflow: 'PLANNED',
      },
      '2026-09-04',
      { userId: 'user-1', username: 'operador' },
    );

    expect(result?.action).toBe('created');
    expect(repos.programacionRepo.save).toHaveBeenCalledWith(
      expect.objectContaining({
        equipo_id: 'equipo-1',
        plan_id: 'plan-1',
        work_order_id: 'wo-1',
        proxima_fecha: '2026-09-04',
        // CALENDARIO evita que recalculateProgramacionFields pise la fecha
        modo_programacion: 'CALENDARIO',
        origen_programacion: 'ORDEN_TRABAJO',
        activo: true,
      }),
    );
  });

  it('reprograma la programacion existente cuando cambia la fecha en la cabecera', async () => {
    const existing = {
      id: 'prog-1',
      work_order_id: 'wo-1',
      equipo_id: 'equipo-1',
      plan_id: 'plan-1',
      proxima_fecha: '2026-09-04',
      modo_programacion: 'CALENDARIO',
      origen_programacion: 'ORDEN_TRABAJO',
      payload_json: {},
      activo: true,
      status: 'ACTIVE',
      is_deleted: false,
    };
    repos.programacionRepo.find.mockResolvedValue([existing]);
    repos.programacionRepo.save.mockImplementation(async (value: any) => value);

    const result = await (service as any).syncCebadoProgramacionFromWorkOrder(
      {
        id: 'wo-1',
        code: 'OT-A00001',
        equipment_id: 'equipo-1',
        plan_id: 'plan-1',
        maintenance_kind: 'CEBADO',
        status_workflow: 'PLANNED',
      },
      '2026-09-11',
      null,
    );

    expect(result).toMatchObject({
      action: 'updated',
      previousDate: '2026-09-04',
    });
    expect(existing.proxima_fecha).toBe('2026-09-11');
    expect(repos.programacionRepo.create).not.toHaveBeenCalled();
  });

  it('ignora la sincronizacion en OT que no son de Cebado', async () => {
    const result = await (service as any).syncCebadoProgramacionFromWorkOrder(
      {
        id: 'wo-1',
        equipment_id: 'equipo-1',
        plan_id: 'plan-1',
        maintenance_kind: 'PREVENTIVO',
      },
      '2026-09-04',
      null,
    );
    expect(result).toBeNull();
    expect(repos.programacionRepo.save).not.toHaveBeenCalled();
  });

  it('refleja en la cabecera la fecha reprogramada desde el modulo de programacion', async () => {
    const workOrder = {
      id: 'wo-1',
      code: 'OT-A00001',
      maintenance_kind: 'CEBADO',
      status_workflow: 'PLANNED',
      valor_json: { fecha_programacion: '2026-09-04' },
      is_deleted: false,
    };
    repos.woRepo.findOne.mockResolvedValue(workOrder);
    repos.woRepo.save.mockImplementation(async (value: any) => value);

    const result = await (service as any).mirrorProgramacionDateIntoCebadoWorkOrder(
      { id: 'prog-1', work_order_id: 'wo-1', proxima_fecha: '2026-09-18' },
      null,
    );

    expect(result).toBe('2026-09-18');
    expect(workOrder.valor_json.fecha_programacion).toBe('2026-09-18');
  });
});

describe('KpiMaintenanceService equipos - estado_funcionamiento', () => {
  let repos: RepoBag;
  let service: KpiMaintenanceService;

  beforeEach(() => {
    jest.clearAllMocks();
    repos = createRepos();
    service = createService(repos, createDataSourceMock());
  });

  it('actualiza solo el estado_funcionamiento y conserva el estado_operativo', async () => {
    const equipo = {
      id: 'equipo-1',
      codigo: 'EQ-001',
      estado_operativo: 'OPERATIVO',
      estado_funcionamiento: 'PARADO',
      updated_by: 'anterior',
      is_deleted: false,
    };
    repos.equipoRepo.findOne.mockResolvedValue(equipo);
    repos.equipoRepo.save.mockImplementation(async (value: any) => value);

    const result = await service.updateEquipoEstadoFuncionamiento(
      'equipo-1',
      { estado_funcionamiento: 'FUNCIONAMIENTO' as any },
      { userId: 'u1', username: 'jdoe', displayName: 'John Doe' },
    );

    expect(equipo.estado_funcionamiento).toBe('FUNCIONAMIENTO');
    expect(equipo.estado_operativo).toBe('OPERATIVO');
    expect(equipo.updated_by).toBe('John Doe');
    expect(repos.equipoRepo.save).toHaveBeenCalledWith(equipo);
    expect(result.data.estado_funcionamiento).toBe('FUNCIONAMIENTO');
  });

  it('rechaza valores distintos de FUNCIONAMIENTO/PARADO', async () => {
    repos.equipoRepo.findOne.mockResolvedValue({
      id: 'equipo-1',
      estado_operativo: 'OPERATIVO',
      estado_funcionamiento: 'PARADO',
      is_deleted: false,
    });

    await expect(
      service.updateEquipoEstadoFuncionamiento(
        'equipo-1',
        { estado_funcionamiento: 'ENCENDIDO' as any },
        {},
      ),
    ).rejects.toBeInstanceOf(BadRequestException);
    expect(repos.equipoRepo.save).not.toHaveBeenCalled();
  });

  it('devuelve NotFound cuando el equipo no existe', async () => {
    repos.equipoRepo.findOne.mockResolvedValue(null);

    await expect(
      service.updateEquipoEstadoFuncionamiento(
        'equipo-inexistente',
        { estado_funcionamiento: 'FUNCIONAMIENTO' as any },
        {},
      ),
    ).rejects.toBeInstanceOf(NotFoundException);
  });

  it('crea un equipo con estado_funcionamiento por defecto PARADO', async () => {
    repos.equipoRepo.findOne.mockResolvedValue(null);
    repos.equipoRepo.save.mockImplementation(async (value: any) => ({
      id: 'equipo-nuevo',
      ...value,
    }));
    repos.equipoComponenteRepo.find.mockResolvedValue([]);

    const result = await service.createEquipo({
      nombre: 'Generador',
      equipo_tipo_id: 'tipo-1',
    } as any);

    expect(result.data.estado_funcionamiento).toBe('PARADO');
  });
});
