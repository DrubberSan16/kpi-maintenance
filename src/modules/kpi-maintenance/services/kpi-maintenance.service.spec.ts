import { ConflictException } from '@nestjs/common';
import { DataSource } from 'typeorm';
import { KpiMaintenanceService } from './kpi-maintenance.service';

const createRepo = () => ({
  findOne: jest.fn(),
  find: jest.fn(),
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
  bodegaRepo: createRepo(),
  reservaRepo: createRepo(),
  woTareaRepo: createRepo(),
  woAdjuntoRepo: createRepo(),
});

type RepoBag = ReturnType<typeof createRepos>;

const createDataSourceMock = () =>
  ({
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
    );
  });

  it('actualiza la OT preservando el estado y mezclando valor_json de forma correcta', async () => {
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
      { fromStatus: 'PLANNED' },
    );
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
});
