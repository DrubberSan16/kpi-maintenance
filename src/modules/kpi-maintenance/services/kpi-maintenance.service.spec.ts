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
});
