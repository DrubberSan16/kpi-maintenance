import {
  BadRequestException,
  ConflictException,
  ForbiddenException,
  NotFoundException,
} from '@nestjs/common';
import { DataSource } from 'typeorm';
import { KpiMaintenanceService } from './kpi-maintenance.service';
import {
  EquipoEntity,
  EquipoFuncionamientoHistorialEntity,
  EntregaMaterialEntity,
  EntregaMaterialDetEntity,
} from '../entities/kpi-maintenance.entity';

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
  equipoFuncionamientoHistorialRepo: createRepo(),
  equipoHorometroHistorialRepo: createRepo(),
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
    repos.equipoFuncionamientoHistorialRepo as any,
    repos.equipoHorometroHistorialRepo as any,
    ds,
  );

describe('KpiMaintenanceService alerts', () => {
  let repos: RepoBag;
  let service: KpiMaintenanceService;
  let dataSource: DataSource;

  beforeEach(() => {
    jest.clearAllMocks();
    repos = createRepos();
    dataSource = createDataSourceMock();
    service = createService(repos, dataSource);
  });

  describe('visibilidad de costos de materiales', () => {
    const costItems = [
      {
        producto_label: 'MAT-001 - Filtro',
        bodega_label: 'BOD-01 - Principal',
        cantidad: 2,
        costo_unitario: 15,
        subtotal: 30,
        observacion: null,
      },
    ];

    it.each([
      'GERENTE GENERAL',
      'GERENCIA GENERAL',
      'ADMINISTRADOR',
      'ADMINISTRADOR DEL SISTEMA',
      'ADMIN',
      'SUPER ADMINISTRADOR',
      'SUPERADMINISTRADOR',
      'SUPER_ADMINISTRADOR',
      'SUPER_ADMIN',
      'SUPER ADMIN',
    ])('permite costos al rol %s', (roleName) => {
      expect((service as any).puedeVerCostos(roleName)).toBe(true);
    });

    it.each(['SUPERVISOR', 'OPERADOR', 'BODEGUERO', 'ADMINISTRATIVO', ''])(
      'oculta costos al rol %s',
      (roleName) => {
        expect((service as any).puedeVerCostos(roleName)).toBe(false);
      },
    );

    it('el correo sin permiso conserva cantidades y omite importes', () => {
      const html = (service as any).buildConsumoEmailTableHtml(costItems, false);
      expect(html).toContain('Cantidad');
      expect(html).toContain('2.00');
      expect(html).not.toContain('Costo unit.');
      expect(html).not.toContain('Subtotal');
      expect(html).not.toContain('30.00');
    });

    it('el correo autorizado incluye importes', () => {
      const html = (service as any).buildConsumoEmailTableHtml(costItems, true);
      expect(html).toContain('Costo unit.');
      expect(html).toContain('Subtotal');
      expect(html).toContain('30.00');
    });
  });

  it('incluye el nombre de la marca en el catálogo de equipos', async () => {
    const equipment = {
      id: 'equipment-1',
      codigo: 'EQ-001',
      nombre: 'EXCAVADORA',
      modelo: '320D',
      marca_id: 'brand-1',
      is_deleted: false,
    };
    const queryBuilder = {
      leftJoin: jest.fn().mockReturnThis(),
      where: jest.fn().mockReturnThis(),
      andWhere: jest.fn().mockReturnThis(),
      orderBy: jest.fn().mockReturnThis(),
      skip: jest.fn().mockReturnThis(),
      take: jest.fn().mockReturnThis(),
      getManyAndCount: jest.fn().mockResolvedValue([[equipment], 1]),
    };
    repos.equipoRepo.createQueryBuilder.mockReturnValue(queryBuilder);
    repos.marcaRepo.find.mockResolvedValue([
      { id: 'brand-1', nombre: 'CATERPILLAR', is_deleted: false },
    ]);

    const result = await service.listEquipos({ page: 1, limit: 10 } as any);

    expect(result.data).toEqual([
      expect.objectContaining({
        id: 'equipment-1',
        marca_nombre: 'CATERPILLAR',
      }),
    ]);
    expect(repos.marcaRepo.find).toHaveBeenCalledTimes(1);
  });

  it('arma la identidad del equipo como marca - nombre (modelo)', () => {
    const label = (service as any).buildEquipmentReportLabel({
      marca_nombre: 'MILLER',
      nombre: 'MOTOSOLDADORA MILLER',
      modelo: 'MILLER BLUE 405',
    });
    expect(label).toBe('MILLER - MOTOSOLDADORA MILLER (MILLER BLUE 405)');

    expect(
      (service as any).buildEquipmentReportLabel({
        marca_nombre: 'BOSCH',
        nombre: 'PERFORADORA',
      }),
    ).toBe('BOSCH - PERFORADORA');

    expect(
      (service as any).buildEquipmentReportLabel({
        marca_nombre: null,
        nombre: 'GRUA',
        modelo: 'XCMG 25',
      }),
    ).toBe('Sin marca - GRUA (XCMG 25)');
  });

  it('resume el inventario por rango con nomenclatura clara y corte a las fechas elegidas', async () => {
    (dataSource.query as jest.Mock).mockResolvedValueOnce([
      {
        producto_id: 'product-1',
        bodega_id: 'warehouse-1',
        producto_codigo: 'MAT-001',
        producto_nombre: 'Filtro de aceite',
        producto_descripcion: 'Motor principal',
        ingresos: '12',
        salidas: '5',
        stock_actual: '19',
      },
      {
        producto_id: 'product-1',
        bodega_id: 'warehouse-2',
        producto_codigo: 'MAT-001',
        producto_nombre: 'Filtro de aceite',
        producto_descripcion: 'Motor principal',
        ingresos: '3',
        salidas: '2',
        stock_actual: '8',
      },
    ]);

    const result = await service.getMonthlyInventoryReport(
      { from: '2026-07-15', to: '2026-08-18' } as any,
      null,
    );

    expect(dataSource.query).toHaveBeenCalledWith(
      expect.stringContaining('kardex.fecha <= $2::date'),
      ['2026-07-15', '2026-08-18'],
    );
    expect((result as any).data.filters).toEqual(
      expect.objectContaining({ from: '2026-07-15', to: '2026-08-18' }),
    );
    expect((result as any).data.inventory).toEqual([
      expect.objectContaining({
        material_label: 'MAT-001 - Filtro de aceite (Motor principal)',
        inventario_inicial: 19,
        ingresos: 15,
        salidas: 7,
        inventario_actual: 27,
      }),
    ]);
  });

  it('reemplaza el codigo por la identidad completa en el detalle de la alerta', async () => {
    repos.equipoRepo.find.mockResolvedValue([
      {
        id: 'equipo-1',
        codigo: 'EQ-A00024',
        marca_id: 'brand-1',
        nombre: 'MOTOSOLDADORA MILLER',
        nombre_real: 'MOTOSOLDADORA MILLER BLUE',
        modelo: 'MILLER BLUE 405',
      },
    ]);
    repos.marcaRepo.find.mockResolvedValue([
      { id: 'brand-1', nombre: 'MILLER', is_deleted: false },
    ]);

    const [candidate] = await (service as any).applyEquipmentIdentityToAlertCandidates([
      {
        equipo_id: 'equipo-1',
        tipo_alerta: 'PROGRAMACION_VENCIDA',
        categoria: 'MANTENIMIENTO',
        nivel: 'CRITICAL',
        origen: 'PROGRAMACION',
        detalle: 'EQ-A00024 · MPG 250H · horas faltantes 12.00',
        payload_json: { equipo_id: 'equipo-1', equipo_codigo: 'EQ-A00024' },
      },
    ]);

    expect(candidate.detalle).toBe(
      'MILLER - MOTOSOLDADORA MILLER (MILLER BLUE 405) · MPG 250H · horas faltantes 12.00',
    );
    expect(candidate.payload_json.equipo_label).toBe(
      'MILLER - MOTOSOLDADORA MILLER (MILLER BLUE 405)',
    );
    expect(candidate.payload_json.equipo_nombre_real).toBe(
      'MOTOSOLDADORA MILLER BLUE',
    );
    expect(candidate.payload_json.equipo_modelo).toBe('MILLER BLUE 405');
  });

  it('envia la identidad completa del equipo en el correo de la alerta', async () => {
    const sendMail = jest.fn().mockResolvedValue(undefined);
    jest
      .spyOn(service as any, 'resolveAlertNotificationRecipients')
      .mockResolvedValue([
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
    repos.equipoRepo.findOne.mockResolvedValue({
      id: 'equipo-1',
      codigo: 'EQ-A00024',
      marca_id: 'brand-1',
      nombre: 'MOTOSOLDADORA MILLER',
      nombre_real: 'MOTOSOLDADORA MILLER BLUE',
      modelo: 'MILLER BLUE 405',
    });
    repos.marcaRepo.find.mockResolvedValue([
      { id: 'brand-1', nombre: 'MILLER', is_deleted: false },
    ]);

    await (service as any).sendAlertTriggerEmails({
      id: 'alert-2',
      categoria: 'MANTENIMIENTO',
      nivel: 'CRITICAL',
      estado: 'ABIERTA',
      origen: 'PROGRAMACION',
      tipo_alerta: 'PROGRAMACION_VENCIDA',
      detalle: 'Mantenimiento vencido',
      referencia: 'PROGRAMACION:1',
      referencia_tipo: 'PROGRAMACION',
      fecha_generada: new Date('2026-08-29T10:00:00Z'),
      equipo_id: 'equipo-1',
      payload_json: { equipo_id: 'equipo-1', equipo_codigo: 'EQ-A00024' },
    });

    const identity = 'MILLER - MOTOSOLDADORA MILLER (MILLER BLUE 405)';
    const mail = sendMail.mock.calls[0][0];
    expect(mail.subject).toContain(identity);
    expect(mail.text).toContain(`Equipo: ${identity}`);
    expect(mail.html).toContain(identity);
  });

  it('envía el token interno solo hacia el servicio de seguridad', () => {
    (service as any).securityServiceUrl = 'http://security.local/kpi_security';
    (service as any).internalServiceToken = 'token-interno';

    expect(
      (service as any).buildServiceRequestHeaders(
        'http://security.local/kpi_security/log-transacts',
        true,
      ),
    ).toEqual({
      'Content-Type': 'application/json',
      'X-Internal-Service-Token': 'token-interno',
    });
    expect(
      (service as any).buildServiceRequestHeaders(
        'http://notification.local/notifications/in-app',
        true,
      ),
    ).toEqual({ 'Content-Type': 'application/json' });
  });

  it('registra el incidente tecnico con el metodo, la URL y los datos enviados', async () => {
    (service as any).securityServiceUrl = 'http://security.local/kpi_security';
    const postJson = jest
      .spyOn(service as any, 'postJson')
      .mockResolvedValue(undefined);
    jest
      .spyOn(service as any, 'sendTechnicalIncidentEmail')
      .mockResolvedValue(undefined);

    await (service as any).processTechnicalIncidentReport({
      ticket: 'TCK-1',
      module: 'kpi_maintenance',
      method: 'post',
      request_url: '/kpi_maintenance/equipos',
      status_code: 500,
      user_name: 'jenny.ramirez',
      request_payload: { codigo: 'EQ-A00024', estado_operativo: 'CORRECTIVO' },
      response_message: 'violates check constraint',
    });

    expect(postJson).toHaveBeenCalledWith(
      'http://security.local/kpi_security/log-transacts',
      expect.objectContaining({
        typeLog: 'TECHNICAL_INCIDENT',
        status: 'ERROR',
        requestMethod: 'post',
        requestUrl: '/kpi_maintenance/equipos',
        requestPayload: {
          codigo: 'EQ-A00024',
          estado_operativo: 'CORRECTIVO',
        },
      }),
    );
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

  it('resuelve el actor y agrega gerencia, administradores, superadministradores y todos los supervisores activos', async () => {
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
      {
        id: 'u-super',
        nameUser: 'superadmin',
        nameSurname: 'Super Administrador',
        email: 'superadmin@example.com',
        roleName: 'SUPER ADMINISTRADOR',
        roleNames: ['SUPER ADMINISTRADOR'],
        status: 'ACTIVE',
        isDeleted: false,
      },
      {
        id: 'u-supervisor-2',
        nameUser: 'supervisor2',
        nameSurname: 'Supervisor Dos',
        email: 'supervisor2@example.com',
        roleName: 'SUPERVISOR',
        roleNames: ['SUPERVISOR'],
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
        expect.objectContaining({
          type: 'ADMINISTRATOR',
          email: 'superadmin@example.com',
          userId: 'u-super',
        }),
        expect.objectContaining({
          type: 'SUPERVISOR',
          email: 'supervisor2@example.com',
          userId: 'u-supervisor-2',
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
        nivel: 'INFO',
      }),
    );
  });

  it('bitácora con horómetro retrocedido rechaza la operación sin generar alerta ni correo', async () => {
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

    // ANOMALIA_HOROMETRO quedó fuera del alcance vigente: la validación sigue
    // bloqueando el retroceso, pero ya no crea alerta ni notifica.
    expect(repos.alertaRepo.save).not.toHaveBeenCalled();
    expect(dispatchSpy).not.toHaveBeenCalled();
  });

  describe('alertas de programación por atraso de la OT', () => {
    const HOUR = 60 * 60 * 1000;

    const buildProgramacionRow = (overrides: Record<string, unknown> = {}) => ({
      id: 'prog-1',
      equipo_id: 'equipo-1',
      equipo_codigo: 'EQ-01',
      plan_id: 'plan-1',
      plan_nombre: 'Plan 325',
      work_order_id: 'wo-1',
      estado_programacion: 'VENCIDA',
      horas_restantes: null,
      dias_restantes: null,
      payload_json: {},
      ...overrides,
    });

    const setup = (progRow: any, workOrder: any | null) => {
      repos.programacionRepo.find.mockResolvedValue([progRow]);
      jest
        .spyOn(service as any, 'recalculateProgramacionFields')
        .mockResolvedValue(progRow);
      repos.woRepo.find.mockResolvedValue(workOrder ? [workOrder] : []);
    };

    it('no alerta si la OT planificada aún no acumula 24 h de atraso', async () => {
      setup(buildProgramacionRow(), {
        id: 'wo-1',
        code: 'OT-1',
        status_workflow: 'PLANNED',
        scheduled_end: new Date(Date.now() - 3 * HOUR),
      });

      await expect(
        (service as any).buildProgramacionAlertCandidates(),
      ).resolves.toHaveLength(0);
    });

    it('alerta cuando la OT planificada supera las 24 h de atraso', async () => {
      setup(buildProgramacionRow(), {
        id: 'wo-1',
        code: 'OT-1',
        status_workflow: 'PLANNED',
        scheduled_end: new Date(Date.now() - 30 * HOUR),
      });

      const candidates = await (
        service as any
      ).buildProgramacionAlertCandidates();

      expect(candidates).toHaveLength(1);
      expect(candidates[0]).toEqual(
        expect.objectContaining({
          tipo_alerta: 'MANTENIMIENTO_VENCIDO',
          origen: 'PROGRAMACION',
        }),
      );
      expect(candidates[0].payload_json.overdue_hours).toBeGreaterThanOrEqual(
        24,
      );
    });

    it('deja de alertar cuando la OT ya arrancó, aunque esté atrasada', async () => {
      setup(buildProgramacionRow(), {
        id: 'wo-1',
        code: 'OT-1',
        status_workflow: 'IN_PROGRESS',
        scheduled_end: new Date(Date.now() - 96 * HOUR),
      });

      await expect(
        (service as any).buildProgramacionAlertCandidates(),
      ).resolves.toHaveLength(0);
    });

    it('no alerta una programación sin OT asignada', async () => {
      setup(buildProgramacionRow({ work_order_id: null }), null);

      await expect(
        (service as any).buildProgramacionAlertCandidates(),
      ).resolves.toHaveLength(0);
    });
  });

  describe('avisos del cronograma semanal', () => {
    it('genera un evento a realizar para la actividad semanal sin OT', async () => {
      repos.cronogramaSemanalDetRepo.find.mockResolvedValue([
        {
          id: 'det-1',
          cronograma_id: 'crono-1',
          dia_semana: 'LUNES',
          fecha_actividad: '2026-01-05',
          hora_inicio: '08:00',
          hora_fin: '10:00',
          actividad: 'Cambio de filtros',
          equipo_codigo: 'EQ-01',
          work_order_id: null,
          is_deleted: false,
        },
      ]);
      repos.cronogramaSemanalRepo.find.mockResolvedValue([
        { id: 'crono-1', codigo: 'CRON-01', resumen: 'Semana 1', is_deleted: false },
      ]);
      repos.equipoRepo.find.mockResolvedValue([
        { id: 'equipo-1', codigo: 'EQ-01', nombre: 'Excavadora', is_deleted: false },
      ]);

      const candidates = await (
        service as any
      ).buildCronogramaSemanalAlertCandidates();

      expect(candidates).toHaveLength(1);
      expect(candidates[0]).toEqual(
        expect.objectContaining({
          tipo_alerta: 'EVENTO_A_REALIZAR',
          equipo_id: 'equipo-1',
          referencia: 'CRONOGRAMA_SEMANAL:det-1',
        }),
      );
      expect(candidates[0].payload_json.actividad).toBe('Cambio de filtros');
      expect(candidates[0].payload_json.cronograma_codigo).toBe('CRON-01');
    });

    it('omite la actividad semanal que ya tiene OT vinculada', async () => {
      repos.cronogramaSemanalDetRepo.find.mockResolvedValue([
        {
          id: 'det-1',
          cronograma_id: 'crono-1',
          fecha_actividad: '2026-01-05',
          actividad: 'Cambio de filtros',
          equipo_codigo: 'EQ-01',
          work_order_id: 'wo-9',
          is_deleted: false,
        },
      ]);

      await expect(
        (service as any).buildCronogramaSemanalAlertCandidates(),
      ).resolves.toHaveLength(0);
    });
  });

  describe('correos de orden de trabajo restringidos a cebado', () => {
    it('solo notifica por correo las ordenes de cebado', () => {
      const notifica = (kind: string) =>
        (service as any).notificaPorCorreo({ maintenance_kind: kind });

      expect(notifica('CEBADO')).toBe(true);
      expect(notifica('CORRECTIVO')).toBe(false);
      expect(notifica('PREVENTIVO')).toBe(false);
      expect(notifica('PREDICTIVO')).toBe(false);
    });

    it('no avisa a supervision cuando la OT en revision no es de cebado', async () => {
      const enviar = jest
        .spyOn(service as any, 'sendWorkOrderReviewEmails')
        .mockResolvedValue({ sent: 0, failed: 0, recipients: 0 });

      const resultado = await (service as any).handleWorkOrderReview({
        id: 'wo-1',
        code: 'OT-1',
        maintenance_kind: 'CORRECTIVO',
      });

      expect(resultado).toBeNull();
      expect(enviar).not.toHaveBeenCalled();
    });

    it('el semaforo de aceite se mide sobre lo consumido por la orden', () => {
      const nivel = (galones: number) =>
        (service as any).resolveConsumoAceiteSemaforo(galones).nivel;

      expect(nivel(0)).toBe('VERDE');
      expect(nivel(5)).toBe('VERDE');
      expect(nivel(5.1)).toBe('AMARILLO');
      expect(nivel(9.9)).toBe('AMARILLO');
      expect(nivel(10)).toBe('ROJO');
      expect(nivel(27)).toBe('ROJO');
    });
  });

  describe('estado En revisión de la orden de trabajo', () => {
    it('reconoce el estado y lo mantiene editable', () => {
      expect((service as any).normalizeWorkflowStatus('EN_REVISION')).toBe(
        'REVIEW',
      );
      expect((service as any).normalizeWorkflowStatus('REVIEW')).toBe('REVIEW');
      expect((service as any).isEditableWorkOrderStatus('REVIEW')).toBe(true);
      expect((service as any).isEditableWorkOrderStatus('CLOSED')).toBe(false);
    });

    it('permite registrar consumos con la OT en revisión', () => {
      expect(() =>
        (service as any).assertWorkOrderAllowsMaterialReservation({
          status_workflow: 'REVIEW',
          valor_json: {},
          status: 'ACTIVE',
        }),
      ).not.toThrow();
    });

    it('bloquea el registro de consumos con la OT finalizada', () => {
      expect(() =>
        (service as any).assertWorkOrderAllowsMaterialReservation({
          status_workflow: 'CLOSED',
          valor_json: {},
          status: 'ACTIVE',
        }),
      ).toThrow();
    });
  });

  it('la bitácora conserva su registro pero no sobrescribe el horómetro del equipo', async () => {
    repos.equipoRepo.findOne.mockResolvedValue({
      id: 'equipo-1',
      horometro_actual: 100,
      is_deleted: false,
    });
    repos.bitacoraRepo.findOne.mockResolvedValue({ horometro: 100 });

    await service.createBitacora('equipo-1', {
      fecha: '2026-08-28',
      horometro: 110,
    } as any);

    expect(repos.bitacoraRepo.save).toHaveBeenCalled();
    expect(repos.equipoRepo.save).not.toHaveBeenCalled();
  });

  it('una lectura genérica no sobrescribe el horómetro manual del equipo', async () => {
    repos.equipoRepo.findOne.mockResolvedValue({
      id: 'equipo-1',
      horometro_actual: 100,
      is_deleted: false,
    });

    await service.createLectura({
      equipo_id: 'equipo-1',
      tipo: 'HOROMETRO',
      valor: 120,
    } as any);

    expect(repos.lecturaRepo.save).toHaveBeenCalled();
    expect(repos.equipoRepo.save).not.toHaveBeenCalled();
  });

  it('la OT conserva el horómetro editable, su valor anterior y las horas-hombre', () => {
    const result = (service as any).buildWorkOrderHorometerPayload(
      { horometro_actual: 999, horas_a_realizar: 25 },
      { horometro_actual: 150 },
      null,
    );

    expect(result).toEqual(
      expect.objectContaining({
        horometro_actual: 999,
        horometro_anterior: 150,
        horas_a_realizar: 25,
        horas_plantilla: 25,
      }),
    );
    expect(result).not.toHaveProperty('horometro_proyectado');
    expect(result).not.toHaveProperty('horometro_equipo_referencia');
  });

  it('sincroniza el horómetro editado en la OT con el equipo y su historial', async () => {
    const equipment = {
      id: 'equipo-1',
      codigo: 'EQ-1',
      horometro_actual: 125,
      is_deleted: false,
    };
    repos.equipoRepo.findOne.mockResolvedValue(equipment);

    const result = await (service as any).syncEquipmentHorometerFromWorkOrder(
      {
        id: 'wo-1',
        code: 'OT-A00001',
        equipment_id: 'equipo-1',
        updated_by: 'supervisor',
        valor_json: { horometro_anterior: 125, horometro_actual: 90 },
      },
      { horometro_anterior: 100, horometro_actual: 125 },
      { username: 'supervisor' },
    );

    expect(repos.equipoRepo.save).toHaveBeenCalledWith(
      expect.objectContaining({ horometro_actual: 90 }),
    );
    expect(repos.equipoHorometroHistorialRepo.save).toHaveBeenCalledWith(
      expect.objectContaining({
        equipo_id: 'equipo-1',
        horometro_anterior: 125,
        horometro_nuevo: 90,
        fuente: 'ORDEN_TRABAJO',
        observacion: expect.stringContaining('OT-A00001'),
      }),
    );
    expect(result).toEqual(
      expect.objectContaining({
        equipmentUpdated: true,
        notes: expect.arrayContaining([
          expect.stringContaining('Horómetro del equipo actualizado desde la OT'),
        ]),
      }),
    );
  });

  it('la actualización manual del equipo registra historial interno', async () => {
    const current = {
      id: 'equipo-1',
      codigo: 'EQ-1',
      horometro_actual: 100,
      es_servicio: false,
      is_deleted: false,
    };
    repos.equipoRepo.findOne.mockResolvedValue(current);
    const equipmentTxRepo = createRepo();
    equipmentTxRepo.findOne.mockResolvedValue({ ...current });
    const historyTxRepo = createRepo();
    (dataSource as any).transaction = jest.fn(async (callback: any) =>
      callback({
        getRepository: (entity: unknown) =>
          entity === EquipoEntity ? equipmentTxRepo : historyTxRepo,
      }),
    );
    jest
      .spyOn(service, 'triggerAlertRecalculation')
      .mockResolvedValue({ data: { accepted: true }, message: 'OK' } as any);

    await service.updateEquipo(
      'equipo-1',
      { horometro_actual: 125 } as any,
      { userId: 'a5de5046-606a-4ea4-b934-6ac4e28db551', username: 'supervisor' } as any,
    );

    expect(equipmentTxRepo.save).toHaveBeenCalledWith(
      expect.objectContaining({ horometro_actual: 125 }),
    );
    expect(historyTxRepo.save).toHaveBeenCalledWith(
      expect.objectContaining({
        equipo_id: 'equipo-1',
        horometro_anterior: 100,
        horometro_nuevo: 125,
        fuente: 'MANUAL_EQUIPOS',
      }),
    );
  });

  it('permite corregir el horómetro hacia abajo y usa la lectura menor como nueva base', async () => {
    const current = {
      id: 'equipo-1',
      codigo: 'EQ-1',
      horometro_actual: 125,
      es_servicio: false,
      is_deleted: false,
    };
    repos.equipoRepo.findOne.mockResolvedValue(current);
    const equipmentTxRepo = createRepo();
    equipmentTxRepo.findOne.mockResolvedValue({ ...current });
    const historyTxRepo = createRepo();
    (dataSource as any).transaction = jest.fn(async (callback: any) =>
      callback({
        getRepository: (entity: unknown) =>
          entity === EquipoEntity ? equipmentTxRepo : historyTxRepo,
      }),
    );
    jest
      .spyOn(service, 'triggerAlertRecalculation')
      .mockResolvedValue({ data: { accepted: true }, message: 'OK' } as any);

    await service.updateEquipoHorometro(
      'equipo-1',
      { horometro_actual: 90 },
      { username: 'supervisor' },
    );

    expect(equipmentTxRepo.save).toHaveBeenCalledWith(
      expect.objectContaining({ horometro_actual: 90 }),
    );
    expect(historyTxRepo.save).toHaveBeenCalledWith(
      expect.objectContaining({
        equipo_id: 'equipo-1',
        horometro_anterior: 90,
        horometro_nuevo: 90,
        fuente: 'MANUAL_EQUIPOS',
        observacion: expect.stringContaining('Correccion manual descendente'),
      }),
    );
    expect(service.triggerAlertRecalculation).toHaveBeenCalledWith(
      'horometro-manual',
    );
  });

  it('el endpoint dedicado de horómetro reutiliza la actualización manual auditada', async () => {
    const updateSpy = jest
      .spyOn(service, 'updateEquipo')
      .mockResolvedValue({ data: { horometro_actual: 140 }, message: 'OK' } as any);
    const actor = { userId: 'u-1', username: 'supervisor' } as any;

    await service.updateEquipoHorometro(
      'equipo-1',
      { horometro_actual: 140 },
      actor,
    );

    expect(updateSpy).toHaveBeenCalledWith(
      'equipo-1',
      { horometro_actual: 140 },
      actor,
    );
  });

  it('el recordatorio diario se envía únicamente a usuarios supervisores activos', async () => {
    const sendMail = jest.fn().mockResolvedValue(undefined);
    jest.spyOn(service as any, 'fetchSecurityUsers').mockResolvedValue([
      {
        id: 'u-1',
        nameUser: 'supervisor',
        email: 'supervisor@example.com',
        roleName: 'Supervisor',
        roleNames: ['Supervisor'],
        status: 'ACTIVE',
        isDeleted: false,
        sucursalIds: [],
        allSucursales: true,
      },
      {
        id: 'u-2',
        nameUser: 'operador',
        email: 'operador@example.com',
        roleName: 'Operador',
        roleNames: ['Operador'],
        status: 'ACTIVE',
        isDeleted: false,
        sucursalIds: [],
        allSucursales: true,
      },
    ]);
    jest
      .spyOn(service as any, 'getAlertMailTransporter')
      .mockResolvedValue({ sendMail });
    (dataSource.query as jest.Mock).mockResolvedValue([
      { total_equipment: 23, updated_today: 2 },
    ]);
    repos.eventoProcesoRepo.findOne.mockResolvedValue(null);

    await (service as any).sendDailyHorometerReminder(
      '2026-08-28',
      'test',
    );

    expect(sendMail).toHaveBeenCalledTimes(1);
    expect(sendMail).toHaveBeenCalledWith(
      expect.objectContaining({ to: 'supervisor@example.com' }),
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

  it('mantiene abierta una alerta con OT planificada y la pasa a en proceso solo cuando la OT inicia', () => {
    expect(
      (service as any).resolveAlertStateFromLinkedWorkOrders(
        [{ status_workflow: 'PLANNED' }],
        'ABIERTA',
      ),
    ).toBe('ABIERTA');
    expect(
      (service as any).resolveAlertStateFromLinkedWorkOrders(
        [{ status_workflow: 'IN_PROGRESS' }],
        'ABIERTA',
      ),
    ).toBe('EN_PROCESO');
    expect(
      (service as any).resolveAlertStateFromLinkedWorkOrders(
        [{ status_workflow: 'CLOSED' }],
        'ABIERTA',
      ),
    ).toBe('CERRADA');
  });

  it('consolida materiales consecutivos de una OT en un solo correo tabular', async () => {
    jest.useFakeTimers();
    const sendSpy = jest
      .spyOn(service as any, 'sendInventoryReservationEmails')
      .mockResolvedValue({ sent: 1, failed: 0, recipients: 1 });
    const workOrder = { id: 'wo-1', code: 'OT-A00037' } as any;

    (service as any).queueInventoryReservationEmail(
      workOrder,
      [{ producto_id: 'p-1', bodega_id: 'b-1', cantidad: 2 }],
      { username: 'supervisor' },
    );
    (service as any).queueInventoryReservationEmail(
      workOrder,
      [
        { producto_id: 'p-1', bodega_id: 'b-1', cantidad: 3 },
        { producto_id: 'p-2', bodega_id: 'b-1', cantidad: 1 },
      ],
      { username: 'supervisor' },
    );

    jest.advanceTimersByTime(45_000);
    await Promise.resolve();

    expect(sendSpy).toHaveBeenCalledTimes(1);
    expect(sendSpy).toHaveBeenCalledWith(
      workOrder,
      expect.arrayContaining([
        expect.objectContaining({ producto_id: 'p-1', cantidad: 5 }),
        expect.objectContaining({ producto_id: 'p-2', cantidad: 1 }),
      ]),
      expect.objectContaining({ username: 'supervisor' }),
    );
    service.onModuleDestroy();
    jest.useRealTimers();
  });

  it('excluye programaciones de Cebado de las alertas de mantenimiento', async () => {
    repos.programacionRepo.find.mockResolvedValue([{ id: 'prog-cebado' }]);
    jest
      .spyOn(service as any, 'recalculateProgramacionFields')
      .mockResolvedValue({
        id: 'prog-cebado',
        equipo_id: 'equipo-1',
        plan_tipo: 'CEBADO',
        estado_programacion: 'VENCIDA',
        payload_json: {},
      });

    await expect(
      (service as any).buildProgramacionAlertCandidates(),
    ).resolves.toEqual([]);
  });

  it('cierra como informativa una alerta vinculada a una OT de Cebado', async () => {
    repos.alertaRepo.findOne.mockResolvedValue({
      id: 'alert-cebado',
      equipo_id: 'equipo-1',
      nivel: 'CRITICAL',
      estado: 'ABIERTA',
      payload_json: {},
      is_deleted: false,
    });

    await (service as any).syncAlertWorkOrderLink('alert-cebado', {
      id: 'wo-cebado',
      code: 'OT-CEBADO',
      title: 'Cebado',
      equipment_id: 'equipo-1',
      maintenance_kind: 'CEBADO',
      status_workflow: 'PLANNED',
    });

    expect(repos.alertaRepo.save).toHaveBeenCalledWith(
      expect.objectContaining({
        id: 'alert-cebado',
        estado: 'CERRADA',
        nivel: 'INFO',
        resolved_at: expect.any(Date),
        payload_json: expect.objectContaining({
          exclusion_reason: 'CEBADO_NO_GENERA_ALERTA',
        }),
      }),
    );
  });

  it('no reabre una alerta histórica duplicada cuando cambia su OT', async () => {
    repos.alertaRepo.find.mockResolvedValue([
      {
        id: 'alert-duplicada',
        equipo_id: 'equipo-1',
        nivel: 'INFO',
        estado: 'CERRADA',
        origen: 'WORK_ORDER',
        work_order_id: 'wo-1',
        payload_json: {
          logic_migration: {
            duplicate_of_alert_id: 'alert-programacion',
          },
          work_orders: [
            {
              id: 'wo-1',
              code: 'OT-A00040',
              status_workflow: 'PLANNED',
            },
          ],
        },
        is_deleted: false,
      },
    ]);

    await (service as any).syncAlertsForWorkOrder({
      id: 'wo-1',
      code: 'OT-A00040',
      title: 'Mantenimiento preventivo',
      equipment_id: 'equipo-1',
      maintenance_kind: 'PREVENTIVO',
      status_workflow: 'IN_PROGRESS',
    });

    expect(repos.alertaRepo.save).toHaveBeenCalledWith(
      expect.objectContaining({
        id: 'alert-duplicada',
        estado: 'CERRADA',
        nivel: 'INFO',
        payload_json: expect.objectContaining({
          exclusion_reason: 'DUPLICADO_ALERTA_PROGRAMACION',
        }),
      }),
    );
  });

  it('enlaza una alerta crítica con la OT activa del equipo y baja su nivel', async () => {
    repos.alertaRepo.find.mockResolvedValue([]);
    repos.woRepo.find.mockResolvedValue([
      {
        id: 'wo-1',
        code: 'OT-A00037',
        title: 'Mantenimiento preventivo',
        equipment_id: 'equipo-1',
        maintenance_kind: 'PREVENTIVO',
        status_workflow: 'PLANNED',
        created_at: new Date('2026-08-28T08:00:00Z'),
        updated_at: new Date('2026-08-28T08:00:00Z'),
        is_deleted: false,
      },
    ]);
    repos.alertaRepo.create.mockImplementation((value) => value);
    repos.alertaRepo.save.mockImplementation(async (value) => ({
      id: 'alert-1',
      ...value,
    }));
    jest
      .spyOn(service as any, 'dispatchAlertTriggeredNotifications')
      .mockResolvedValue(undefined);

    await (service as any).syncAlertCandidates([
      {
        equipo_id: 'equipo-1',
        tipo_alerta: 'MANTENIMIENTO_PROXIMO',
        categoria: 'MANTENIMIENTO',
        nivel: 'CRITICAL',
        origen: 'PROGRAMACION',
        referencia_tipo: 'PROGRAMACION',
        referencia: 'PROGRAMACION:prog-1',
        detalle: 'Mantenimiento próximo',
        payload_json: { programacion_id: 'prog-1' },
      },
    ]);

    expect(repos.alertaRepo.save).toHaveBeenCalledWith(
      expect.objectContaining({
        nivel: 'WARNING',
        estado: 'ABIERTA',
        work_order_id: 'wo-1',
        payload_json: expect.objectContaining({
          work_orders: [
            expect.objectContaining({
              id: 'wo-1',
              status_workflow: 'PLANNED',
            }),
          ],
        }),
      }),
    );
  });

  it('cierra la alerta de OT duplicada al generar la alerta de programación', async () => {
    repos.alertaRepo.find.mockResolvedValue([
      {
        id: 'alert-work-order',
        equipo_id: 'equipo-1',
        tipo_alerta: 'MANTENIMIENTO_PROXIMO',
        categoria: 'MANTENIMIENTO',
        nivel: 'CRITICAL',
        origen: 'WORK_ORDER',
        referencia_tipo: 'WORK_ORDER',
        referencia: 'WORK_ORDER:wo-1',
        estado: 'ABIERTA',
        work_order_id: 'wo-1',
        payload_json: {},
        is_deleted: false,
      },
    ]);
    repos.woRepo.find.mockResolvedValue([
      {
        id: 'wo-1',
        code: 'OT-A00040',
        title: 'Mantenimiento preventivo',
        equipment_id: 'equipo-1',
        maintenance_kind: 'PREVENTIVO',
        status_workflow: 'IN_PROGRESS',
        created_at: new Date('2026-08-28T08:00:00Z'),
        updated_at: new Date('2026-08-28T09:00:00Z'),
        is_deleted: false,
      },
    ]);
    repos.alertaRepo.create.mockImplementation((value) => value);
    repos.alertaRepo.save.mockImplementation(async (value) => value);
    jest
      .spyOn(service as any, 'dispatchAlertTriggeredNotifications')
      .mockResolvedValue(undefined);

    const stats = await (service as any).syncAlertCandidates([
      {
        equipo_id: 'equipo-1',
        tipo_alerta: 'MANTENIMIENTO_PROXIMO',
        categoria: 'MANTENIMIENTO',
        nivel: 'CRITICAL',
        origen: 'PROGRAMACION',
        referencia_tipo: 'PROGRAMACION',
        referencia: 'PROGRAMACION:prog-1',
        detalle: 'Mantenimiento próximo',
        payload_json: { programacion_id: 'prog-1' },
      },
    ]);

    expect(stats).toEqual(expect.objectContaining({ created: 1, resolved: 1 }));
    expect(repos.alertaRepo.save).toHaveBeenCalledWith(
      expect.objectContaining({
        id: 'alert-work-order',
        estado: 'CERRADA',
        nivel: 'INFO',
        payload_json: expect.objectContaining({
          exclusion_reason: 'DUPLICADO_ALERTA_PROGRAMACION',
          duplicate_of_alert_id: 'PROGRAMACION:prog-1',
        }),
      }),
    );
    expect(repos.alertaRepo.save).toHaveBeenCalledWith(
      expect.objectContaining({
        origen: 'PROGRAMACION',
        estado: 'EN_PROCESO',
        nivel: 'WARNING',
        work_order_id: 'wo-1',
      }),
    );
  });

  describe('executeAlertManually', () => {
    it('rechaza la ejecucion manual antes de tocar el repositorio o el correo cuando el actor no es Super Administrador', async () => {
      await expect(
        service.executeAlertManually('alert-1', {
          roleName: 'SUPERVISOR',
        } as any),
      ).rejects.toBeInstanceOf(ForbiddenException);

      expect(repos.alertaRepo.findOne).not.toHaveBeenCalled();
    });

    it('lanza NotFound cuando la alerta no existe', async () => {
      repos.alertaRepo.findOne.mockResolvedValue(null);

      await expect(
        service.executeAlertManually('alert-missing', {
          roleName: 'SUPER ADMINISTRADOR',
        } as any),
      ).rejects.toBeInstanceOf(NotFoundException);
    });

    it('rechaza alertas sin equipo asociado', async () => {
      repos.alertaRepo.findOne.mockResolvedValue({
        id: 'alert-1',
        equipo_id: null,
        estado: 'ABIERTA',
        payload_json: {},
        is_deleted: false,
      });

      await expect(
        service.executeAlertManually('alert-1', {
          roleName: 'SUPER ADMINISTRADOR',
        } as any),
      ).rejects.toBeInstanceOf(BadRequestException);
    });

    it('rechaza alertas cerradas o resueltas', async () => {
      repos.alertaRepo.findOne.mockResolvedValue({
        id: 'alert-1',
        equipo_id: 'equipo-1',
        estado: 'CERRADA',
        payload_json: {},
        is_deleted: false,
      });

      await expect(
        service.executeAlertManually('alert-1', {
          roleName: 'SUPER ADMINISTRADOR',
        } as any),
      ).rejects.toBeInstanceOf(BadRequestException);
    });

    it('ejecuta la alerta activa de un equipo para un alias de Super Administrador, preserva el payload existente, anexa metadata acotada y audita el resultado', async () => {
      const alert = {
        id: 'alert-1',
        equipo_id: 'equipo-1',
        estado: 'EN_PROCESO',
        payload_json: { existing_key: 'valor-previo' },
        is_deleted: false,
      };
      repos.alertaRepo.findOne.mockResolvedValue(alert);
      repos.alertaRepo.save.mockImplementation(async (value: any) => value);
      const sendSpy = jest
        .spyOn(service as any, 'sendAlertTriggerEmails')
        .mockResolvedValue({
          recipients: [
            {
              type: 'TRANSACTION_OWNER',
              email: 'operador@example.com',
              userId: 'u-1',
              username: 'operador',
            },
          ],
          userIds: ['u-1'],
          recipientTokens: ['u-1'],
          sent: ['operador@example.com'],
          failed: [],
          skippedReason: null,
        });
      const auditSpy = jest
        .spyOn(service as any, 'writeSecurityLog')
        .mockResolvedValue(undefined);

      const result = await service.executeAlertManually(
        'alert-1',
        {
          userId: 'super-1',
          username: 'super',
          displayName: 'Super Administrador',
          roleName: 'SUPERADMINISTRADOR',
        } as any,
        'dashboard-super-admin',
      );

      expect(sendSpy).toHaveBeenCalledWith(alert);
      expect(repos.alertaRepo.save).toHaveBeenCalledWith(
        expect.objectContaining({
          id: 'alert-1',
          payload_json: expect.objectContaining({
            existing_key: 'valor-previo',
            manual_alert_notifications: expect.arrayContaining([
              expect.objectContaining({
                source: 'dashboard-super-admin',
                actor_user_id: 'super-1',
                email_sent: ['operador@example.com'],
                email_failed: [],
              }),
            ]),
          }),
        }),
      );
      expect(
        (alert.payload_json as any).manual_alert_notifications,
      ).toHaveLength(1);
      expect(auditSpy).toHaveBeenCalled();
      expect(result.data).toMatchObject({
        alert_id: 'alert-1',
        equipo_id: 'equipo-1',
        sent_count: 1,
        failed_count: 0,
        email_sent: ['operador@example.com'],
        email_failed: [],
      });
    });

    it('acota el historial de ejecuciones manuales a las 20 mas recientes', async () => {
      const existingHistory = Array.from({ length: 20 }, (_, index) => ({
        executed_at: `2026-01-${String(index + 1).padStart(2, '0')}T00:00:00.000Z`,
        source: 'dashboard-super-admin',
      }));
      const alert = {
        id: 'alert-1',
        equipo_id: 'equipo-1',
        estado: 'ABIERTA',
        payload_json: { manual_alert_notifications: existingHistory },
        is_deleted: false,
      };
      repos.alertaRepo.findOne.mockResolvedValue(alert);
      repos.alertaRepo.save.mockImplementation(async (value: any) => value);
      jest.spyOn(service as any, 'sendAlertTriggerEmails').mockResolvedValue({
        recipients: [],
        userIds: [],
        recipientTokens: [],
        sent: [],
        failed: [],
        skippedReason: 'No se encontraron destinatarios para la alerta.',
      });
      jest
        .spyOn(service as any, 'writeSecurityLog')
        .mockResolvedValue(undefined);

      await service.executeAlertManually('alert-1', {
        roleName: 'SUPER ADMIN',
      } as any);

      const savedPayload = (
        repos.alertaRepo.save as jest.Mock
      ).mock.calls[0][0].payload_json;
      expect(savedPayload.manual_alert_notifications).toHaveLength(20);
    });

    it('devuelve una respuesta explicita de cero envios cuando el SMTP esta omitido o no hay destinatarios, sin declarar un falso exito', async () => {
      const alert = {
        id: 'alert-1',
        equipo_id: 'equipo-1',
        estado: 'ABIERTA',
        payload_json: {},
        is_deleted: false,
      };
      repos.alertaRepo.findOne.mockResolvedValue(alert);
      repos.alertaRepo.save.mockImplementation(async (value: any) => value);
      jest.spyOn(service as any, 'sendAlertTriggerEmails').mockResolvedValue({
        recipients: [],
        userIds: [],
        recipientTokens: [],
        sent: [],
        failed: [],
        skippedReason: 'SMTP no configurado para envio de alertas.',
      });
      jest
        .spyOn(service as any, 'writeSecurityLog')
        .mockResolvedValue(undefined);

      const result = await service.executeAlertManually('alert-1', {
        roleName: 'SUPER ADMINISTRADOR',
      } as any);

      expect(result.data.sent_count).toBe(0);
      expect(result.data.skipped_reason).toBe(
        'SMTP no configurado para envio de alertas.',
      );
      expect(result.message).not.toMatch(/correo enviado/);
    });
  });

  describe('sendMissingEquipmentAlertsToSupervisors', () => {
    it('rechaza el reenvio cuando el actor no es Super Administrador', async () => {
      await expect(
        service.sendMissingEquipmentAlertsToSupervisors({
          roleName: 'SUPERVISOR',
        } as any),
      ).rejects.toBeInstanceOf(ForbiddenException);

      expect(repos.alertaRepo.find).not.toHaveBeenCalled();
    });

    it('envia solo a los supervisores faltantes de alertas activas de equipo y registra la auditoria', async () => {
      const activeAlert = {
        id: 'alert-active',
        equipo_id: 'equipo-1',
        estado: 'ABIERTA',
        fecha_generada: new Date('2026-08-29T08:00:00Z'),
        payload_json: {
          alert_notification: {
            email_sent: ['supervisor1@example.com', 'admin@example.com'],
          },
        },
        is_deleted: false,
      };
      repos.alertaRepo.find.mockResolvedValue([
        activeAlert,
        {
          id: 'alert-closed',
          equipo_id: 'equipo-2',
          estado: 'CERRADA',
          fecha_generada: new Date('2026-08-28T08:00:00Z'),
          payload_json: {},
          is_deleted: false,
        },
      ]);
      repos.alertaRepo.save.mockImplementation(async (value: any) => value);
      jest
        .spyOn(service as any, 'resolveAlertNotificationRecipients')
        .mockResolvedValue([
          {
            type: 'TRANSACTION_OWNER',
            email: 'supervisor1@example.com',
            roleName: 'SUPERVISOR',
          },
          {
            type: 'SUPERVISOR',
            email: 'supervisor2@example.com',
            roleName: 'SUPERVISOR',
          },
          {
            type: 'ADMINISTRATOR',
            email: 'admin@example.com',
            roleName: 'ADMINISTRADOR',
          },
        ]);
      const sendSpy = jest
        .spyOn(service as any, 'sendAlertTriggerEmails')
        .mockResolvedValue({
          recipients: [
            {
              type: 'SUPERVISOR',
              email: 'supervisor2@example.com',
              roleName: 'SUPERVISOR',
            },
          ],
          userIds: [],
          recipientTokens: [],
          sent: ['supervisor2@example.com'],
          failed: [],
          skippedReason: null,
        });
      const auditSpy = jest
        .spyOn(service as any, 'writeSecurityLog')
        .mockResolvedValue(undefined);

      const result = await service.sendMissingEquipmentAlertsToSupervisors(
        {
          userId: 'super-1',
          username: 'superadmin',
          displayName: 'Super Administrador',
          roleName: 'SUPER ADMINISTRADOR',
        } as any,
        'test-backfill',
      );

      expect(sendSpy).toHaveBeenCalledWith(activeAlert, [
        expect.objectContaining({
          email: 'supervisor2@example.com',
          type: 'SUPERVISOR',
        }),
      ]);
      expect(repos.alertaRepo.save).toHaveBeenCalledWith(
        expect.objectContaining({
          id: 'alert-active',
          payload_json: expect.objectContaining({
            supervisor_alert_notifications: expect.arrayContaining([
              expect.objectContaining({
                source: 'test-backfill',
                email_sent: ['supervisor2@example.com'],
                email_failed: [],
              }),
            ]),
          }),
        }),
      );
      expect(auditSpy).toHaveBeenCalled();
      expect(result.data).toMatchObject({
        alerts_scanned: 1,
        alerts_with_missing_recipients: 1,
        attempted_count: 1,
        sent_count: 1,
        failed_count: 0,
        already_delivered_count: 1,
      });
    });

    it('no repite correos de supervisores ya registrados como enviados', async () => {
      repos.alertaRepo.find.mockResolvedValue([
        {
          id: 'alert-complete',
          equipo_id: 'equipo-1',
          estado: 'EN_PROCESO',
          fecha_generada: new Date('2026-08-29T08:00:00Z'),
          payload_json: {
            supervisor_alert_notifications: [
              { email_sent: ['supervisor@example.com'] },
            ],
          },
          is_deleted: false,
        },
      ]);
      jest
        .spyOn(service as any, 'resolveAlertNotificationRecipients')
        .mockResolvedValue([
          {
            type: 'SUPERVISOR',
            email: 'supervisor@example.com',
            roleName: 'SUPERVISOR',
          },
        ]);
      const sendSpy = jest.spyOn(service as any, 'sendAlertTriggerEmails');
      jest
        .spyOn(service as any, 'writeSecurityLog')
        .mockResolvedValue(undefined);

      const result = await service.sendMissingEquipmentAlertsToSupervisors({
        roleName: 'SUPERADMINISTRADOR',
      } as any);

      expect(sendSpy).not.toHaveBeenCalled();
      expect(repos.alertaRepo.save).not.toHaveBeenCalled();
      expect(result.data).toMatchObject({
        alerts_scanned: 1,
        alerts_with_missing_recipients: 0,
        attempted_count: 0,
        sent_count: 0,
        failed_count: 0,
        already_delivered_count: 1,
      });
    });
  });

  describe('sendMissingEquipmentAlertsToRequiredRoles', () => {
    it('rechaza el reenvio cuando el actor no es Super Administrador', async () => {
      await expect(
        service.sendMissingEquipmentAlertsToRequiredRoles({
          roleName: 'ADMINISTRADOR',
        } as any),
      ).rejects.toBeInstanceOf(ForbiddenException);

      expect(repos.alertaRepo.find).not.toHaveBeenCalled();
    });

    it('envia solo a administradores faltantes sin repetir supervisores ya registrados', async () => {
      const activeAlert = {
        id: 'alert-required-roles',
        equipo_id: 'equipo-1',
        estado: 'EN_PROCESO',
        fecha_generada: new Date('2026-08-29T08:00:00Z'),
        payload_json: {
          supervisor_alert_notifications: [
            { email_sent: ['supervisor@example.com'] },
          ],
          alert_notification: {
            email_sent: ['admin1@example.com'],
          },
        },
        is_deleted: false,
      };
      repos.alertaRepo.find.mockResolvedValue([activeAlert]);
      repos.alertaRepo.save.mockImplementation(async (value: any) => value);
      jest
        .spyOn(service as any, 'resolveAlertNotificationRecipients')
        .mockResolvedValue([
          {
            type: 'SUPERVISOR',
            email: 'supervisor@example.com',
            roleName: 'SUPERVISOR',
          },
          {
            type: 'ADMINISTRATOR',
            email: 'admin1@example.com',
            roleName: 'ADMINISTRADOR',
          },
          {
            type: 'ADMINISTRATOR',
            email: 'admin2@example.com',
            roleName: 'ADMINISTRADOR',
          },
          {
            type: 'ADMINISTRATOR',
            email: 'superadmin@example.com',
            roleName: 'SUPER ADMINISTRADOR',
          },
          {
            type: 'GENERAL_MANAGER',
            email: 'manager@example.com',
            roleName: 'GERENTE GENERAL',
          },
        ]);
      const sendSpy = jest
        .spyOn(service as any, 'sendAlertTriggerEmails')
        .mockResolvedValue({
          recipients: [],
          userIds: [],
          recipientTokens: [],
          sent: ['admin2@example.com', 'superadmin@example.com'],
          failed: [],
          skippedReason: null,
        });
      jest
        .spyOn(service as any, 'writeSecurityLog')
        .mockResolvedValue(undefined);

      const result = await service.sendMissingEquipmentAlertsToRequiredRoles(
        { roleName: 'SUPER ADMINISTRADOR' } as any,
        'test-required-roles',
      );

      expect(sendSpy).toHaveBeenCalledWith(activeAlert, [
        expect.objectContaining({ email: 'admin2@example.com' }),
        expect.objectContaining({ email: 'superadmin@example.com' }),
      ]);
      expect(repos.alertaRepo.save).toHaveBeenCalledWith(
        expect.objectContaining({
          payload_json: expect.objectContaining({
            required_role_alert_notifications: expect.arrayContaining([
              expect.objectContaining({
                source: 'test-required-roles',
                email_sent: [
                  'admin2@example.com',
                  'superadmin@example.com',
                ],
              }),
            ]),
          }),
        }),
      );
      expect(result.data).toMatchObject({
        alerts_scanned: 1,
        alerts_with_missing_recipients: 1,
        attempted_count: 2,
        sent_count: 2,
        failed_count: 0,
        already_delivered_count: 2,
      });
    });

    it('no repite destinatarios registrados por el saneamiento de perfiles', async () => {
      repos.alertaRepo.find.mockResolvedValue([
        {
          id: 'alert-required-complete',
          equipo_id: 'equipo-1',
          estado: 'ABIERTA',
          fecha_generada: new Date('2026-08-29T08:00:00Z'),
          payload_json: {
            required_role_alert_notifications: [
              { email_sent: ['admin@example.com'] },
            ],
          },
          is_deleted: false,
        },
      ]);
      jest
        .spyOn(service as any, 'resolveAlertNotificationRecipients')
        .mockResolvedValue([
          {
            type: 'ADMINISTRATOR',
            email: 'admin@example.com',
            roleName: 'ADMINISTRADOR',
          },
        ]);
      const sendSpy = jest.spyOn(service as any, 'sendAlertTriggerEmails');
      jest
        .spyOn(service as any, 'writeSecurityLog')
        .mockResolvedValue(undefined);

      const result = await service.sendMissingEquipmentAlertsToRequiredRoles({
        roleName: 'SUPERADMINISTRADOR',
      } as any);

      expect(sendSpy).not.toHaveBeenCalled();
      expect(repos.alertaRepo.save).not.toHaveBeenCalled();
      expect(result.data).toMatchObject({
        alerts_scanned: 1,
        alerts_with_missing_recipients: 0,
        attempted_count: 0,
        sent_count: 0,
        failed_count: 0,
        already_delivered_count: 1,
      });
    });
  });
});

describe('KpiMaintenanceService analisis de lubricante', () => {
  let repos: RepoBag;
  let service: KpiMaintenanceService;

  beforeEach(() => {
    jest.clearAllMocks();
    repos = createRepos();
    service = createService(repos, createDataSourceMock());
  });

  it('exige seleccionar un aceite para anexar el analisis', async () => {
    await expect(
      (service as any).resolveAnalisisOilProduct(null),
    ).rejects.toThrow('Debes seleccionar el aceite asociado al análisis');
  });

  it('exige seleccionar un equipo antes de crear o importar un analisis', async () => {
    await expect(
      service.createAnalisisLubricante({ producto_id: 'aceite-1' } as any),
    ).rejects.toThrow('Debes seleccionar el equipo asociado al análisis');

    repos.productoRepo.findOne.mockResolvedValue({
      id: '578f7006-1849-47fb-b9b6-f1e598f42427',
      codigo: 'ACE-15W40',
      nombre: 'Aceite 15W40',
      es_aceite: true,
      is_deleted: false,
    });
    await expect(
      service.startAnalisisLubricanteImport(
        {
          originalname: 'analisis.xlsx',
          buffer: Buffer.from('excel'),
          size: 5,
        },
        { producto_id: '578f7006-1849-47fb-b9b6-f1e598f42427' },
      ),
    ).rejects.toThrow(
      'Debes seleccionar el equipo asociado a los análisis del Excel',
    );
  });

  it('rechaza un producto que no esta configurado como aceite', async () => {
    repos.productoRepo.findOne.mockResolvedValue({
      id: '4fd51a40-5ca3-4ecf-8ff4-2ccbed34f757',
      codigo: 'MAT-001',
      nombre: 'Material general',
      es_aceite: false,
      is_deleted: false,
    });

    await expect(
      (service as any).resolveAnalisisOilProduct(
        '4fd51a40-5ca3-4ecf-8ff4-2ccbed34f757',
      ),
    ).rejects.toThrow('no está configurado como aceite');
  });

  it('anexa el aceite seleccionado como fuente autoritativa del analisis y del Excel', async () => {
    const producto = {
      id: '578f7006-1849-47fb-b9b6-f1e598f42427',
      codigo: 'ACE-15W40',
      nombre: 'Aceite 15W40',
      descripcion: 'Motor diesel',
      marca_id: 'ae93661d-91a8-4652-b043-b450b44bc454',
      es_aceite: true,
      is_deleted: false,
    };
    repos.productoRepo.findOne.mockResolvedValue(producto);
    repos.marcaRepo.findOne.mockResolvedValue({
      id: producto.marca_id,
      nombre: 'Mobil',
      is_deleted: false,
    });

    const oil = await (service as any).resolveAnalisisOilProduct(producto.id);
    const normalized = (service as any).applyAnalisisOilSnapshot(
      {
        producto_id: producto.id,
        lubricante: 'Texto proveniente del Excel',
        marca_lubricante: 'Otra marca',
        payload_json: { source: 'EXCEL' },
      },
      oil,
    );

    expect(normalized).toMatchObject({
      producto_id: producto.id,
      lubricante: 'Aceite 15W40',
      marca_lubricante: 'Mobil',
      payload_json: {
        source: 'EXCEL',
        producto_id: producto.id,
        producto_label: 'ACE-15W40 - Aceite 15W40 (Motor diesel)',
        lubricante_codigo: 'ACE-15W40',
        lubricante_descripcion: 'Motor diesel',
      },
    });
  });

  it('reemplaza el equipo escrito en el Excel por el equipo seleccionado', () => {
    const selectedEquipment = {
      equipo: {
        id: 'equipo-seleccionado',
        codigo: 'EQ-001',
        nombre: 'Generador principal',
        modelo: 'CAT 500',
      },
      marcaNombre: 'Caterpillar',
    };
    const normalized = (service as any).applyAnalisisEquipmentSnapshot(
      {
        equipo_id: 'equipo-equivocado',
        equipo_codigo: 'MAL-999',
        equipo_nombre: 'Nombre incorrecto del Excel',
        payload_json: {
          source: 'EXCEL',
          equipo_modelo: 'Modelo incorrecto',
          sample_info: {
            equipo_marca: 'Marca incorrecta',
            equipo_modelo: 'Modelo incorrecto',
          },
        },
      },
      selectedEquipment,
    );

    expect(normalized).toMatchObject({
      equipo_id: 'equipo-seleccionado',
      equipo_codigo: 'EQ-001',
      equipo_nombre: 'Generador principal',
      payload_json: {
        source: 'EXCEL',
        equipo_id: 'equipo-seleccionado',
        equipo_codigo: 'EQ-001',
        equipo_nombre: 'Generador principal',
        equipo_modelo: 'CAT 500',
        equipo_marca: 'Caterpillar',
        sample_info: {
          equipo_id: 'equipo-seleccionado',
          equipo_codigo: 'EQ-001',
          equipo_nombre: 'Generador principal',
          equipo_modelo: 'CAT 500',
          equipo_marca: 'Caterpillar',
        },
      },
    });
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

  it('crea la OT con multiples compartimientos, deduplicando y preservando el orden de entrada', async () => {
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
    repos.equipoComponenteRepo.find.mockResolvedValue([
      {
        id: 'comp-2',
        equipo_id: 'equipo-1',
        codigo: 'C2',
        nombre: 'Generador',
        nombre_oficial: 'Generador principal',
        is_deleted: false,
      },
      {
        id: 'comp-1',
        equipo_id: 'equipo-1',
        codigo: 'C1',
        nombre: 'Motor',
        nombre_oficial: 'Motor principal',
        is_deleted: false,
      },
    ]);
    repos.woRepo.save.mockImplementation(async (value) => ({
      id: 'wo-1',
      ...value,
    }));

    jest
      .spyOn(service as any, 'syncPlanFromProcedimiento')
      .mockResolvedValue({ plan: { id: 'plan-1' } });
    jest.spyOn(service as any, 'enrichWorkOrder').mockResolvedValue({
      id: 'wo-1',
      code: 'OT-A00010',
      title: 'Orden',
      status_workflow: 'PLANNED',
    });

    await service.createWorkOrder({
      code: 'OT-A00010',
      type: 'MANTENIMIENTO',
      title: 'Orden',
      equipment_id: 'equipo-1',
      maintenance_kind: 'CORRECTIVO',
      status_workflow: 'PLANNED',
      equipo_componente_ids: ['comp-1', 'comp-2', 'comp-1'],
      valor_json: {
        causa: 'Fuga detectada',
        accion: 'Cambio de componente',
        prevencion: 'Revisión semanal',
      },
    } as any);

    expect(repos.woRepo.save).toHaveBeenCalledWith(
      expect.objectContaining({
        equipo_componente_id: 'comp-1',
        equipo_componente_nombre: 'Motor',
        equipo_componente_nombre_oficial: 'Motor principal',
        valor_json: expect.objectContaining({
          equipo_componentes: [
            {
              id: 'comp-1',
              codigo: 'C1',
              nombre: 'Motor',
              nombre_oficial: 'Motor principal',
              label: 'Motor principal',
            },
            {
              id: 'comp-2',
              codigo: 'C2',
              nombre: 'Generador',
              nombre_oficial: 'Generador principal',
              label: 'Generador principal',
            },
          ],
        }),
      }),
    );
  });

  it('rechaza crear una OT si un compartimiento no existe o fue eliminado', async () => {
    repos.equipoRepo.findOne.mockResolvedValue({
      id: 'equipo-1',
      nombre: 'UG 03',
      codigo: 'UG03',
      is_deleted: false,
    });
    repos.equipoComponenteRepo.find.mockResolvedValue([]);

    await expect(
      service.createWorkOrder({
        code: 'OT-A00011',
        type: 'MANTENIMIENTO',
        title: 'Orden',
        equipment_id: 'equipo-1',
        maintenance_kind: 'CORRECTIVO',
        status_workflow: 'PLANNED',
        equipo_componente_ids: ['comp-missing'],
        valor_json: {
          causa: 'Fuga detectada',
          accion: 'Cambio de componente',
          prevencion: 'Revisión semanal',
        },
      } as any),
    ).rejects.toBeInstanceOf(BadRequestException);
  });

  it('rechaza crear una OT si el compartimiento pertenece a otro equipo', async () => {
    repos.equipoRepo.findOne.mockResolvedValue({
      id: 'equipo-1',
      nombre: 'UG 03',
      codigo: 'UG03',
      is_deleted: false,
    });
    repos.equipoComponenteRepo.find.mockResolvedValue([
      {
        id: 'comp-ajeno',
        equipo_id: 'equipo-99',
        codigo: 'C9',
        nombre: 'Ajeno',
        nombre_oficial: null,
        is_deleted: false,
      },
    ]);

    await expect(
      service.createWorkOrder({
        code: 'OT-A00012',
        type: 'MANTENIMIENTO',
        title: 'Orden',
        equipment_id: 'equipo-1',
        maintenance_kind: 'CORRECTIVO',
        status_workflow: 'PLANNED',
        equipo_componente_ids: ['comp-ajeno'],
        valor_json: {
          causa: 'Fuga detectada',
          accion: 'Cambio de componente',
          prevencion: 'Revisión semanal',
        },
      } as any),
    ).rejects.toBeInstanceOf(BadRequestException);
  });

  it('crea la OT usando el escalar legado equipo_componente_id cuando no se envia el arreglo', async () => {
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
    repos.equipoComponenteRepo.find.mockResolvedValue([
      {
        id: 'comp-1',
        equipo_id: 'equipo-1',
        codigo: 'C1',
        nombre: 'Motor',
        nombre_oficial: 'Motor principal',
        is_deleted: false,
      },
    ]);
    repos.woRepo.save.mockImplementation(async (value) => ({
      id: 'wo-1',
      ...value,
    }));
    jest
      .spyOn(service as any, 'syncPlanFromProcedimiento')
      .mockResolvedValue({ plan: { id: 'plan-1' } });
    jest.spyOn(service as any, 'enrichWorkOrder').mockResolvedValue({
      id: 'wo-1',
      code: 'OT-A00013',
      title: 'Orden',
      status_workflow: 'PLANNED',
    });

    await service.createWorkOrder({
      code: 'OT-A00013',
      type: 'MANTENIMIENTO',
      title: 'Orden',
      equipment_id: 'equipo-1',
      maintenance_kind: 'CORRECTIVO',
      status_workflow: 'PLANNED',
      equipo_componente_id: 'comp-1',
      valor_json: {
        causa: 'Fuga detectada',
        accion: 'Cambio de componente',
        prevencion: 'Revisión semanal',
      },
    } as any);

    expect(repos.woRepo.save).toHaveBeenCalledWith(
      expect.objectContaining({
        equipo_componente_id: 'comp-1',
        valor_json: expect.objectContaining({
          equipo_componentes: [
            {
              id: 'comp-1',
              codigo: 'C1',
              nombre: 'Motor',
              nombre_oficial: 'Motor principal',
              label: 'Motor principal',
            },
          ],
        }),
      }),
    );
  });

  it('actualiza reemplazando la seleccion de compartimientos preservando el nombre historico de los ids repetidos', async () => {
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
    repos.woRepo.findOne.mockResolvedValue({
      id: 'wo-1',
      code: 'OT-A00014',
      type: 'MANTENIMIENTO',
      equipment_id: 'equipo-1',
      plan_id: 'plan-1',
      title: 'Orden',
      maintenance_kind: 'CORRECTIVO',
      status_workflow: 'PLANNED',
      equipo_componente_id: 'comp-1',
      equipo_componente_nombre: 'Motor viejo',
      equipo_componente_nombre_oficial: 'Motor viejo oficial',
      valor_json: {
        causa: 'Fuga detectada',
        accion: 'Cambio de componente',
        prevencion: 'Revisión semanal',
        equipo_componentes: [
          {
            id: 'comp-1',
            codigo: 'C1',
            nombre: 'Motor viejo',
            nombre_oficial: 'Motor viejo oficial',
            label: 'Motor viejo oficial',
          },
        ],
      },
      is_deleted: false,
    });
    repos.equipoComponenteRepo.find.mockResolvedValue([
      {
        id: 'comp-1',
        equipo_id: 'equipo-1',
        codigo: 'C1',
        nombre: 'Motor NUEVO NOMBRE',
        nombre_oficial: 'Motor nuevo oficial',
        is_deleted: false,
      },
      {
        id: 'comp-2',
        equipo_id: 'equipo-1',
        codigo: 'C2',
        nombre: 'Generador',
        nombre_oficial: 'Generador principal',
        is_deleted: false,
      },
    ]);
    repos.woRepo.save.mockImplementation(async (value) => value);
    jest
      .spyOn(service as any, 'syncPlanFromProcedimiento')
      .mockResolvedValue({ plan: { id: 'plan-1' } });
    jest.spyOn(service as any, 'enrichWorkOrder').mockResolvedValue({
      id: 'wo-1',
      code: 'OT-A00014',
      title: 'Orden',
      status_workflow: 'PLANNED',
    });

    await service.updateWorkOrder('wo-1', {
      status_workflow: 'PLANNED',
      equipo_componente_ids: ['comp-1', 'comp-2'],
      valor_json: { accion: 'Cambio de componente' },
    } as any);

    expect(repos.woRepo.save).toHaveBeenCalledWith(
      expect.objectContaining({
        equipo_componente_id: 'comp-1',
        equipo_componente_nombre: 'Motor viejo',
        equipo_componente_nombre_oficial: 'Motor viejo oficial',
        valor_json: expect.objectContaining({
          equipo_componentes: [
            {
              id: 'comp-1',
              codigo: 'C1',
              nombre: 'Motor viejo',
              nombre_oficial: 'Motor viejo oficial',
              label: 'Motor viejo oficial',
            },
            {
              id: 'comp-2',
              codigo: 'C2',
              nombre: 'Generador',
              nombre_oficial: 'Generador principal',
              label: 'Generador principal',
            },
          ],
        }),
      }),
    );
  });

  it('preserva los compartimientos guardados cuando la actualizacion omite ambos campos de compartimiento', async () => {
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
    const storedSnapshot = [
      {
        id: 'comp-1',
        codigo: 'C1',
        nombre: 'Motor',
        nombre_oficial: 'Motor principal',
        label: 'Motor principal',
      },
    ];
    repos.woRepo.findOne.mockResolvedValue({
      id: 'wo-1',
      code: 'OT-A00015',
      type: 'MANTENIMIENTO',
      equipment_id: 'equipo-1',
      plan_id: 'plan-1',
      title: 'Orden',
      maintenance_kind: 'CORRECTIVO',
      status_workflow: 'PLANNED',
      equipo_componente_id: 'comp-1',
      equipo_componente_nombre: 'Motor',
      equipo_componente_nombre_oficial: 'Motor principal',
      valor_json: {
        causa: 'Fuga detectada',
        accion: 'Cambio de componente',
        prevencion: 'Revisión semanal',
        equipo_componentes: storedSnapshot,
      },
      is_deleted: false,
    });
    repos.woRepo.save.mockImplementation(async (value) => value);
    jest
      .spyOn(service as any, 'syncPlanFromProcedimiento')
      .mockResolvedValue({ plan: { id: 'plan-1' } });
    jest.spyOn(service as any, 'enrichWorkOrder').mockResolvedValue({
      id: 'wo-1',
      code: 'OT-A00015',
      title: 'Orden',
      status_workflow: 'PLANNED',
    });

    await service.updateWorkOrder('wo-1', {
      status_workflow: 'PLANNED',
      maintenance_kind: 'PREVENTIVO',
    } as any);

    expect(repos.equipoComponenteRepo.find).not.toHaveBeenCalled();
    expect(repos.woRepo.save).toHaveBeenCalledWith(
      expect.objectContaining({
        equipo_componente_id: 'comp-1',
        equipo_componente_nombre: 'Motor',
        equipo_componente_nombre_oficial: 'Motor principal',
        valor_json: expect.objectContaining({
          equipo_componentes: storedSnapshot,
        }),
      }),
    );
  });

  it('limpia los compartimientos cuando la actualizacion envia un arreglo vacio explicito', async () => {
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
    repos.woRepo.findOne.mockResolvedValue({
      id: 'wo-1',
      code: 'OT-A00016',
      type: 'MANTENIMIENTO',
      equipment_id: 'equipo-1',
      plan_id: 'plan-1',
      title: 'Orden',
      maintenance_kind: 'CORRECTIVO',
      status_workflow: 'PLANNED',
      equipo_componente_id: 'comp-1',
      equipo_componente_nombre: 'Motor',
      equipo_componente_nombre_oficial: 'Motor principal',
      valor_json: {
        causa: 'Fuga detectada',
        accion: 'Cambio de componente',
        prevencion: 'Revisión semanal',
        equipo_componentes: [
          {
            id: 'comp-1',
            codigo: 'C1',
            nombre: 'Motor',
            nombre_oficial: 'Motor principal',
            label: 'Motor principal',
          },
        ],
      },
      is_deleted: false,
    });
    repos.woRepo.save.mockImplementation(async (value) => value);
    jest
      .spyOn(service as any, 'syncPlanFromProcedimiento')
      .mockResolvedValue({ plan: { id: 'plan-1' } });
    jest.spyOn(service as any, 'enrichWorkOrder').mockResolvedValue({
      id: 'wo-1',
      code: 'OT-A00016',
      title: 'Orden',
      status_workflow: 'PLANNED',
    });

    await service.updateWorkOrder('wo-1', {
      status_workflow: 'PLANNED',
      equipo_componente_ids: [],
      valor_json: { accion: 'Cambio de componente' },
    } as any);

    expect(repos.woRepo.save).toHaveBeenCalledWith(
      expect.objectContaining({
        equipo_componente_id: null,
        equipo_componente_nombre: null,
        equipo_componente_nombre_oficial: null,
        valor_json: expect.objectContaining({ equipo_componentes: [] }),
      }),
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
      status_workflow: 'PLANNED',
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

  it('rechaza nuevas reservas cuando la OT está cerrada', async () => {
    repos.woRepo.findOne.mockResolvedValue({
      id: 'wo-closed',
      status_workflow: 'CLOSED',
      status: 'ACTIVE',
      valor_json: {},
      is_deleted: false,
    });

    await expect(
      service.createConsumo('wo-closed', {
        producto_id: 'producto-1',
        bodega_id: 'bodega-1',
        cantidad: 1,
      } as any),
    ).rejects.toBeInstanceOf(ForbiddenException);
    expect(repos.consumoRepo.save).not.toHaveBeenCalled();
    expect(repos.reservaRepo.save).not.toHaveBeenCalled();
  });

  it('impide eliminar un consumo cuando ya existe una salida de material', async () => {
    jest
      .spyOn(service as any, 'assertWorkOrderAnnulmentAllowed')
      .mockResolvedValue(undefined);
    jest
      .spyOn(service as any, 'calculatePlannedAndIssuedMaterialTotals')
      .mockResolvedValue({ plannedQty: 5, issuedQty: 2, pendingQty: 3 });
    const manager = {
      findOne: jest
        .fn()
        .mockResolvedValueOnce({
          id: 'wo-1',
          status_workflow: 'IN_PROGRESS',
          valor_json: {},
          is_deleted: false,
        })
        .mockResolvedValueOnce({
          id: 'consumo-1',
          work_order_id: 'wo-1',
          producto_id: 'producto-1',
          bodega_id: 'bodega-1',
          cantidad: 5,
          is_deleted: false,
        }),
      save: jest.fn(),
    };
    const rollbackTransaction = jest.fn();
    (service as any).dataSource = {
      createQueryRunner: () => ({
        connect: jest.fn(),
        startTransaction: jest.fn(),
        commitTransaction: jest.fn(),
        rollbackTransaction,
        release: jest.fn(),
        manager,
      }),
    };

    await expect(
      service.deleteConsumo('wo-1', 'consumo-1', {
        roleName: 'ADMINISTRADOR',
      } as any),
    ).rejects.toBeInstanceOf(ConflictException);
    expect(manager.save).not.toHaveBeenCalled();
    expect(rollbackTransaction).toHaveBeenCalledTimes(1);
  });

  it('solo permite reducir la cantidad pendiente y conserva lo ya emitido', async () => {
    jest
      .spyOn(service as any, 'assertWorkOrderAnnulmentAllowed')
      .mockResolvedValue(undefined);
    jest
      .spyOn(service as any, 'calculatePlannedAndIssuedMaterialTotals')
      .mockResolvedValue({ plannedQty: 5, issuedQty: 2, pendingQty: 3 });
    const rebuildSpy = jest
      .spyOn(service as any, 'rebuildPendingReservaFromConsumos')
      .mockResolvedValue(undefined);
    jest
      .spyOn(service as any, 'appendWorkOrderHistory')
      .mockResolvedValue(undefined);
    jest.spyOn(service as any, 'writeSecurityLog').mockResolvedValue(undefined);
    const workOrder = {
      id: 'wo-1',
      status_workflow: 'IN_PROGRESS',
      valor_json: {},
      is_deleted: false,
    };
    const consumo = {
      id: 'consumo-1',
      work_order_id: 'wo-1',
      producto_id: 'producto-1',
      bodega_id: 'bodega-1',
      cantidad: 5,
      costo_unitario: 2,
      subtotal: 10,
      is_deleted: false,
    };
    const manager = {
      findOne: jest
        .fn()
        .mockResolvedValueOnce(workOrder)
        .mockResolvedValueOnce(consumo),
      save: jest.fn(async (_entity, value) => value),
    };
    (service as any).dataSource = {
      createQueryRunner: () => ({
        connect: jest.fn(),
        startTransaction: jest.fn(),
        commitTransaction: jest.fn(),
        rollbackTransaction: jest.fn(),
        release: jest.fn(),
        manager,
      }),
    };

    await service.reduceConsumo(
      'wo-1',
      'consumo-1',
      { cantidad_restar: 3 },
      { username: 'admin', roleName: 'ADMINISTRADOR' } as any,
    );

    expect(consumo).toMatchObject({ cantidad: 2, subtotal: 4, is_deleted: false });
    expect(rebuildSpy).toHaveBeenCalledWith(
      'wo-1',
      'producto-1',
      'bodega-1',
      manager,
    );
  });

  it('presenta OT, equipo, solicitante y materiales en el correo de reserva', () => {
    (service as any).publicBaseUrl = 'https://justicecompany-ec.com';
    const items = [
      {
        work_order_id: 'wo-1',
        work_order_code: 'OT-A00025',
        work_order_title: 'Mantenimiento preventivo',
        equipment_label: 'EQ-001 - Generador (CAT 500)',
        requester_labels: ['Operador Uno', 'Supervisor Dos'],
        producto_id: 'producto-1',
        producto_label: 'MAT-001-Aceite (15W40)',
        bodega_id: 'bodega-1',
        bodega_label: 'BOD-001 - Principal',
        sucursal_id: 'sucursal-1',
        cantidad_reservada: 5,
        observacion: 'Entrega para turno nocturno',
      },
    ];

    const html = (service as any).buildReservationEmailHtml(
      {
        type: 'WAREHOUSE_STAFF',
        email: 'bodega@example.com',
        displayName: 'Bodega Uno',
      },
      items,
    );
    const text = (service as any).buildReservationEmailText(items);

    expect(html).toContain('<!doctype html>');
    expect(html).toContain('OT-A00025');
    expect(html).toContain('EQ-001 - Generador (CAT 500)');
    expect(html).toContain('Operador Uno, Supervisor Dos');
    expect(html).toContain('MAT-001-Aceite (15W40)');
    expect(html).toContain(
      'https://justicecompany-ec.com/app/work-orders',
    );
    expect(text).toContain('verificar el stock');
  });

  it('redirige las alertas al módulo que corresponde', () => {
    (service as any).publicBaseUrl = 'https://justicecompany-ec.com/app';
    const workOrderDestination = (service as any).resolveAlertEmailDestination({
      work_order_id: 'wo-1',
      origen: 'MANTENIMIENTO',
      categoria: 'MANTENIMIENTO',
      referencia_tipo: 'WORK_ORDER',
      payload_json: {},
    });
    const materialDestination = (service as any).resolveAlertEmailDestination({
      work_order_id: null,
      origen: 'INVENTARIO',
      categoria: 'INVENTARIO',
      referencia_tipo: 'STOCK_BODEGA',
      payload_json: {},
    });

    expect(workOrderDestination).toMatchObject({
      label: 'Abrir órdenes de trabajo',
      url: 'https://justicecompany-ec.com/app/work-orders',
    });
    expect(materialDestination).toMatchObject({
      label: 'Abrir materiales',
      url: 'https://justicecompany-ec.com/app/productos',
    });
  });

  it('aplica el formato corporativo y escapa datos en incidentes técnicos', async () => {
    const sendMail = jest.fn().mockResolvedValue(undefined);
    (service as any).alertAdministratorEmail = 'admin@example.com';
    jest
      .spyOn(service as any, 'getAlertMailTransporter')
      .mockResolvedValue({ sendMail });

    await (service as any).sendTechnicalIncidentEmail({
      ticket: 'INC-1',
      moduleName: 'Inventario',
      method: 'POST',
      requestUrl: '/productos',
      statusCode: 500,
      createdBy: 'Usuario Uno',
      payload: { response_message: '<script>alert(1)</script>' },
    });

    const message = sendMail.mock.calls[0][0];
    expect(message.html).toContain('<!doctype html>');
    expect(message.html).toContain('&lt;script&gt;alert(1)&lt;/script&gt;');
    expect(message.html).not.toContain('<script>alert(1)</script>');
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

  it('detecta OT anuladas por status, approval_action o marca de anulacion', () => {
    const isAnnulled = (workOrder: any) =>
      (service as any).isWorkOrderAnnulled(workOrder);

    expect(isAnnulled({ status: 'ANULADA', valor_json: {} })).toBe(true);
    expect(
      isAnnulled({ status: 'ACTIVE', valor_json: { approval_action: 'ANULADA' } }),
    ).toBe(true);
    expect(
      isAnnulled({
        status: 'ACTIVE',
        valor_json: { annulment: { motivo: 'stock' } },
      }),
    ).toBe(true);
    expect(
      isAnnulled({ status: 'ACTIVE', valor_json: { approval_action: 'CERRADA' } }),
    ).toBe(false);
    expect(isAnnulled(null)).toBe(false);
  });

  it('construye la etiqueta de equipo como marca - nombre (modelo)', () => {
    const buildLabel = (equipment: any) =>
      (service as any).buildEquipmentReportLabel(equipment);

    expect(
      buildLabel({ marca_nombre: 'CAT', nombre: 'GENERADOR', modelo: '3512' }),
    ).toBe('CAT - GENERADOR (3512)');
    expect(buildLabel({ marca_nombre: 'CAT', nombre: 'GENERADOR', modelo: null })).toBe(
      'CAT - GENERADOR',
    );
    expect(buildLabel(null)).toBe('Sin equipo');
  });

  it('excluye OT anuladas del listado para roles distintos de Super Administrador', async () => {
    const rows = [
      {
        id: 'wo-cerrada',
        status: 'ACTIVE',
        status_workflow: 'CLOSED',
        valor_json: { approval_action: 'CERRADA' },
        is_deleted: false,
      },
      {
        id: 'wo-anulada',
        status: 'ANULADA',
        status_workflow: 'CLOSED',
        valor_json: { approval_action: 'ANULADA' },
        is_deleted: false,
      },
    ];
    const qb: any = {
      where: jest.fn().mockReturnThis(),
      andWhere: jest.fn().mockReturnThis(),
      orderBy: jest.fn().mockReturnThis(),
      getMany: jest.fn().mockResolvedValue(rows),
    };
    repos.woRepo.createQueryBuilder.mockReturnValue(qb);
    jest
      .spyOn(service as any, 'enrichWorkOrder')
      .mockImplementation(async (row: any) => row);

    const result = await service.listWorkOrders(
      {} as any,
      undefined,
      { roleName: 'SUPERVISOR' } as any,
    );

    expect(result.data.map((row: any) => row.id)).toEqual(['wo-cerrada']);
  });

  it('incluye OT anuladas en el listado solo para Super Administrador', async () => {
    const rows = [
      {
        id: 'wo-cerrada',
        status: 'ACTIVE',
        status_workflow: 'CLOSED',
        valor_json: { approval_action: 'CERRADA' },
        is_deleted: false,
      },
      {
        id: 'wo-anulada',
        status: 'ANULADA',
        status_workflow: 'CLOSED',
        valor_json: { approval_action: 'ANULADA' },
        is_deleted: false,
      },
    ];
    const qb: any = {
      where: jest.fn().mockReturnThis(),
      andWhere: jest.fn().mockReturnThis(),
      orderBy: jest.fn().mockReturnThis(),
      getMany: jest.fn().mockResolvedValue(rows),
    };
    repos.woRepo.createQueryBuilder.mockReturnValue(qb);
    jest
      .spyOn(service as any, 'enrichWorkOrder')
      .mockImplementation(async (row: any) => row);

    const result = await service.listWorkOrders(
      {} as any,
      undefined,
      { roleName: 'SUPER ADMINISTRADOR' } as any,
    );

    expect(result.data.map((row: any) => row.id)).toEqual([
      'wo-cerrada',
      'wo-anulada',
    ]);
  });

  it('oculta el detalle de una OT anulada a roles distintos de Super Administrador', async () => {
    repos.woRepo.findOne.mockResolvedValue({
      id: 'wo-anulada',
      status: 'ANULADA',
      status_workflow: 'CLOSED',
      valor_json: { approval_action: 'ANULADA' },
      is_deleted: false,
    });

    await expect(
      service.getWorkOrder('wo-anulada', undefined, {
        roleName: 'SUPERVISOR',
      } as any),
    ).rejects.toBeInstanceOf(NotFoundException);
  });

  it('permite al Super Administrador consultar el detalle de una OT anulada', async () => {
    const workOrder = {
      id: 'wo-anulada',
      status: 'ANULADA',
      status_workflow: 'CLOSED',
      valor_json: { approval_action: 'ANULADA' },
      is_deleted: false,
    };
    repos.woRepo.findOne.mockResolvedValue(workOrder);
    jest
      .spyOn(service as any, 'enrichWorkOrder')
      .mockResolvedValue(workOrder);

    const result = await service.getWorkOrder('wo-anulada', undefined, {
      roleName: 'SUPER ADMINISTRADOR',
    } as any);

    expect(result.data).toEqual(workOrder);
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

  const createTransactionalDataSourceMock = (bag: RepoBag) => {
    const entityRepoMap = new Map<any, any>([
      [EquipoEntity, bag.equipoRepo],
      [
        EquipoFuncionamientoHistorialEntity,
        bag.equipoFuncionamientoHistorialRepo,
      ],
    ]);
    return {
      query: jest.fn().mockResolvedValue([]),
      transaction: jest.fn(async (cb: any) =>
        cb({ getRepository: (entity: any) => entityRepoMap.get(entity) }),
      ),
    } as unknown as DataSource;
  };

  beforeEach(() => {
    jest.clearAllMocks();
    repos = createRepos();
    service = createService(repos, createTransactionalDataSourceMock(repos));
  });

  it('sincroniza el constraint de estado operativo con el catálogo vigente', async () => {
    const dataSource = createTransactionalDataSourceMock(repos);
    const schemaService = createService(repos, dataSource);

    await (schemaService as any).ensureEquipoEstadoOperativoSchema();

    expect(dataSource.query).toHaveBeenCalledWith(
      expect.stringContaining('DROP CONSTRAINT IF EXISTS ck_tb_equipo_estado_operativo'),
    );
    const migrationSql = (dataSource.query as jest.Mock).mock.calls[0][0];
    expect(migrationSql).toContain("WHEN estado_operativo = 'BLOQUEADO' THEN 'BLOQUEADA'");
    expect(migrationSql).toContain("WHEN estado_operativo = 'FUERA_SERVICIO' THEN 'BLOQUEADA'");
    for (const value of [
      'OPERATIVO',
      'RESERVA',
      'MPG',
      'CORRECTIVO',
      'BLOQUEADA',
    ]) {
      expect(migrationSql).toContain(`'${value}'`);
    }
  });

  it('actualiza el estado_funcionamiento, conserva el estado_operativo y registra el historial', async () => {
    const equipo = {
      id: 'equipo-1',
      codigo: 'EQ-001',
      estado_operativo: 'OPERATIVO',
      estado_funcionamiento: 'PARADO',
      estado_funcionamiento_actualizado_en: null,
      updated_by: 'anterior',
      is_deleted: false,
    };
    repos.equipoRepo.findOne.mockResolvedValue(equipo);
    repos.equipoRepo.save.mockImplementation(async (value: any) => value);
    repos.equipoFuncionamientoHistorialRepo.save.mockImplementation(
      async (value: any) => value,
    );

    const result = await service.updateEquipoEstadoFuncionamiento(
      'equipo-1',
      { estado_funcionamiento: 'FUNCIONAMIENTO' as any },
      {
        userId: '2c2f5e02-2f0a-4c4d-9f2a-9c9d1f3a5b21',
        username: 'jdoe',
        displayName: 'John Doe',
      },
    );

    expect(equipo.estado_funcionamiento).toBe('FUNCIONAMIENTO');
    expect(equipo.estado_operativo).toBe('OPERATIVO');
    expect(equipo.updated_by).toBe('John Doe');
    expect(equipo.estado_funcionamiento_actualizado_en).toBeInstanceOf(Date);
    expect(repos.equipoRepo.save).toHaveBeenCalledWith(equipo);
    expect(repos.equipoFuncionamientoHistorialRepo.save).toHaveBeenCalledWith(
      expect.objectContaining({
        equipo_id: 'equipo-1',
        estado_anterior: 'PARADO',
        estado_nuevo: 'FUNCIONAMIENTO',
        estado_anterior_desde: null,
        duracion_estado_anterior_segundos: null,
        changed_by_id: '2c2f5e02-2f0a-4c4d-9f2a-9c9d1f3a5b21',
        changed_by: 'John Doe',
        changed_at: equipo.estado_funcionamiento_actualizado_en,
      }),
    );
    expect(result.data.estado_funcionamiento).toBe('FUNCIONAMIENTO');
    expect(result.data.estado_funcionamiento_actualizado_en).toBeInstanceOf(
      Date,
    );
  });

  it('calcula la duracion exacta del tramo anterior al cerrar PARADO->FUNCIONAMIENTO', async () => {
    const inicioParada = new Date('2026-08-20T10:00:00.000Z');
    const ahora = new Date('2026-08-20T10:05:30.000Z'); // +330s
    jest.useFakeTimers().setSystemTime(ahora);
    const equipo = {
      id: 'equipo-1',
      codigo: 'EQ-001',
      estado_operativo: 'OPERATIVO',
      estado_funcionamiento: 'PARADO',
      estado_funcionamiento_actualizado_en: inicioParada,
      updated_by: 'anterior',
      is_deleted: false,
    };
    repos.equipoRepo.findOne.mockResolvedValue(equipo);
    repos.equipoRepo.save.mockImplementation(async (value: any) => value);
    repos.equipoFuncionamientoHistorialRepo.save.mockImplementation(
      async (value: any) => value,
    );

    try {
      await service.updateEquipoEstadoFuncionamiento(
        'equipo-1',
        { estado_funcionamiento: 'FUNCIONAMIENTO' as any },
        { userId: 'u1', username: 'jdoe', displayName: 'John Doe' },
      );
    } finally {
      jest.useRealTimers();
    }

    expect(
      repos.equipoFuncionamientoHistorialRepo.save,
    ).toHaveBeenCalledWith(
      expect.objectContaining({
        estado_anterior: 'PARADO',
        estado_nuevo: 'FUNCIONAMIENTO',
        estado_anterior_desde: inicioParada,
        duracion_estado_anterior_segundos: 330,
        changed_at: ahora,
      }),
    );
  });

  it('no crea un evento ni cambia la fecha del ultimo cambio cuando el estado se repite', async () => {
    const fecha = new Date('2026-08-20T10:00:00.000Z');
    const equipo = {
      id: 'equipo-1',
      estado_operativo: 'OPERATIVO',
      estado_funcionamiento: 'FUNCIONAMIENTO',
      estado_funcionamiento_actualizado_en: fecha,
      updated_by: 'anterior',
      is_deleted: false,
    };
    repos.equipoRepo.findOne.mockResolvedValue(equipo);

    const result = await service.updateEquipoEstadoFuncionamiento(
      'equipo-1',
      { estado_funcionamiento: 'FUNCIONAMIENTO' as any },
      { userId: 'u1', username: 'jdoe', displayName: 'John Doe' },
    );

    expect(repos.equipoRepo.save).not.toHaveBeenCalled();
    expect(
      repos.equipoFuncionamientoHistorialRepo.save,
    ).not.toHaveBeenCalled();
    expect(equipo.estado_funcionamiento_actualizado_en).toBe(fecha);
    expect(equipo.updated_by).toBe('anterior');
    expect(result.data.estado_funcionamiento_actualizado_en).toBe(fecha);
  });

  it('rechaza valores distintos de FUNCIONAMIENTO/PARADO', async () => {
    await expect(
      service.updateEquipoEstadoFuncionamiento(
        'equipo-1',
        { estado_funcionamiento: 'ENCENDIDO' as any },
        {},
      ),
    ).rejects.toBeInstanceOf(BadRequestException);
    expect(repos.equipoRepo.findOne).not.toHaveBeenCalled();
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
    expect(
      repos.equipoFuncionamientoHistorialRepo.save,
    ).not.toHaveBeenCalled();
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

  it('lista el historial de estado de funcionamiento ordenado por fecha descendente', async () => {
    repos.equipoRepo.findOne.mockResolvedValue({
      id: 'equipo-1',
      is_deleted: false,
    });
    const rows = [
      { id: 'h-2', changed_at: new Date('2026-08-21T00:00:00.000Z') },
      { id: 'h-1', changed_at: new Date('2026-08-20T00:00:00.000Z') },
    ];
    const qb: any = {
      where: jest.fn().mockReturnThis(),
      andWhere: jest.fn().mockReturnThis(),
      orderBy: jest.fn().mockReturnThis(),
      getMany: jest.fn().mockResolvedValue(rows),
    };
    repos.equipoFuncionamientoHistorialRepo.createQueryBuilder.mockReturnValue(
      qb,
    );

    const result = await service.listEquipoFuncionamientoHistorial(
      'equipo-1',
      { from: '2026-08-01', to: '2026-08-31' } as any,
    );

    expect(qb.where).toHaveBeenCalledWith('h.equipo_id = :equipoId', {
      equipoId: 'equipo-1',
    });
    expect(qb.andWhere).toHaveBeenCalledWith('h.changed_at >= :from', {
      from: '2026-08-01',
    });
    expect(qb.andWhere).toHaveBeenCalledWith('h.changed_at <= :to', {
      to: '2026-08-31',
    });
    expect(qb.orderBy).toHaveBeenCalledWith('h.changed_at', 'DESC');
    expect(result.data).toEqual(rows);
  });

  describe('mantenimiento por tiempo: unidad HORAS y recordatorios por unidad', () => {
    it.each([
      ['HORA', 'HORAS'],
      ['HORAS', 'HORAS'],
      ['Hora', 'HORAS'],
      ['hour', 'HORAS'],
      ['HOURS', 'HORAS'],
      ['dias', 'DIAS'],
      ['semanas', 'SEMANAS'],
      ['anios', 'ANIOS'],
    ])('normaliza "%s" como unidad canonica %s', (raw, expected) => {
      expect(
        (service as any).normalizeEquipmentServiceIntervalUnit(raw),
      ).toBe(expected);
    });

    it('redondea un intervalo de 25 horas hacia arriba a 2 dias calendario', () => {
      const schedule = (service as any).resolveEquipmentServiceSchedule({
        es_servicio: true,
        intervalo_mantenimiento_valor: 25,
        intervalo_mantenimiento_unidad: 'HORAS',
        ultimo_servicio_fecha: '2026-08-24',
      });

      expect(schedule.intervalo_mantenimiento_unidad).toBe('HORAS');
      expect(schedule.proximo_servicio_fecha).toBe('2026-08-26');
    });

    it('redondea un intervalo de 1 a 24 horas a exactamente 1 dia calendario', () => {
      const scheduleUnaHora = (service as any).resolveEquipmentServiceSchedule({
        es_servicio: true,
        intervalo_mantenimiento_valor: 1,
        intervalo_mantenimiento_unidad: 'HORAS',
        ultimo_servicio_fecha: '2026-08-24',
      });
      const scheduleVeinticuatroHoras = (
        service as any
      ).resolveEquipmentServiceSchedule({
        es_servicio: true,
        intervalo_mantenimiento_valor: 24,
        intervalo_mantenimiento_unidad: 'HORAS',
        ultimo_servicio_fecha: '2026-08-24',
      });

      expect(scheduleUnaHora.proximo_servicio_fecha).toBe('2026-08-25');
      expect(scheduleVeinticuatroHoras.proximo_servicio_fecha).toBe(
        '2026-08-25',
      );
    });

    const buildEquipoServicioRow = (overrides: Record<string, unknown>) => ({
      id: 'equipo-1',
      codigo: 'EQ-001',
      nombre: 'Generador',
      nombre_real: 'Generador principal',
      es_servicio: true,
      is_deleted: false,
      ultimo_servicio_fecha: '2026-08-01',
      intervalo_mantenimiento_valor: 1,
      updated_by: 'tester',
      created_by: 'tester',
      ...overrides,
    });

    async function runCandidatesAt(nowIso: string, row: Record<string, unknown>) {
      jest.useFakeTimers().setSystemTime(new Date(nowIso));
      try {
        repos.equipoRepo.find.mockResolvedValue([row]);
        return await (service as any).buildEquipmentServiceAlertCandidates();
      } finally {
        jest.useRealTimers();
      }
    }

    it.each([
      ['3', '2026-08-27', 'D-3', 'Alerta 1: 3 días antes'],
      ['2', '2026-08-26', 'D-2', 'Alerta 2: 2 días antes'],
      ['1', '2026-08-25', 'D-1', 'Alerta 3: 1 día antes'],
      ['0', '2026-08-24', 'D-0', 'Alerta 4: el mismo día'],
    ])(
      'DIAS: genera el recordatorio exacto a %s dia(s) restantes',
      async (daysLabel, proximoServicioFecha, stage, label) => {
        const row = buildEquipoServicioRow({
          intervalo_mantenimiento_unidad: 'DIAS',
          proximo_servicio_fecha: proximoServicioFecha,
        });
        const candidates = await runCandidatesAt(
          '2026-08-24T15:00:00.000Z',
          row,
        );

        expect(candidates).toHaveLength(1);
        expect(candidates[0].referencia).toBe(
          `EQUIPO_SERVICIO:equipo-1:${stage}:${proximoServicioFecha}`,
        );
        expect(candidates[0].detalle).toContain(label);
        expect(candidates[0].payload_json.dias_restantes).toBe(
          Number(daysLabel),
        );
        expect(candidates[0].payload_json.horas_restantes).toBeNull();
      },
    );

    it.each([
      ['7', '2026-08-31', 'S-7', 'Alerta 1: 1 semana antes (7 días)'],
      ['3', '2026-08-27', 'S-3', 'Alerta 2: media semana antes (3 días)'],
      ['1', '2026-08-25', 'S-1', 'Alerta 3: 1 día antes'],
      ['0', '2026-08-24', 'S-0', 'Alerta 4: el mismo día'],
    ])(
      'SEMANAS: genera el recordatorio exacto a %s dia(s) restantes',
      async (_label, proximoServicioFecha, stage, label) => {
        const row = buildEquipoServicioRow({
          intervalo_mantenimiento_unidad: 'SEMANAS',
          proximo_servicio_fecha: proximoServicioFecha,
        });
        const candidates = await runCandidatesAt(
          '2026-08-24T15:00:00.000Z',
          row,
        );

        expect(candidates).toHaveLength(1);
        expect(candidates[0].referencia).toBe(
          `EQUIPO_SERVICIO:equipo-1:${stage}:${proximoServicioFecha}`,
        );
        expect(candidates[0].detalle).toContain(label);
      },
    );

    it('ANIOS: genera el recordatorio de 30 dias antes', async () => {
      const row = buildEquipoServicioRow({
        intervalo_mantenimiento_unidad: 'ANIOS',
        proximo_servicio_fecha: '2026-09-23',
      });
      const candidates = await runCandidatesAt('2026-08-24T15:00:00.000Z', row);

      expect(candidates).toHaveLength(1);
      expect(candidates[0].referencia).toBe(
        'EQUIPO_SERVICIO:equipo-1:A-30:2026-09-23',
      );
      expect(candidates[0].detalle).toContain(
        'Alerta 1: 1 mes antes (30 días)',
      );
    });

    it('ANIOS: genera dos recordatorios simultaneos y distintos a 15 dias antes', async () => {
      const row = buildEquipoServicioRow({
        intervalo_mantenimiento_unidad: 'ANIOS',
        proximo_servicio_fecha: '2026-09-08',
      });
      const candidates = await runCandidatesAt('2026-08-24T15:00:00.000Z', row);

      expect(candidates).toHaveLength(2);
      expect(candidates.map((c: any) => c.referencia)).toEqual([
        'EQUIPO_SERVICIO:equipo-1:A-15A:2026-09-08',
        'EQUIPO_SERVICIO:equipo-1:A-15B:2026-09-08',
      ]);
      expect(candidates[0].detalle).toContain(
        'Alerta 2: medio mes antes (15 días)',
      );
      expect(candidates[1].detalle).toContain('Alerta 3: 15 día antes');
    });

    it.each([
      ['1', '2026-08-25', 'A-1', 'Alerta 4: 1 día antes'],
      ['0', '2026-08-24', 'A-0', 'Alerta 5: el mismo día'],
    ])(
      'ANIOS: genera el recordatorio exacto a %s dia(s) restantes',
      async (_label, proximoServicioFecha, stage, label) => {
        const row = buildEquipoServicioRow({
          intervalo_mantenimiento_unidad: 'ANIOS',
          proximo_servicio_fecha: proximoServicioFecha,
        });
        const candidates = await runCandidatesAt(
          '2026-08-24T15:00:00.000Z',
          row,
        );

        expect(candidates).toHaveLength(1);
        expect(candidates[0].referencia).toBe(
          `EQUIPO_SERVICIO:equipo-1:${stage}:${proximoServicioFecha}`,
        );
        expect(candidates[0].detalle).toContain(label);
      },
    );

    it.each([
      ['24', '2026-08-24T05:00:00.000Z', 'H-24', 'Alerta 1: 24 H antes'],
      ['12', '2026-08-24T17:00:00.000Z', 'H-12', 'Alerta 2: 12 H antes'],
      ['6', '2026-08-24T23:00:00.000Z', 'H-6', 'Alerta 3: 6 H antes'],
      ['1', '2026-08-25T04:00:00.000Z', 'H-1', 'Alerta 4: 1 H antes'],
    ])(
      'HORAS: genera el recordatorio exacto a %s hora(s) restantes',
      async (hours, nowIso, stage, label) => {
        // proximo_servicio_fecha = 2026-08-25 equivale a 2026-08-25T05:00:00.000Z
        // (medianoche en America/Guayaquil, UTC-05:00 fijo).
        const row = buildEquipoServicioRow({
          intervalo_mantenimiento_unidad: 'HORAS',
          proximo_servicio_fecha: '2026-08-25',
        });
        const candidates = await runCandidatesAt(nowIso, row);

        expect(candidates).toHaveLength(1);
        expect(candidates[0].referencia).toBe(
          `EQUIPO_SERVICIO:equipo-1:${stage}:2026-08-25`,
        );
        expect(candidates[0].detalle).toContain(label);
        expect(candidates[0].payload_json.horas_restantes).toBe(
          Number(hours),
        );
        expect(candidates[0].payload_json.dias_restantes).toBeNull();
      },
    );

    it('no genera recordatorios fuera de los umbrales solicitados (legado 10/5/vencido eliminado)', async () => {
      const rowDiasFueraDeUmbral = buildEquipoServicioRow({
        intervalo_mantenimiento_unidad: 'DIAS',
        proximo_servicio_fecha: '2026-08-19', // -5 dias: antes era "vencido"
      });
      const candidatesVencido = await runCandidatesAt(
        '2026-08-24T15:00:00.000Z',
        rowDiasFueraDeUmbral,
      );
      expect(candidatesVencido).toHaveLength(0);

      const rowDiezDias = buildEquipoServicioRow({
        intervalo_mantenimiento_unidad: 'DIAS',
        proximo_servicio_fecha: '2026-09-03', // +10 dias: antes disparaba D-10
      });
      const candidatesDiez = await runCandidatesAt(
        '2026-08-24T15:00:00.000Z',
        rowDiezDias,
      );
      expect(candidatesDiez).toHaveLength(0);

      const rowHorasFueraDeUmbral = buildEquipoServicioRow({
        intervalo_mantenimiento_unidad: 'HORAS',
        proximo_servicio_fecha: '2026-08-25',
      });
      const candidatesHoras = await runCandidatesAt(
        '2026-08-24T09:00:00.000Z', // faltan 20h, ningun umbral de HORAS
        rowHorasFueraDeUmbral,
      );
      expect(candidatesHoras).toHaveLength(0);
    });
  });
});

describe('KpiMaintenanceService reservas de bodega', () => {
  let repos: RepoBag;
  let service: KpiMaintenanceService;
  let entregaRepo: ReturnType<typeof createRepo>;
  let entregaDetRepo: ReturnType<typeof createRepo>;

  const createReservationsDataSourceMock = () => {
    const repoMap = new Map<any, any>([
      [EntregaMaterialEntity, entregaRepo],
      [EntregaMaterialDetEntity, entregaDetRepo],
    ]);
    return {
      query: jest.fn().mockResolvedValue([]),
      getRepository: (entity: any) => repoMap.get(entity),
      manager: { getRepository: (entity: any) => repoMap.get(entity) },
    } as unknown as DataSource;
  };

  beforeEach(() => {
    jest.clearAllMocks();
    repos = createRepos();
    entregaRepo = createRepo();
    entregaDetRepo = createRepo();
    service = createService(repos, createReservationsDataSourceMock());
  });

  describe('listWorkOrderReservations', () => {
    it('lista reservas consolidadas y aplica filtros de estado y busqueda', async () => {
      repos.reservaRepo.find.mockResolvedValue([
        {
          id: 'reserva-1',
          work_order_id: 'wo-1',
          producto_id: 'producto-1',
          bodega_id: 'bodega-1',
          cantidad: 5,
          estado: 'RESERVADO',
          is_deleted: false,
        },
        {
          id: 'reserva-2',
          work_order_id: 'wo-2',
          producto_id: 'producto-2',
          bodega_id: 'bodega-2',
          cantidad: 0,
          estado: 'CONSUMIDO',
          is_deleted: false,
        },
      ]);
      repos.woRepo.find.mockResolvedValue([
        {
          id: 'wo-1',
          code: 'OT-A00001',
          title: 'Cambio de filtro',
          status_workflow: 'IN_PROGRESS',
          equipment_id: 'equipo-1',
          valor_json: null,
        },
        {
          id: 'wo-2',
          code: 'OT-A00002',
          title: 'Cambio de aceite',
          status_workflow: 'CLOSED',
          equipment_id: null,
          valor_json: null,
        },
      ]);
      repos.productoRepo.find.mockResolvedValue([
        { id: 'producto-1', codigo: 'MAT-1', nombre: 'Filtro' },
        { id: 'producto-2', codigo: 'MAT-2', nombre: 'Aceite' },
      ]);
      repos.bodegaRepo.find.mockResolvedValue([
        { id: 'bodega-1', codigo: 'BOD-1', nombre: 'Bodega principal' },
        { id: 'bodega-2', codigo: 'BOD-2', nombre: 'Bodega secundaria' },
      ]);
      repos.equipoRepo.find.mockResolvedValue([
        { id: 'equipo-1', codigo: 'EQ-1', nombre: 'Excavadora' },
      ]);
      repos.consumoRepo.find.mockResolvedValue([
        {
          work_order_id: 'wo-1',
          producto_id: 'producto-1',
          bodega_id: 'bodega-1',
          cantidad: 8,
          is_deleted: false,
          observacion: 'Consumo planificado',
        },
        {
          work_order_id: 'wo-2',
          producto_id: 'producto-2',
          bodega_id: 'bodega-2',
          cantidad: 4,
          is_deleted: false,
          observacion: null,
        },
      ]);
      entregaRepo.find.mockResolvedValue([
        { id: 'entrega-1', work_order_id: 'wo-1', is_deleted: false },
        { id: 'entrega-2', work_order_id: 'wo-2', is_deleted: false },
      ]);
      entregaDetRepo.find.mockResolvedValue([
        { entrega_id: 'entrega-1', producto_id: 'producto-1', bodega_id: 'bodega-1', cantidad: 3 },
        { entrega_id: 'entrega-2', producto_id: 'producto-2', bodega_id: 'bodega-2', cantidad: 4 },
      ]);

      const result = await service.listWorkOrderReservations({} as any, null);

      expect(result.data.items).toHaveLength(2);
      const reservado = result.data.items.find(
        (item: any) => item.reserva_id === 'reserva-1',
      );
      expect(reservado).toMatchObject({
        tipo_registro: 'RESERVA DE MATERIAL',
        estado: 'RESERVADO',
        cantidad_solicitada: 8,
        cantidad_entregada: 3,
        cantidad_pendiente: 5,
        cantidad_reservada_activa: 5,
        reserva_activa: true,
        observacion_reserva: 'Consumo planificado',
      });
      expect(result.data.resumen.total_registros).toBe(2);
      expect(result.data.resumen.total_reservados).toBe(1);
      expect(result.data.resumen.total_consumidos).toBe(1);

      const searchResult = await service.listWorkOrderReservations(
        { search: 'excavadora' } as any,
        null,
      );
      expect(searchResult.data.items).toHaveLength(1);
      expect(searchResult.data.items[0].reserva_id).toBe('reserva-1');

      const estadoResult = await service.listWorkOrderReservations(
        { estado: 'consumido' } as any,
        null,
      );
      expect(estadoResult.data.items).toHaveLength(1);
      expect(estadoResult.data.items[0].reserva_id).toBe('reserva-2');
    });

    it('deriva la cantidad liberada de legado (LIBERADO con cantidad en cero) desde lo solicitado menos lo entregado', async () => {
      repos.reservaRepo.find.mockResolvedValue([
        {
          id: 'reserva-legado',
          work_order_id: 'wo-3',
          producto_id: 'producto-3',
          bodega_id: 'bodega-3',
          cantidad: 0,
          estado: 'LIBERADO',
          is_deleted: false,
        },
      ]);
      repos.woRepo.find.mockResolvedValue([
        {
          id: 'wo-3',
          code: 'OT-A00003',
          title: 'OT legado',
          status_workflow: 'CLOSED',
          equipment_id: null,
          valor_json: null,
        },
      ]);
      repos.productoRepo.find.mockResolvedValue([
        { id: 'producto-3', codigo: 'MAT-3', nombre: 'Manguera' },
      ]);
      repos.bodegaRepo.find.mockResolvedValue([
        { id: 'bodega-3', codigo: 'BOD-3', nombre: 'Bodega 3' },
      ]);
      repos.consumoRepo.find.mockResolvedValue([
        {
          work_order_id: 'wo-3',
          producto_id: 'producto-3',
          bodega_id: 'bodega-3',
          cantidad: 10,
          is_deleted: false,
          observacion: null,
        },
      ]);
      entregaRepo.find.mockResolvedValue([
        { id: 'entrega-3', work_order_id: 'wo-3', is_deleted: false },
      ]);
      entregaDetRepo.find.mockResolvedValue([
        { entrega_id: 'entrega-3', producto_id: 'producto-3', bodega_id: 'bodega-3', cantidad: 6 },
      ]);

      const result = await service.listWorkOrderReservations({} as any, null);

      expect(result.data.items[0]).toMatchObject({
        estado: 'LIBERADO',
        cantidad_solicitada: 10,
        cantidad_entregada: 6,
        cantidad_liberada: 4,
        reserva_activa: false,
      });
    });
  });

  describe('assertMaterialShortfallAcknowledged', () => {
    it('exige un motivo solo cuando existe un remanente real entre lo solicitado y lo entregado', async () => {
      repos.consumoRepo.find.mockResolvedValue([
        {
          work_order_id: 'wo-1',
          producto_id: 'producto-1',
          bodega_id: 'bodega-1',
          cantidad: 10,
          is_deleted: false,
        },
      ]);
      repos.reservaRepo.find.mockResolvedValue([
        {
          id: 'reserva-1',
          work_order_id: 'wo-1',
          producto_id: 'producto-1',
          bodega_id: 'bodega-1',
          cantidad: 4,
          estado: 'RESERVADO',
          is_deleted: false,
        },
      ]);
      entregaRepo.find.mockResolvedValue([
        { id: 'entrega-1', work_order_id: 'wo-1', is_deleted: false },
      ]);
      entregaDetRepo.find.mockResolvedValue([
        { entrega_id: 'entrega-1', producto_id: 'producto-1', bodega_id: 'bodega-1', cantidad: 6 },
      ]);

      await expect(
        (service as any).assertMaterialShortfallAcknowledged(undefined, 'wo-1', {}),
      ).rejects.toBeInstanceOf(BadRequestException);

      await expect(
        (service as any).assertMaterialShortfallAcknowledged(undefined, 'wo-1', {
          observacion_menor_uso_reserva: '   ',
        }),
      ).rejects.toBeInstanceOf(BadRequestException);

      await expect(
        (service as any).assertMaterialShortfallAcknowledged(undefined, 'wo-1', {
          observacion_menor_uso_reserva: 'a'.repeat(501),
        }),
      ).rejects.toBeInstanceOf(BadRequestException);

      await expect(
        (service as any).assertMaterialShortfallAcknowledged(undefined, 'wo-1', {
          observacion_menor_uso_reserva:
            'Se uso menos porque el filtro estaba en buen estado',
        }),
      ).resolves.toBeUndefined();
    });

    it('no exige motivo cuando lo entregado iguala o supera lo solicitado', async () => {
      repos.consumoRepo.find.mockResolvedValue([
        {
          work_order_id: 'wo-2',
          producto_id: 'producto-1',
          bodega_id: 'bodega-1',
          cantidad: 5,
          is_deleted: false,
        },
      ]);
      entregaRepo.find.mockResolvedValue([
        { id: 'entrega-2', work_order_id: 'wo-2', is_deleted: false },
      ]);
      entregaDetRepo.find.mockResolvedValue([
        { entrega_id: 'entrega-2', producto_id: 'producto-1', bodega_id: 'bodega-1', cantidad: 5 },
      ]);

      await expect(
        (service as any).assertMaterialShortfallAcknowledged(undefined, 'wo-2', {}),
      ).resolves.toBeUndefined();
    });

    it('no exige motivo cuando no hubo material reservado para la OT', async () => {
      repos.consumoRepo.find.mockResolvedValue([]);
      entregaRepo.find.mockResolvedValue([]);
      entregaDetRepo.find.mockResolvedValue([]);

      await expect(
        (service as any).assertMaterialShortfallAcknowledged(undefined, 'wo-3', null),
      ).resolves.toBeUndefined();
    });
  });

  describe('releaseOpenReservationsForWorkOrder', () => {
    it('libera reservas activas conservando el remanente y marcando LIBERADO', async () => {
      const reservationRow = {
        id: 'reserva-1',
        work_order_id: 'wo-1',
        producto_id: 'producto-1',
        bodega_id: 'bodega-1',
        cantidad: 4,
        estado: 'RESERVADO',
        is_deleted: false,
      };
      repos.reservaRepo.createQueryBuilder.mockReturnValue({
        where: jest.fn().mockReturnThis(),
        andWhere: jest.fn().mockReturnThis(),
        getMany: jest.fn().mockResolvedValue([reservationRow]),
      });
      repos.reservaRepo.save.mockImplementation(async (value: any) => value);
      repos.productoRepo.find.mockResolvedValue([
        { id: 'producto-1', codigo: 'MAT-1', nombre: 'Filtro' },
      ]);
      repos.bodegaRepo.find.mockResolvedValue([
        { id: 'bodega-1', codigo: 'BOD-1', nombre: 'Bodega principal' },
      ]);
      repos.woHistoryRepo.create.mockImplementation((value: any) => value);
      repos.woHistoryRepo.save.mockImplementation(async (value: any) => value);
      repos.consumoRepo.find.mockResolvedValue([
        {
          work_order_id: 'wo-1',
          producto_id: 'producto-1',
          bodega_id: 'bodega-1',
          cantidad: 10,
          is_deleted: false,
        },
      ]);
      entregaRepo.find.mockResolvedValue([
        { id: 'entrega-1', work_order_id: 'wo-1', is_deleted: false },
      ]);
      entregaDetRepo.find.mockResolvedValue([
        { entrega_id: 'entrega-1', producto_id: 'producto-1', bodega_id: 'bodega-1', cantidad: 6 },
      ]);

      const releasedCount = await (service as any).releaseOpenReservationsForWorkOrder(
        'wo-1',
        undefined,
        null,
      );

      expect(releasedCount).toBe(1);
      expect(reservationRow.estado).toBe('LIBERADO');
      expect(reservationRow.cantidad).toBe(4);
      expect(repos.reservaRepo.save).toHaveBeenCalledWith([reservationRow]);
    });
  });

  describe('reservas cerradas o liberadas no se consideran activas', () => {
    it('una reserva de una OT cerrada o anulada no se considera activa', () => {
      expect((service as any).isWorkOrderReservationActive('CLOSED')).toBe(false);
      expect((service as any).isWorkOrderReservationActive('CERRADA')).toBe(false);
      expect((service as any).isWorkOrderReservationActive('IN_PROGRESS')).toBe(true);
    });
  });
});
