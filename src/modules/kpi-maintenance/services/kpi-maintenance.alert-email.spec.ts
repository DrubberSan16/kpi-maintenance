import { KpiMaintenanceService } from './kpi-maintenance.service';

/**
 * Lo que ve quien recibe un correo de alerta.
 *
 * Referencias descriptivas para todos los orígenes y fechas de Guayaquil.
 */
type ServiceUnderTest = Record<string, any>;

function createService(): ServiceUnderTest {
  return Object.create(KpiMaintenanceService.prototype) as ServiceUnderTest;
}

describe('KpiMaintenanceService correos de alerta', () => {
  describe('formatAlertEmailDate', () => {
    it('devuelve yyyy-MM-dd HH:mm en hora de Guayaquil', () => {
      const service = createService();
      // 13:44 UTC son las 08:44 en Guayaquil (UTC-5).
      expect(service.formatAlertEmailDate('2026-09-16T13:44:00.000Z')).toBe(
        '2026-09-16 08:44',
      );
    });

    it('usa reloj de 24 horas, con medianoche en 00', () => {
      const service = createService();
      expect(service.formatAlertEmailDate('2026-09-16T05:00:00.000Z')).toBe(
        '2026-09-16 00:00',
      );
      expect(service.formatAlertEmailDate('2026-09-16T22:05:00.000Z')).toBe(
        '2026-09-16 17:05',
      );
    });

    it('acepta un Date y avisa cuando la fecha no sirve', () => {
      const service = createService();
      expect(
        service.formatAlertEmailDate(new Date('2026-01-02T15:30:00.000Z')),
      ).toBe('2026-01-02 10:30');
      expect(service.formatAlertEmailDate('cualquier cosa')).toBe(
        'No disponible',
      );
      expect(service.formatAlertEmailDate(null)).toBe('No disponible');
    });
  });

  describe('resolveAlertReferenceDisplay', () => {
    it('describe una OT por numero y titulo', () => {
      const service = createService();
      expect(
        service.resolveAlertReferenceDisplay(
          {
            referencia:
              'WORK_ORDER:c5151c5a-9fe3-421d-ab42-40c029eeb6f2:FINALIZADA:1789566248988',
            referencia_tipo: 'WORK_ORDER',
          },
          {
            work_order_code: 'OT-A00180',
            work_order_title: 'Cambio de aceite UG02',
          },
        ),
      ).toBe('OT-A00180 - Cambio de aceite UG02');
    });

    it('nunca muestra la llave interna aunque falten codigo y titulo', () => {
      const service = createService();
      const shown = service.resolveAlertReferenceDisplay(
        {
          referencia:
            'WORK_ORDER:c5151c5a-9fe3-421d-ab42-40c029eeb6f2:FINALIZADA:1789566248988',
          referencia_tipo: 'WORK_ORDER',
        },
        {},
      );
      expect(shown).toBe('Orden de trabajo');
      expect(shown).not.toContain('WORK_ORDER:');
      expect(shown).not.toContain('1789566248988');
    });

    it('se conforma con el numero cuando la OT no tiene titulo', () => {
      const service = createService();
      expect(
        service.resolveAlertReferenceDisplay(
          { referencia: 'WORK_ORDER:x', referencia_tipo: 'WORK_ORDER' },
          { work_order_code: 'OT-A00180' },
        ),
      ).toBe('OT-A00180');
    });

    it('conserva códigos descriptivos de otros módulos', () => {
      const service = createService();
      expect(
        service.resolveAlertReferenceDisplay(
          { referencia: 'PROGRAMACION:1', referencia_tipo: 'PROGRAMACION' },
          { plan_nombre: 'MPG 250 horas' },
        ),
      ).toBe('Programación · MPG 250 horas');
      expect(
        service.resolveAlertReferenceDisplay(
          { referencia: 'TANQUE-3', referencia_tipo: 'COMBUSTIBLE' },
          {},
        ),
      ).toBe('TANQUE-3');
    });
  });
});

const uuid = 'e9b6967d-1f3a-4baa-941e-092bdf52883c';
const recipient = {
  type: 'ADMINISTRATOR',
  email: 'admin@example.com',
  displayName: 'Administradora',
  roleName: 'ADMINISTRADOR',
};
const equipment = {
  id: uuid,
  nombre: 'JC - UG11',
  modelo: '3512 B',
  marca_id: 'marca',
  location_id: 'central',
};
const equipmentLabel = 'CATERPILLAR - JC - UG11 (3512 B) · CENTRAL TPTA';

function emailService() {
  const service = createService();
  const sendMail = jest.fn().mockResolvedValue({});
  Object.assign(service, {
    logger: { log: jest.fn(), warn: jest.fn() },
    publicBaseUrl: 'https://example.com',
    equipoRepo: { findOne: jest.fn().mockResolvedValue(equipment) },
    marcaRepo: {
      find: jest
        .fn()
        .mockResolvedValue([{ id: 'marca', nombre: 'CATERPILLAR' }]),
    },
    locationRepo: {
      find: jest
        .fn()
        .mockResolvedValue([{ id: 'central', nombre: 'CENTRAL TPTA' }]),
    },
    woRepo: {
      findOne: jest
        .fn()
        .mockResolvedValue({
          code: 'OT-A00163',
          title: 'Cebado UG11',
          equipment_id: uuid,
        }),
    },
    getAlertMailTransporter: jest.fn().mockResolvedValue({ sendMail }),
    writeSecurityLog: jest.fn().mockResolvedValue(undefined),
    emitWorkOrderLifecycleAlert: jest.fn().mockResolvedValue(undefined),
  });
  return { service, sendMail };
}

describe('correos sin referencias técnicas', () => {
  it('describe material y bodega si el catálogo no se puede consultar', async () => {
    const { service } = emailService();
    service.buildInventoryCatalogMaps = jest.fn().mockRejectedValue(new Error('Catálogo no disponible'));
    const prepared = await service.prepareAlertEmail({ payload_json: { inventory_items: [{
      producto_id: uuid, bodega_id: uuid, producto_label: uuid, bodega_label: uuid,
    }] } });
    expect(prepared.payload_json.inventory_items[0].producto_label).toBe('Material sin registro');
    expect(prepared.payload_json.inventory_items[0].bodega_label).toBe('Bodega sin registro');
  });

  it.each([
    'resumen diario',
    'carga masiva',
    'movimiento',
    'reserva',
    'salida',
    'matriz',
    'recordatorio',
  ])('envía contenido comprensible en el correo de %s', async (kind) => {
    const { service, sendMail } = emailService();
    const item = {
      producto_id: uuid,
      bodega_id: uuid,
      producto_label: 'MAT-001 - Aceite',
      bodega_label: 'BOD-001 - TPTA',
      stock_actual: 10,
      stock_critico: 2,
      stock_disponible_minimo: 8,
      stock_min_bodega: 15,
      observacion: `Material ${uuid}`,
      work_order_code: 'OT-A00163',
      work_order_title: 'Cebado UG11',
      equipment_label: equipmentLabel,
      requester_labels: ['Operador TPTA'],
      cantidad_reservada: 2,
    };
    const stock = [item];
    let html: string;
    let text: string;
    if (kind === 'resumen diario') {
      html = service.buildLowStockDigestHtml(recipient, stock, '2026-10-02');
      text = service.buildLowStockDigestText(stock, '2026-10-02');
    } else if (kind === 'carga masiva' || kind === 'movimiento') {
      const mode = kind === 'carga masiva' ? 'bulk' : 'movement';
      html = service.buildScopedInventoryEmailHtml(recipient, stock, mode);
      text = service.buildScopedInventoryEmailText(stock, mode);
    } else if (kind === 'reserva') {
      html = service.buildReservationEmailHtml(recipient, stock);
      text = service.buildReservationEmailText(stock);
    } else if (kind === 'salida') {
      const context = {
        workOrder: { code: 'OT-A00163', title: 'Cebado UG11' },
        equipmentLabel,
        requesterLabels: ['Operador TPTA'],
        issuerLabel: 'Bodeguero TPTA',
        rows: [{ ...item, cantidad_entregada: 2, cantidad_pendiente: 0 }],
      };
      html = service.buildMaterialIssueEmailHtml(recipient, context);
      text = service.buildMaterialIssueEmailText(context);
    } else if (kind === 'matriz') {
      const context = {
        bodegaLabel: 'BOD-001 - TPTA',
        productoLabel: 'MAT-001 - Aceite',
        cantidadSolicitada: 2,
        stockActual: 0,
        workOrderLabel: 'OT-A00163 - Cebado UG11',
        equipmentLabel,
        solicitanteLabel: 'Bodeguero TPTA',
      };
      html = service.buildMatrizRequestEmailHtml(recipient, context);
      text = service.buildMatrizRequestEmailText(context);
    } else {
      const input = {
        recipient: { nameSurname: 'Supervisor TPTA' },
        totalEquipment: 17,
        updatedToday: 10,
        pendingToday: 7,
        dateKey: '2026-10-02',
      };
      html = service.buildDailyHorometerReminderHtml(input);
      text = service.buildDailyHorometerReminderText(input);
    }
    await service.sendReadableEmail(
      { sendMail },
      { to: recipient.email, html, text, subject: kind },
    );
    const mail = sendMail.mock.calls[0][0];
    expect(mail.html).not.toContain(uuid);
    expect(mail.text).not.toContain(uuid);
    expect(mail.html).toContain('TPTA');
    expect(mail.text).toContain('TPTA');
    if (kind !== 'recordatorio') {
      expect(mail.html).toContain('MAT-001 - Aceite');
      expect(mail.text).toContain('MAT-001 - Aceite');
    }
  });

  it.each([
    [
      'HOROMETRO',
      { horometro_proximo_mantenimiento: 38855 },
      'Mantenimiento a las 38855.00 h',
    ],
    ['HOROMETRO', {}, 'Mantenimiento a las 38855.00 h'],
    [
      'EQUIPO_SERVICIO_TIEMPO',
      { proximo_servicio_fecha: '2026-10-15' },
      'Servicio del equipo · 2026-10-15',
    ],
    [
      'CRONOGRAMA_SEMANAL',
      { cronograma_codigo: 'CS-010', actividad: 'Inspección UG11' },
      'Cronograma semanal · CS-010 · Inspección UG11',
    ],
    [
      'PROGRAMACION_MENSUAL',
      { tipo_mantenimiento: 'SSA', fecha_programada_nueva: '2026-10-15' },
      'Programación mensual · SSA · 2026-10-15',
    ],
    ['REPORTE_DIARIO', {}, 'Reporte diario'],
    ['ANALISIS_LUBRICANTE', { codigo: 'AL-A00118' }, 'Análisis · AL-A00118'],
    ['COMBUSTIBLE', { tanque: 'TPTA' }, 'Tanque TPTA'],
    ['COMBUSTIBLE', { tanque: '3' }, 'Tanque 3'],
    [
      'STOCK_BODEGA',
      { producto_label: 'Aceite', bodega_label: 'Bodega TPTA' },
      'Aceite · Bodega TPTA',
    ],
    ['OTRO_ORIGEN', {}, 'Otro origen'],
  ])('resuelve la referencia de %s', (type, payload, expected) => {
    const { service } = emailService();
    const shown = service.resolveAlertReferenceDisplay(
      { referencia_tipo: type, referencia: `HOROMETRO:${uuid}:38855` },
      payload,
    );
    expect(shown).toBe(expected);
    expect(shown).not.toContain(uuid);
  });

  it('envía el caso de la imagen con UG, central, horas y etiquetas claras, sin modificar la alerta', async () => {
    const { service, sendMail } = emailService();
    const row = {
      id: uuid,
      equipo_id: uuid,
      categoria: 'MANTENIMIENTO',
      nivel: 'WARNING',
      origen: 'SYSTEM',
      estado: 'ABIERTA',
      tipo_alerta: 'MANTENIMIENTO_PROXIMO',
      referencia_tipo: 'HOROMETRO',
      referencia: `HOROMETRO:${uuid}:38855`,
      detalle: 'Faltan 15 h para el mantenimiento',
      fecha_generada: '2026-10-02T13:44:00Z',
      payload_json: {
        tipo_alerta_publico: 'HOROMETRO_PROXIMO',
        horometro_actual: 38840,
        horas_restantes: 15,
      },
    };
    const before = JSON.stringify(row);
    await service.sendAlertTriggerEmails(row, [recipient]);
    const mail = sendMail.mock.calls[0][0];
    for (const content of [mail.subject, mail.html, mail.text])
      expect(content).not.toContain(uuid);
    for (const content of [mail.html, mail.text]) {
      expect(content).toContain(equipmentLabel);
      expect(content).toContain('Sistema automático');
      expect(content).toContain('Mantenimiento próximo por horómetro');
      expect(content).toContain('Mantenimiento a las 38855.00 h');
      expect(content).toContain('38840.00 h');
      expect(content).toContain('15.00 h');
    }
    expect(JSON.stringify(row)).toBe(before);
  });

  it('recupera el número y título de una OT antigua desde su referencia', async () => {
    const { service, sendMail } = emailService();
    await service.sendAlertTriggerEmails(
      {
        id: uuid,
        categoria: 'MANTENIMIENTO',
        nivel: 'INFO',
        origen: 'WORK_ORDER',
        estado: 'CERRADA',
        tipo_alerta: 'ORDEN_TRABAJO_FINALIZADA',
        referencia_tipo: 'WORK_ORDER',
        referencia: `WORK_ORDER:${uuid}:FINALIZADA:1789566248988`,
        payload_json: {},
      },
      [recipient],
    );
    expect(sendMail.mock.calls[0][0].text).toContain(
      'Referencia: OT-A00163 - Cebado UG11',
    );
    expect(sendMail.mock.calls[0][0].text).toContain(equipmentLabel);
  });

  it('recupera nombres históricos de materiales y bodegas para alertas guardadas con IDs', async () => {
    const { service } = emailService();
    service.buildInventoryCatalogMaps = jest.fn().mockResolvedValue({
      productMap: new Map([[uuid, { codigo: 'MAT-001', nombre: 'Aceite' }]]),
      warehouseMap: new Map([[uuid, { codigo: 'BOD-001', nombre: 'TPTA' }]]),
    });
    const prepared = await service.prepareAlertEmail({
      payload_json: {
        inventory_items: [
          {
            producto_id: uuid,
            bodega_id: uuid,
            producto_label: uuid,
            bodega_label: uuid,
          },
        ],
      },
    });
    expect(prepared.payload_json.inventory_items[0].producto_label).toBe(
      'MAT-001 - Aceite',
    );
    expect(prepared.payload_json.inventory_items[0].bodega_label).toBe(
      'BOD-001 - TPTA',
    );
    expect(service.buildInventoryCatalogMaps).toHaveBeenCalledWith(
      [uuid],
      [uuid],
      true,
    );
  });

  it('no revela costos al supervisor en el correo de revisión de OT', async () => {
    const { service, sendMail } = emailService();
    service.fetchSecurityUsers = jest.fn().mockResolvedValue([
      {
        id: uuid,
        nameUser: 'supervisor',
        nameSurname: 'Supervisor TPTA',
        email: 'supervisor@example.com',
        roleName: 'SUPERVISOR',
        status: 'ACTIVE',
      },
    ]);
    service.resolveWorkOrderOilGallons = jest
      .fn()
      .mockResolvedValue({ galones: 2, costo: 9876.54, productos: 'Aceite' });
    await service.sendWorkOrderReviewEmails({
      id: uuid,
      code: 'OT-A00163',
      equipment_id: uuid,
    });
    const mail = sendMail.mock.calls[0][0];
    expect(mail.html).toContain(equipmentLabel);
    expect(mail.html).not.toContain('Costo del aceite');
    expect(mail.html).not.toContain('9876.54');
    expect(mail.text).not.toContain('9876.54');
  });

  it('resuelve el usuario del incidente técnico y oculta IDs en el mensaje de soporte', async () => {
    const { service, sendMail } = emailService();
    service.alertAdministratorEmail = 'admin@example.com';
    service.fetchSecurityUsers = jest.fn().mockResolvedValue([
      {
        id: uuid,
        nameUser: 'operador',
        nameSurname: 'Operador TPTA',
        status: 'ACTIVE',
      },
    ]);
    await service.sendTechnicalIncidentEmail({
      ticket: 'INC-001',
      moduleName: 'Mantenimiento',
      method: 'GET',
      requestUrl: '/work-orders',
      statusCode: 500,
      createdBy: uuid,
      payload: { response_message: `No se encontró ${uuid}` },
    });
    expect(sendMail.mock.calls[0][0].text).toContain('Usuario: Operador TPTA');
    expect(sendMail.mock.calls[0][0].html).not.toContain(uuid);
  });

  it.each([
    'ADMINISTRADOR',
    'SUPER ADMINISTRADOR',
    'GERENTE GENERAL',
    'SUPERVISOR',
    'OPERADOR',
    'BODEGUERO',
  ])(
    'respeta el perfil %s en el correo de consumos y resuelve el equipo',
    async (roleName) => {
      const { service, sendMail } = emailService();
      service.buildInventoryCatalogMaps = jest
        .fn()
        .mockResolvedValue({ productMap: new Map(), warehouseMap: new Map() });
      service.resolveAlertNotificationRecipients = jest
        .fn()
        .mockResolvedValue([{ ...recipient, roleName }]);
      await service.sendWorkOrderConsumoEmails(
        {
          id: uuid,
          code: 'OT-A00163',
          equipment_id: uuid,
          status_workflow: 'IN_PROGRESS',
        },
        [
          {
            producto_id: uuid,
            bodega_id: uuid,
            cantidad: 2,
            costo_unitario: 9876.54,
          },
        ],
      );
      const mail = sendMail.mock.calls[0][0];
      const allowed = [
        'ADMINISTRADOR',
        'SUPER ADMINISTRADOR',
        'GERENTE GENERAL',
      ].includes(roleName);
      expect(mail.html.includes('9876.54')).toBe(allowed);
      expect(mail.text.includes('9876.54')).toBe(allowed);
      expect(mail.html).toContain(equipmentLabel);
      expect(mail.html).toContain('En proceso');
      expect(mail.html).toContain('Material sin registro');
      expect(mail.html).not.toContain(uuid);
    },
  );
});
