import { BadRequestException } from '@nestjs/common';
import { KpiMaintenanceService } from './kpi-maintenance.service';
import { isSsaEquipment, operatorMaintenanceKindForEquipment } from './operator-work-order-kind';

function fixture(procedure: Record<string, unknown> = {}, existingPlan: any = null) {
  // El constructor inicializa los catalogos reales de tipos, sin iniciar Nest.
  const service: any = new (KpiMaintenanceService as any)();
  const activity = {
    id: 'activity-ssa', orden: 1, actividad: 'Verificar condiciones de seguridad',
    requiere_permiso: true, requiere_epp: true, requiere_bloqueo: true,
    requiere_evidencia: true, meta: { required: true },
  };
  const repo = () => ({
    find: jest.fn().mockResolvedValue([]),
    findOne: jest.fn().mockResolvedValue(null),
    create: jest.fn(value => value),
    save: jest.fn(async value => Array.isArray(value) ? value : { id: 'plan-ssa', ...value }),
  });
  const planRepo = repo();
  planRepo.findOne.mockResolvedValue(existingPlan);
  const planTareaRepo = repo();
  Object.assign(service, {
    procedimientoRepo: { findOne: jest.fn().mockResolvedValue({
      id: 'procedure-ssa', codigo: 'PMP-SSA', nombre: 'Plantilla SSA',
      tipo_proceso: 'SSA', ...procedure,
    }) },
    procedimientoActividadRepo: { find: jest.fn().mockResolvedValue([activity]) },
    planRepo, planTareaRepo,
  });
  return { service, planRepo, planTareaRepo, activity };
}

describe('SSA en OT, plantillas y planes', () => {
  it.each(['SSA', 'ssa', ' SSA '])('acepta y normaliza el tipo de OT %s', kind => {
    const { service } = fixture();
    expect(service.resolveWorkOrderMaintenanceKind(kind)).toBe('SSA');
    expect(service.buildWorkOrderMaintenanceKindLabel(kind)).toBe('SSA');
  });

  it('conserva los tipos anteriores y rechaza tipos desconocidos', () => {
    const { service } = fixture();
    for (const kind of ['CORRECTIVO', 'PREVENTIVO', 'PREDICTIVO', 'CEBADO', 'INSPECCION', 'PROYECTO']) {
      expect(service.resolveWorkOrderMaintenanceKind(kind)).toBe(kind);
    }
    expect(service.resolveWorkOrderMaintenanceKind('MPG')).toBe('PREVENTIVO');
    expect(() => service.resolveWorkOrderMaintenanceKind('NO_EXISTE')).toThrow(BadRequestException);
  });

  it('SSA mantiene el flujo de equipo y admite materiales distintos de aceite', () => {
    const { service } = fixture();
    expect(service.isProyectoMaintenanceKind('SSA')).toBe(false);
    expect(service.requiresOilProductsForMaintenanceKind('SSA')).toBe(false);
    expect(service.requirePlanMaintenanceType('ssa')).toBe('SSA');
  });

  it.each([
    { tipo_proceso: 'PROCEDIMIENTO_TRABAJO', clase_mantenimiento: 'SSA' },
    { tipo_proceso: 'SSA', clase_mantenimiento: undefined },
  ])('genera un plan SSA desde la plantilla %j y conserva su checklist', async procedure => {
    const { service, planTareaRepo, activity } = fixture(procedure);
    const result = await service.syncPlanFromProcedimiento('procedure-ssa');
    expect(result.plan.tipo).toBe('SSA');
    expect(result.plan.requiere_parada).toBe(true);
    expect(planTareaRepo.save).toHaveBeenCalledWith([expect.objectContaining({
      actividad: activity.actividad,
      required: true,
      meta: expect.objectContaining({
        procedimiento_actividad_id: activity.id,
        requiere_permiso: true, requiere_epp: true,
        requiere_bloqueo: true, requiere_evidencia: true,
      }),
    })]);
  });

  it('actualiza el plan de una plantilla cambiada a SSA', async () => {
    const { service } = fixture({ clase_mantenimiento: 'SSA' }, { id: 'old-plan', tipo: 'PREVENTIVO' });
    expect((await service.syncPlanFromProcedimiento('procedure-ssa')).plan.tipo).toBe('SSA');
  });

  it('respeta una clase explicita y el comportamiento de plantillas anteriores', async () => {
    const explicit = fixture({ clase_mantenimiento: 'CORRECTIVO' });
    expect((await explicit.service.syncPlanFromProcedimiento('procedure-ssa')).plan.tipo).toBe('CORRECTIVO');
    const legacy = fixture({ tipo_proceso: 'MPG' }, { id: 'old-plan', tipo: 'CORRECTIVO' });
    expect((await legacy.service.syncPlanFromProcedimiento('procedure-ssa')).plan.tipo).toBe('CORRECTIVO');
  });
});

describe('OT de operador según el equipo SSA', () => {
  const actor = { roleName: 'OPERADOR', userId: 'operator-id', username: 'operador' };
  const equipment = { id: 'equipment', nombre: 'Unidad', marca_id: 'brand', equipo_tipo_id: 'type', location_id: 'location' };

  function operatorFixture(overrides: Record<string, unknown> = {}) {
    const { service } = fixture();
    Object.assign(service, {
      marcaRepo: { find: jest.fn().mockResolvedValue([{ id: 'brand', nombre: 'Marca' }]) },
      equipoTipoRepo: { find: jest.fn().mockResolvedValue([{ id: 'type', nombre: 'Generador' }]) },
      locationRepo: { find: jest.fn().mockResolvedValue([{ id: 'location', nombre: 'Central', codigo: 'LOC-01' }]) },
      ...overrides,
    });
    return service;
  }

  it.each(['codigo', 'nombre', 'nombre_real', 'descripcion', 'modelo', 'serie', 'codigo_lubricante', 'equipo_tipo_nombre', 'marca_nombre', 'ubicacion_label'])(
    'detecta SSA en el campo %s sin distinguir mayúsculas', field => {
      expect(operatorMaintenanceKindForEquipment({ [field]: 'Equipo ssa-01' })).toBe('SSA');
    },
  );

  it('revisa las secciones anidadas y conserva Cebado para los demás equipos', () => {
    expect(isSsaEquipment({ secciones: [{ detalle: { nombre: 'Seguridad SSA' } }] })).toBe(true);
    expect(operatorMaintenanceKindForEquipment({ nombre: 'Generador', marca_nombre: 'NISSAN', created_by: 'ssa', updated_by: 'ssa' })).toBe('CEBADO');
    expect(operatorMaintenanceKindForEquipment(null)).toBe('CEBADO');
  });

  it.each(['codigo', 'nombre', 'descripcion'])('resuelve SSA desde %s del tipo de equipo guardado en catálogo', async field => {
    const service = operatorFixture({ equipoTipoRepo: { find: jest.fn().mockResolvedValue([{ id: 'type', [field]: 'SSA' }]) } });
    const [row] = await service.attachEquipmentBrandNames([equipment]);
    expect(row.is_ssa).toBe(true);
    await expect(service.assertOperatorWorkOrderKind(actor, 'SSA', equipment)).resolves.toBeUndefined();
    await expect(service.assertOperatorWorkOrderKind(actor, 'CEBADO', equipment)).rejects.toThrow(/requiere.*SSA/);
  });

  it.each(['marcaRepo', 'locationRepo'])('usa los datos de %s del servidor para validar el tipo', async repo => {
    const id = repo === 'marcaRepo' ? 'brand' : 'location';
    const service = operatorFixture({ [repo]: { find: jest.fn().mockResolvedValue([{ id, nombre: 'SSA' }]) } });
    await expect(service.assertOperatorWorkOrderKind(actor, 'SSA', equipment)).resolves.toBeUndefined();
  });

  it('detecta SSA en un compartimiento del equipo y no lo propaga a otros equipos', async () => {
    const service = operatorFixture({ equipoComponenteRepo: { find: jest.fn().mockResolvedValue([
      { equipo_id: equipment.id, nombre: 'Armario', descripcion: 'Elementos SSA' },
    ]) } });
    const rows = await service.attachEquipmentBrandNames([equipment, { ...equipment, id: 'other' }]);
    expect(rows.map((row: any) => row.is_ssa)).toEqual([true, false]);
    await expect(service.assertOperatorWorkOrderKind(actor, 'SSA', equipment)).resolves.toBeUndefined();
  });

  it('exige Cebado para operador sin equipo SSA y mantiene los tipos de otros perfiles', async () => {
    const service = operatorFixture();
    await expect(service.assertOperatorWorkOrderKind(actor, 'CEBADO', equipment)).resolves.toBeUndefined();
    await expect(service.assertOperatorWorkOrderKind(actor, 'SSA', equipment)).rejects.toThrow(/requiere.*CEBADO/);
    await expect(service.assertOperatorWorkOrderKind(actor, 'CORRECTIVO', equipment)).rejects.toThrow(BadRequestException);
    await expect(service.assertOperatorWorkOrderKind({ ...actor, roleName: 'SUPERVISOR' }, 'CORRECTIVO', equipment)).resolves.toBeUndefined();
  });

  it('SSA admite un material normal y Cebado conserva la restricción a aceite', () => {
    const service = operatorFixture();
    const product = { id: 'product', nombre: 'Guantes de seguridad', es_aceite: false };
    expect(() => service.assertOilProductAllowedForWorkOrder({ code: 'OT-SSA', maintenance_kind: 'SSA' }, product)).not.toThrow();
    expect(() => service.assertOilProductAllowedForWorkOrder({ code: 'OT-CEBADO', maintenance_kind: 'CEBADO' }, product)).toThrow(/solo permite materiales marcados como aceite/);
  });

  it.each(['SSA', 'CEBADO'])('el creador puede acceder y cerrar su OT %s, sin dar esos permisos a otros operadores', async maintenance_kind => {
    const service = operatorFixture();
    const order = { id: 'order', maintenance_kind, requested_by: actor.userId, created_by: actor.username, valor_json: {} };
    service.woRepo = { findOne: jest.fn().mockResolvedValue(order) };
    await expect(service.canActorCloseOrVoidWorkOrder(order, actor)).resolves.toBe(true);
    await expect(service.assertOperatorAssignedToWorkOrder(order.id, actor)).resolves.toBeUndefined();
    await expect(service.canActorCloseOrVoidWorkOrder(order, { ...actor, userId: 'other', username: 'otro' })).resolves.toBe(false);
    await expect(service.canActorCloseOrVoidWorkOrder({ ...order, maintenance_kind: 'CORRECTIVO' }, actor)).resolves.toBe(false);
  });

  it('el listado incluye las OT SSA propias y conserva el filtro de identidad', async () => {
    const service = operatorFixture();
    const sql: string[] = [];
    const qb: any = { orderBy: jest.fn().mockReturnThis(), getMany: jest.fn().mockResolvedValue([]) };
    for (const method of ['where', 'andWhere', 'orWhere']) qb[method] = (condition: any) => {
      if (typeof condition === 'string') sql.push(condition);
      else condition.whereFactory(qb);
      return qb;
    };
    service.woRepo = { createQueryBuilder: jest.fn().mockReturnValue(qb) };
    service.buildSucursalScopeContext = jest.fn().mockResolvedValue(null);
    service.filterWorkOrdersByScope = jest.fn().mockResolvedValue([]);
    await service.listWorkOrders({}, null, actor);
    expect(sql).toContain("UPPER(COALESCE(wo.maintenance_kind, '')) IN ('CEBADO', 'SSA')");
    expect(sql.some(value => value.includes('wo.requested_by::text'))).toBe(true);
    expect(sql.some(value => value.includes('tarea.responsables'))).toBe(true);
  });
});
