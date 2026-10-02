import { BadRequestException } from '@nestjs/common';
import { KpiMaintenanceService } from './kpi-maintenance.service';

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
