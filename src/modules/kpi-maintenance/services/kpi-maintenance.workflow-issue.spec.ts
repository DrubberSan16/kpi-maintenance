import { BadRequestException, ConflictException, ForbiddenException } from '@nestjs/common';
import { KpiMaintenanceService } from './kpi-maintenance.service';
import { EquipoEntity, WorkOrderEntity, WorkOrderStatusHistoryEntity, EquipoFuncionamientoHistorialEntity } from '../entities/kpi-maintenance.entity';

const actor = { roleName: 'BODEGA', userId: '11111111-1111-4111-8111-111111111111', username: 'almacen', displayName: 'Ana Bodega' };
function fixture(state = 'PLANNED') {
  const wo: any = { id: 'wo', code: 'OT-42', equipment_id: 'eq', maintenance_kind: 'CEBADO', status_workflow: state, valor_json: {}, created_by: 'creador' };
  const equipment: any = { id: 'eq', estado_funcionamiento: 'FUNCIONAMIENTO' };
  const saves: any[] = [];
  const manager = {
    findOne: jest.fn(async (entity, options) => entity === EquipoEntity ? equipment : options.where.id === 'wo' ? wo : null),
    find: jest.fn().mockResolvedValue([{ id: 'entrega' }]), count: jest.fn().mockResolvedValue(1),
    create: jest.fn((_entity, value) => value), save: jest.fn(async (entity, value) => { saves.push({ entity, value: structuredClone(value) }); return value; }),
  };
  const service: any = Object.create(KpiMaintenanceService.prototype);
  Object.assign(service, {
    dataSource: { transaction: jest.fn(async callback => callback(manager)) },
    assertWorkOrderVisibleForSucursal: jest.fn(), assertWorkOrderNotBlockedByActiveAnnex: jest.fn(),
    findWorkOrderIssueMovements: jest.fn().mockResolvedValue([{ id: 'egreso', numero_documento: 'EB-42' }]),
    listWorkOrderIssueDocuments: jest.fn().mockResolvedValue({ data: [{ id: 'egreso' }] }),
    syncProgramacionExecutionFromLinkedWorkOrder: jest.fn(), syncAlertsForWorkOrder: jest.fn(),
    logger: { warn: jest.fn() }, MATERIAL_ISSUE_ROLES: ["BODEGA", "BODEGUERO", "SUPER ADMINISTRADOR", "SUPERADMINISTRADOR", "SUPER_ADMINISTRADOR", "SUPER ADMIN", "SUPER_ADMIN"],
  });
  return { service, manager, wo, equipment, saves };
}

describe('Flujo automático de OT y egreso', () => {
  it('crea planificada e impide iniciar por una edición de estado', () => {
    const { service } = fixture();
    expect(service.resolveManualWorkOrderStatus(null, 'IN_PROGRESS', true)).toBe('PLANNED');
    expect(() => service.resolveManualWorkOrderStatus('PLANNED', 'IN_PROGRESS')).toThrow(ForbiddenException);
    expect(service.resolveManualWorkOrderStatus('IN_PROGRESS', 'REVIEW')).toBe('REVIEW');
    expect(() => service.resolveManualWorkOrderStatus('REVIEW', 'IN_PROGRESS')).toThrow(ForbiddenException);
  });
  it.each(['ADMINISTRADOR', 'GERENTE GENERAL', 'SUPERVISOR'])('no permite a %s confirmar el egreso', async roleName => {
    const { service } = fixture();
    await expect(service.confirmWorkOrderIssue('wo', { ...actor, roleName })).rejects.toThrow(ForbiddenException);
    expect(service.dataSource.transaction).not.toHaveBeenCalled();
  });
  it('imprimir inicia la OT, apaga el equipo y registra al usuario de bodega en una transacción', async () => {
    const { service, wo, equipment, saves } = fixture();
    const now = new Date();
    Object.assign(equipment, { horometro_actual: 1000.25, horometro_operativo_desde: new Date(now.getTime() - 3600000) });
    wo.valor_json = { horometro_actual: 999, horometro_anterior: 900, horas_plantilla: 1.5 };
    await service.confirmWorkOrderIssue('wo', actor);
    expect(wo.status_workflow).toBe('IN_PROGRESS');
    expect(wo.valor_json.execution_start).toMatchObject({ by_name: 'Ana Bodega', by_user_id: actor.userId });
    expect(equipment.estado_funcionamiento).toBe('PARADO');
    expect(wo.valor_json.horometro_actual).toBeCloseTo(1001.25, 3);
    expect(equipment.horometro_actual).toBe(wo.valor_json.horometro_actual);
    expect(wo.valor_json.horometro_anterior).toBe(900);
    expect(wo.valor_json.cebado_horometro).toMatchObject({ horas: 1.5, pendiente: true });
    expect(saves.filter(row => row.entity === WorkOrderStatusHistoryEntity)[0].value).toMatchObject({ from_status: 'PLANNED', to_status: 'IN_PROGRESS', changed_by: actor.userId });
    expect(saves.some(row => row.entity === EquipoFuncionamientoHistorialEntity)).toBe(true);
    for (const row of saves.filter(row => [WorkOrderStatusHistoryEntity, EquipoFuncionamientoHistorialEntity].includes(row.entity))) {
      expect(row.value.id).toMatch(/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i);
    }
  });
  it('reanudar desde revisión agrega un evento y conserva el inicio y su actor originales', async () => {
    const { service, wo, saves } = fixture('REVIEW');
    wo.started_at = new Date('2026-10-03T14:00:00Z');
    wo.valor_json.execution_start = { at: wo.started_at.toISOString(), by_name: 'Inicio original' };
    await service.confirmWorkOrderIssue('wo', actor);
    expect(wo.started_at.toISOString()).toBe('2026-10-03T14:00:00.000Z');
    expect(wo.valor_json.execution_start.by_name).toBe('Inicio original');
    expect(saves.find(row => row.entity === WorkOrderStatusHistoryEntity).value).toMatchObject({ from_status: 'REVIEW', to_status: 'IN_PROGRESS', changed_by: actor.userId, note: expect.stringContaining('reanudada') });
  });
  it.each(['IN_PROGRESS', 'CLOSED'])('reimprimir en %s no modifica el inicio ni duplica eventos', async state => {
    const { service, manager } = fixture(state);
    await service.confirmWorkOrderIssue('wo', actor);
    expect(manager.save).not.toHaveBeenCalled();
    expect(service.listWorkOrderIssueDocuments).toHaveBeenCalled();
  });
  it('rechaza un egreso vacío sin cambiar la OT ni el equipo', async () => {
    const { service, wo, equipment, manager } = fixture();
    manager.count.mockResolvedValue(0);
    await expect(service.confirmWorkOrderIssue('wo', actor)).rejects.toThrow(BadRequestException);
    expect(wo.status_workflow).toBe('PLANNED');
    expect(equipment.estado_funcionamiento).toBe('FUNCIONAMIENTO');
    expect(manager.save).not.toHaveBeenCalled();
  });
  it('bloquea otra OT del mismo equipo antes de registrar cualquier modificación', async () => {
    const { service, manager } = fixture();
    manager.findOne.mockImplementation(async (entity, options) => entity === EquipoEntity ? { id: 'eq' } : options.where.id === 'wo' ? { id: 'wo', equipment_id: 'eq', status_workflow: 'PLANNED' } : { code: 'OT-ACTIVA' });
    await expect(service.confirmWorkOrderIssue('wo', actor)).rejects.toThrow(ConflictException);
    expect(manager.save).not.toHaveBeenCalled();
  });
  it('un administrador que editó la OT no obtiene permisos de su creador', async () => {
    const { service, wo } = fixture();
    wo.updated_by = 'administrador';
    wo.valor_json = { actor_username: 'administrador' };
    await expect(service.canActorCloseOrVoidWorkOrder(wo, { username: 'administrador', roleName: 'ADMINISTRADOR' })).resolves.toBe(false);
    await expect(service.canActorCloseOrVoidWorkOrder(wo, { username: 'creador', roleName: 'SUPERVISOR' })).resolves.toBe(true);
  });
  it('el cliente no puede reemplazar la identidad del creador ni del inicio', () => {
    const { service } = fixture();
    expect(service.protectWorkOrderLifecyclePayload({ created_by_username: 'creador', execution_start: { by_name: 'Ana' } }, { created_by_username: 'intruso', execution_start: { by_name: 'Otra' }, causa: 'Ajuste' })).toEqual({ created_by_username: 'creador', execution_start: { by_name: 'Ana' }, causa: 'Ajuste' });
  });
  it('Super Administrador puede imprimir el egreso y queda registrado como quien inició la OT', async () => {
    const { service, wo, equipment, saves } = fixture();
    const superAdmin = { ...actor, roleName: 'Súper Administrador', displayName: 'María Administradora' };
    await service.confirmWorkOrderIssue('wo', superAdmin);
    expect(wo.status_workflow).toBe('IN_PROGRESS');
    expect(wo.valor_json.execution_start).toMatchObject({ by_name: superAdmin.displayName, by_user_id: superAdmin.userId });
    expect(equipment.estado_funcionamiento).toBe('PARADO');
    expect(saves.find(row => row.entity === WorkOrderStatusHistoryEntity).value.changed_by).toBe(superAdmin.userId);
  });
  it('una salida adicional reutiliza el número y la apertura del egreso existente', async () => {
    const { service, manager, wo } = fixture('REVIEW');
    const opened = new Date('2026-10-03T15:00:00Z');
    const existing = { id: 'egreso', numero_documento: 'EB-42', fecha_movimiento: opened, bodega_origen_id: 'bod-1', observacion: 'Salida inicial' };
    service.findWorkOrderIssueMovements.mockResolvedValue([existing]);
    const repo = { save: jest.fn(async value => value), create: jest.fn() };
    Object.assign(manager, { getRepository: jest.fn(() => repo) });
    const result = await service.resolveWorkOrderIssueMovement(manager, wo, { items: [{ producto_id: 'nuevo', bodega_id: 'bod-1', cantidad: 1 }], observacion: 'Filtro adicional' }, actor.username, new Date());
    expect(result.id).toBe('egreso');
    expect(result.numero_documento).toBe('EB-42');
    expect(result.fecha_movimiento).toBe(opened);
    expect(result.updated_by).toBe(actor.username);
    expect(repo.create).not.toHaveBeenCalled();
  });
  it.each(['PLANNED', 'REVIEW'])('permite registrar salidas en %s', state => {
    const { service, wo } = fixture(state);
    expect(() => service.assertWorkOrderAllowsMaterialIssue(wo)).not.toThrow();
  });
  it('no permite nuevas salidas mientras la OT está en proceso', () => {
    const { service, wo } = fixture('IN_PROGRESS');
    expect(() => service.assertWorkOrderAllowsMaterialIssue(wo)).toThrow(ForbiddenException);
  });
});
