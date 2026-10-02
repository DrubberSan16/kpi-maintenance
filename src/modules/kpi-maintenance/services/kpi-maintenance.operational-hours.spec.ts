import { KpiMaintenanceService } from './kpi-maintenance.service';
import { EquipoEntity, WorkOrderEntity } from '../entities/kpi-maintenance.entity';

const id = '11111111-1111-4111-8111-111111111111';
function fixture(state = 'PARADO') {
  const equipment = { id, estado_funcionamiento: state, horometro_actual: 1000 };
  const order = { id: '22222222-2222-4222-8222-222222222222', equipment_id: id, code: 'OT-TEST', maintenance_kind: 'CORRECTIVO', status_workflow: 'IN_PROGRESS', valor_json: { causa: 'Desgaste', accion: 'Cambio de filtro', prevencion: 'Inspección periódica' } };
  const equipmentRepo = { findOne: jest.fn().mockResolvedValue(equipment), save: jest.fn().mockImplementation(async row => row) };
  const orderRepo = { find: jest.fn().mockResolvedValue([order]), save: jest.fn().mockImplementation(async row => row) };
  const historyRepo = { create: jest.fn(row => row), save: jest.fn().mockResolvedValue({}) };
  const manager = { getRepository: jest.fn(entity => entity === EquipoEntity ? equipmentRepo : entity === WorkOrderEntity ? orderRepo : historyRepo) };
  const service = Object.create(KpiMaintenanceService.prototype) as any;
  Object.assign(service, {
    dataSource: { transaction: jest.fn(async callback => callback(manager)) },
    assertCanCloseOrVoidWorkOrder: jest.fn().mockResolvedValue(undefined),
    assertWorkOrderNotBlockedByActiveAnnex: jest.fn().mockResolvedValue(undefined),
    assertWorkOrderTaskCapturesReadyForClosure: jest.fn().mockResolvedValue(undefined),
    assertMaterialShortfallAcknowledged: jest.fn().mockResolvedValue(undefined),
    releaseOpenReservationsForWorkOrder: jest.fn().mockResolvedValue(0),
    releaseBlockedWorkOrdersFor: jest.fn().mockResolvedValue(undefined),
    syncProgramacionExecutionFromLinkedWorkOrder: jest.fn().mockResolvedValue(undefined),
    syncAlertsForWorkOrder: jest.fn().mockResolvedValue(undefined),
    triggerAlertRecalculation: jest.fn().mockResolvedValue(undefined),
  });
  return { service, manager, equipment, order, orderRepo, equipmentRepo, historyRepo };
}

describe('Horas operativas y cierre al encender', () => {
  it('acumula solo mientras funciona y conserva lecturas sin referencia temporal', () => {
    const service = Object.create(KpiMaintenanceService.prototype) as any;
    const now = new Date('2026-10-01T15:00:00Z');
    const equipment = { horometro_actual: 1000, estado_funcionamiento: 'FUNCIONAMIENTO', horometro_operativo_desde: new Date('2026-10-01T13:00:00Z') };
    expect(service.operationalHorometer(equipment, now)).toBe(1002);
    expect(service.operationalHorometer({ ...equipment, estado_funcionamiento: 'PARADO' }, now)).toBe(1000);
    expect(service.operationalHorometer({ ...equipment, horometro_operativo_desde: null }, now)).toBe(1000);
    expect(service.operationalHorometer(equipment, new Date('2026-10-01T12:00:00Z'))).toBe(1000);
  });
  it('al encender congela la OT y libera reservas dentro de la misma transacción', async () => {
    const { service, manager, order, orderRepo, historyRepo } = fixture();
    await service.updateEquipoEstadoFuncionamiento(id, { estado_funcionamiento: 'FUNCIONAMIENTO' }, { userId: id, displayName: 'Operador' });
    expect(order.status_workflow).toBe('CLOSED');
    expect(order.valor_json).toMatchObject({ horometro_actual: 1000, cierre_por_encendido: true });
    expect(orderRepo.save).toHaveBeenCalledWith(order);
    expect(service.releaseOpenReservationsForWorkOrder).toHaveBeenCalledWith(order.id, manager, id);
    expect(historyRepo.save).toHaveBeenCalledWith(expect.objectContaining({ from_status: 'IN_PROGRESS', to_status: 'CLOSED' }));
    expect(service.syncProgramacionExecutionFromLinkedWorkOrder).toHaveBeenCalledWith(order);
  });
  it('no enciende ni cierra si falta una captura obligatoria', async () => {
    const { service, equipmentRepo, orderRepo } = fixture();
    service.assertWorkOrderTaskCapturesReadyForClosure.mockRejectedValue(new Error('Captura pendiente'));
    await expect(service.updateEquipoEstadoFuncionamiento(id, { estado_funcionamiento: 'FUNCIONAMIENTO' })).rejects.toThrow('Captura pendiente');
    expect(equipmentRepo.save).not.toHaveBeenCalled();
    expect(orderRepo.save).not.toHaveBeenCalled();
  });
  it('apagar no finaliza las OT y repetir el estado no duplica eventos', async () => {
    const running = fixture('FUNCIONAMIENTO');
    await running.service.updateEquipoEstadoFuncionamiento(id, { estado_funcionamiento: 'PARADO' });
    expect(running.orderRepo.save).not.toHaveBeenCalled();
    const stopped = fixture();
    await stopped.service.updateEquipoEstadoFuncionamiento(id, { estado_funcionamiento: 'PARADO' });
    expect(stopped.equipmentRepo.save).not.toHaveBeenCalled();
    expect(stopped.historyRepo.save).not.toHaveBeenCalled();
  });
  it.each(['assertCanCloseOrVoidWorkOrder', 'assertWorkOrderNotBlockedByActiveAnnex', 'assertMaterialShortfallAcknowledged'])('no enciende si falla %s', async guard => {
    const { service, equipmentRepo, orderRepo } = fixture();
    service[guard].mockRejectedValue(new Error('Cierre no permitido'));
    await expect(service.updateEquipoEstadoFuncionamiento(id, { estado_funcionamiento: 'FUNCIONAMIENTO' })).rejects.toThrow('Cierre no permitido');
    expect(equipmentRepo.save).not.toHaveBeenCalled();
    expect(orderRepo.save).not.toHaveBeenCalled();
  });
  it('el encendido no cierra proyectos ni OT anuladas', async () => {
    for (const project of [true, false]) {
      const { service, order, orderRepo } = fixture();
      if (project) order.maintenance_kind = 'PROYECTO';
      else Object.assign(order.valor_json, { approval_action: 'ANULADA' });
      await service.updateEquipoEstadoFuncionamiento(id, { estado_funcionamiento: 'FUNCIONAMIENTO' });
      expect(orderRepo.save).not.toHaveBeenCalled();
    }
  });
  it('conserva los campos obligatorios de causa, acción y prevención al cerrar', async () => {
    const { service, order, equipmentRepo } = fixture();
    order.valor_json.causa = '';
    await expect(service.updateEquipoEstadoFuncionamiento(id, { estado_funcionamiento: 'FUNCIONAMIENTO' })).rejects.toThrow();
    expect(equipmentRepo.save).not.toHaveBeenCalled();
  });
  it('la edición general del equipo también cierra la OT al reencender', async () => {
    const { service, equipment, order } = fixture();
    service.findEquipoOrFail = jest.fn().mockResolvedValue(equipment);
    service.resolveEquipmentServiceSchedule = jest.fn().mockReturnValue({});
    await service.updateEquipo(id, { estado_funcionamiento: 'FUNCIONAMIENTO' }, { userId: id });
    expect(order.status_workflow).toBe('CLOSED');
  });
  it('editar datos del equipo conserva las fracciones acumuladas del horómetro', async () => {
    const { service, equipment } = fixture();
    equipment.horometro_actual = 1000.25;
    service.findEquipoOrFail = jest.fn().mockResolvedValue(equipment);
    service.resolveEquipmentServiceSchedule = jest.fn().mockReturnValue({});
    await service.updateEquipo(id, { nombre: 'UG21' });
    expect(equipment.horometro_actual).toBe(1000.25);
  });
});
