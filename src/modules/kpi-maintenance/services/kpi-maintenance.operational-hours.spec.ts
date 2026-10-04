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
  it.each(['IN_PROGRESS', 'REVIEW', 'BLOCKED'])('rechaza encender con una OT activa en %s sin cerrar ni liberar reservas', async state => {
    const { service, order, orderRepo, equipmentRepo, historyRepo } = fixture();
    order.status_workflow = state;
    await expect(service.updateEquipoEstadoFuncionamiento(id, { estado_funcionamiento: 'FUNCIONAMIENTO' }, { userId: id })).rejects.toThrow('Finaliza o anula');
    expect(order.status_workflow).toBe(state);
    expect(orderRepo.save).not.toHaveBeenCalled();
    expect(equipmentRepo.save).not.toHaveBeenCalled();
    expect(historyRepo.save).not.toHaveBeenCalled();
    expect(service.releaseOpenReservationsForWorkOrder).not.toHaveBeenCalled();
  });
  it('permite registrar el encendido cuando ninguna OT está activa', async () => {
    const { service, orderRepo, equipmentRepo } = fixture();
    orderRepo.find.mockResolvedValue([]);
    await service.updateEquipoEstadoFuncionamiento(id, { estado_funcionamiento: 'FUNCIONAMIENTO' });
    expect(equipmentRepo.save).toHaveBeenCalledWith(expect.objectContaining({ estado_funcionamiento: 'FUNCIONAMIENTO' }));
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
  it('la edición general del equipo tampoco permite encender mientras una OT sigue activa', async () => {
    const { service, equipment, order, equipmentRepo } = fixture();
    service.findEquipoOrFail = jest.fn().mockResolvedValue(equipment);
    service.resolveEquipmentServiceSchedule = jest.fn().mockReturnValue({});
    await expect(service.updateEquipo(id, { estado_funcionamiento: 'FUNCIONAMIENTO' }, { userId: id })).rejects.toThrow('Finaliza o anula');
    expect(order.status_workflow).toBe('IN_PROGRESS');
    expect(equipmentRepo.save).not.toHaveBeenCalled();
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
