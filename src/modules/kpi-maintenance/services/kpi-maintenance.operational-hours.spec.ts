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

describe('Horas operativas independientes de las OT', () => {
  it('acumula solo mientras funciona y conserva lecturas sin referencia temporal', () => {
    const service = Object.create(KpiMaintenanceService.prototype) as any;
    const now = new Date('2026-10-01T15:00:00Z');
    const equipment = { horometro_actual: 1000, estado_funcionamiento: 'FUNCIONAMIENTO', horometro_operativo_desde: new Date('2026-10-01T13:00:00Z') };
    expect(service.operationalHorometer(equipment, now)).toBe(1002);
    expect(service.operationalHorometer({ ...equipment, estado_funcionamiento: 'PARADO' }, now)).toBe(1000);
    expect(service.operationalHorometer({ ...equipment, horometro_operativo_desde: null }, now)).toBe(1000);
    expect(service.operationalHorometer(equipment, new Date('2026-10-01T12:00:00Z'))).toBe(1000);
  });
  it.each(['IN_PROGRESS', 'REVIEW', 'BLOCKED'])('permite controlar el equipo con OT en %s sin cerrar ni liberar reservas', async state => {
    const { service, order, orderRepo, equipmentRepo, historyRepo } = fixture();
    order.status_workflow = state;
    await service.updateEquipoEstadoFuncionamiento(id, { estado_funcionamiento: 'FUNCIONAMIENTO' }, { userId: id });
    expect(order.status_workflow).toBe(state);
    expect(orderRepo.save).not.toHaveBeenCalled();
    expect(equipmentRepo.save).toHaveBeenCalledWith(expect.objectContaining({ estado_funcionamiento: 'FUNCIONAMIENTO' }));
    expect(historyRepo.save).toHaveBeenCalledTimes(1);
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
  it('la edición del estado del equipo conserva independiente el estado de la OT activa', async () => {
    const { service, equipment, order, equipmentRepo } = fixture();
    service.findEquipoOrFail = jest.fn().mockResolvedValue(equipment);
    service.resolveEquipmentServiceSchedule = jest.fn().mockReturnValue({});
    await service.updateEquipo(id, { estado_funcionamiento: 'FUNCIONAMIENTO' }, { userId: id });
    expect(order.status_workflow).toBe('IN_PROGRESS');
    expect(equipmentRepo.save).toHaveBeenCalledWith(expect.objectContaining({ estado_funcionamiento: 'FUNCIONAMIENTO' }));
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

describe('Horómetro automático de las OT', () => {
  it('la planificación conserva la última OT y deja pendiente la captura sin modificar el equipo', async () => {
    jest.useFakeTimers().setSystemTime(new Date('2026-10-04T17:30:00Z'));
    try {
      const { service, equipment, equipmentRepo } = fixture('FUNCIONAMIENTO');
      Object.assign(equipment, { horometro_actual: 1000.125, horometro_operativo_desde: new Date('2026-10-04T16:00:00Z') });
      service.woRepo = { findOne: jest.fn().mockResolvedValue({ code: 'OT-ANTERIOR', valor_json: { horometro_actual: 980.5 } }) };
      const snapshot = await service.buildAutomaticWorkOrderHorometerPayload(
        { horometro_actual: 10, horometro_anterior: 20, horas_a_realizar: 999 }, equipment,
        { frecuencia_horas: 1.75 }, { maintenanceKind: 'CEBADO' },
      );
      expect(snapshot).toMatchObject({ horometro_actual: null, horometro_capturado_en: null, horometro_anterior: 980.5, horas_plantilla: 1.75 });
      expect(equipment.horometro_actual).toBe(1000.125);
      expect(equipmentRepo.save).not.toHaveBeenCalled();
      const edited = await service.buildAutomaticWorkOrderHorometerPayload(
        { horometro_actual: 90000, cebado_horometro: { horas: 999, pendiente: true } }, equipment, null,
        { storedPayload: snapshot },
      );
      expect(edited.horometro_actual).toBe(snapshot.horometro_actual);
      expect(edited.horometro_anterior).toBe(snapshot.horometro_anterior);
      expect(edited.cebado_horometro).toBeUndefined();
    } finally { jest.useRealTimers(); }
  });
  it('conserva segundos al parar: acumula fracciones sin redondear cada hora a entero', () => {
    const { service, equipment } = fixture('FUNCIONAMIENTO');
    Object.assign(equipment, { horometro_actual: 1000.123456, horometro_operativo_desde: new Date('2026-10-04T16:00:00Z') });
    expect(service.operationalHorometer(equipment, new Date('2026-10-04T16:00:01Z'))).toBe(1000.123734);
    expect(service.operationalHorometer({ ...equipment, estado_funcionamiento: 'PARADO' }, new Date('2026-10-04T20:00:00Z'))).toBe(1000.123456);
  });
  it('suma las horas de cebado al encender una sola vez y conserva la lectura histórica de la OT', async () => {
    const { service, equipment, order, orderRepo, historyRepo } = fixture();
    const closed = { ...order, maintenance_kind: 'CEBADO', status_workflow: 'CLOSED', started_at: new Date(),
      valor_json: { horometro_actual: 1000, horometro_anterior: 950, cebado_horometro: { horas: 2.5, pendiente: true } } };
    orderRepo.find.mockImplementation(async options => Array.isArray(options.where) ? [] : [closed]);
    await service.updateEquipoEstadoFuncionamiento(id, { estado_funcionamiento: 'FUNCIONAMIENTO' }, { displayName: 'Supervisor' });
    expect(equipment.horometro_actual).toBe(1002.5);
    expect(closed.valor_json.horometro_actual).toBe(1000);
    expect(closed.valor_json.cebado_horometro.pendiente).toBe(false);
    expect(historyRepo.save).toHaveBeenCalledWith(expect.objectContaining({ fuente: 'CEBADO_AUTOMATICO', horometro_nuevo: 1002.5 }));
    equipment.estado_funcionamiento = 'PARADO';
    await service.updateEquipoEstadoFuncionamiento(id, { estado_funcionamiento: 'FUNCIONAMIENTO' });
    expect(equipment.horometro_actual).toBe(1002.5);
    expect(orderRepo.save).toHaveBeenCalledTimes(1);
  });
  it.each(['CORRECTIVO', 'ANULADA'])('no añade horas para %s', async kind => {
    const { service, equipment, order, orderRepo } = fixture();
    const closed = { ...order, status_workflow: 'CLOSED', status: kind === 'ANULADA' ? 'ANULADA' : 'ACTIVE',
      maintenance_kind: kind === 'ANULADA' ? 'CEBADO' : kind,
      valor_json: { cebado_horometro: { horas: 10, pendiente: true } } };
    orderRepo.find.mockImplementation(async options => Array.isArray(options.where) ? [] : [closed]);
    await service.updateEquipoEstadoFuncionamiento(id, { estado_funcionamiento: 'FUNCIONAMIENTO' });
    expect(equipment.horometro_actual).toBe(1000);
    expect(orderRepo.save).not.toHaveBeenCalled();
  });
});
