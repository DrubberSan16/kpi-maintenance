import { KpiMaintenanceService } from './kpi-maintenance.service';
import { WorkOrderTareaEntity } from '../entities/kpi-maintenance.entity';
describe('Responsables obligatorios al cerrar OT', () => {
  const employee = '11111111-1111-4111-8111-111111111111';
  function fixture(tasks: any[]) {
    const service = Object.create(KpiMaintenanceService.prototype) as any;
    const manager = { getRepository: (entity: unknown) => ({ find: jest.fn().mockResolvedValue(entity === WorkOrderTareaEntity ? tasks : []) }) };
    return () => service.assertWorkOrderTaskCapturesReadyForClosure(manager, 'work-order');
  }
  it.each([{ tasks: [] }, { tasks: [{ responsables: [] }] }, { tasks: [{ responsables: [{ display_name: 'Nombre sin identidad' }] }] }])('rechaza una OT sin personas asignadas: %j', async ({ tasks }) => {
    await expect(fixture(tasks)()).rejects.toThrow('Asigna al menos un responsable');
  });
  it('permite un empleado asignado y una captura válida', async () => {
    await expect(fixture([{ responsables: [{ empleado_id: employee, display_name: 'Técnico', horas: 1 }], task_meta: { required: true, field_type: 'BOOLEAN' }, valor_boolean: true }])()).resolves.toBeUndefined();
  });
  it('sigue exigiendo capturas aunque haya responsables', async () => {
    await expect(fixture([{ id: 'tarea', responsables: [{ empleado_id: employee, horas: 1 }], task_meta: { required: true, field_type: 'TEXT' }, valor_text: '' }])()).rejects.toThrow('Completa la captura obligatoria');
  });
});
