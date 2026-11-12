import { BadRequestException } from '@nestjs/common';
import { KpiMaintenanceService } from './kpi-maintenance.service';
import {
  PlanTareaEntity,
  WorkOrderTareaEntity,
} from '../entities/kpi-maintenance.entity';

type ClosureHarness = Pick<
  KpiMaintenanceService,
  'saveWorkOrderBundle' | 'updateWorkOrder'
> & {
  assertWorkOrderTaskCapturesReadyForClosure: (
    manager: unknown,
    workOrderId: string,
  ) => Promise<void>;
  assertMaterialShortfallAcknowledged: jest.Mock<Promise<void>, unknown[]>;
  updateWorkOrderTareaWithManager: jest.Mock<Promise<void>, unknown[]>;
  releaseOpenReservationsForWorkOrder: jest.Mock<Promise<void>, unknown[]>;
};

function fixture(
  tasks: Record<string, unknown>[],
  definitions: Record<string, unknown>[] = [],
) {
  const service = Object.create(
    KpiMaintenanceService.prototype,
  ) as ClosureHarness;
  const taskRepo = { find: jest.fn().mockResolvedValue(tasks) };
  const definitionRepo = { find: jest.fn().mockResolvedValue(definitions) };
  const manager = {
    getRepository: jest.fn((entity: unknown) =>
      entity === WorkOrderTareaEntity ? taskRepo : definitionRepo,
    ),
  };
  return { service, taskRepo, definitionRepo, manager };
}

describe('Responsables obligatorios al finalizar la OT', () => {
  it.each([
    { tasks: [] },
    { tasks: [{ id: 'sin-responsable', responsables: [] }] },
    {
      tasks: [
        {
          id: 'sin-identidad',
          responsables: [{ display_name: 'Nombre suelto', horas: 2 }],
        },
      ],
    },
    {
      tasks: [
        { id: 'blancos', responsables: [{ empleado_id: ' ', user_id: '' }] },
      ],
    },
  ])('rechaza la OT sin responsable identificado: %j', async ({ tasks }) => {
    const { service, manager } = fixture(tasks);
    await expect(
      service.assertWorkOrderTaskCapturesReadyForClosure(manager, 'ot-1'),
    ).rejects.toThrow(/Asigna al menos un responsable/);
  });

  it('consulta únicamente tareas vivas de la OT que se cierra', async () => {
    const { service, manager, taskRepo } = fixture([]);
    await expect(
      service.assertWorkOrderTaskCapturesReadyForClosure(manager, 'ot-1'),
    ).rejects.toThrow(BadRequestException);
    expect(taskRepo.find).toHaveBeenCalledWith({
      where: { work_order_id: 'ot-1', is_deleted: false },
    });
  });

  it.each([
    { empleado_id: 'empleado-1', horas: 0 },
    { user_id: 'usuario-historico', horas: 0 },
  ])(
    'acepta responsable histórico o empleado sin imponer horas positivas: %o',
    async (responsible) => {
      const { service, manager } = fixture([
        { id: 'tarea-1', responsables: [responsible] },
      ]);
      await expect(
        service.assertWorkOrderTaskCapturesReadyForClosure(manager, 'ot-1'),
      ).resolves.toBeUndefined();
    },
  );

  it('acepta un responsable global aunque otras tareas no tengan responsables', async () => {
    const { service, manager } = fixture([
      { id: 'tarea-1', responsables: [] },
      {
        id: 'adicional',
        es_adicional: true,
        responsables: [{ empleado_id: 'empleado-1' }],
      },
    ]);
    await expect(
      service.assertWorkOrderTaskCapturesReadyForClosure(manager, 'ot-1'),
    ).resolves.toBeUndefined();
  });

  it('sigue exigiendo las capturas obligatorias aun con responsables', async () => {
    const { service, manager, definitionRepo } = fixture(
      [
        {
          id: 'tarea-1',
          tarea_id: 'plan-tarea-1',
          responsables: [{ empleado_id: 'empleado-1' }],
        },
      ],
      [
        {
          id: 'plan-tarea-1',
          actividad: 'Medir presión',
          required: true,
          field_type: 'NUMBER',
        },
      ],
    );
    await expect(
      service.assertWorkOrderTaskCapturesReadyForClosure(manager, 'ot-1'),
    ).rejects.toThrow(/captura obligatoria/);
    expect(manager.getRepository).toHaveBeenCalledWith(PlanTareaEntity);
    expect(definitionRepo.find).toHaveBeenCalled();
  });

  it('el guardado de conjunto comprueba las tareas después de guardar sus responsables', async () => {
    const { service, manager, taskRepo } = fixture([]);
    const order = { id: 'ot-1', status_workflow: 'CLOSED' };
    Object.assign(service, {
      dataSource: {
        transaction: jest.fn(
          (callback: (transaction: typeof manager) => Promise<unknown>) =>
            callback(manager),
        ),
      },
      logger: { error: jest.fn() },
      saveWorkOrderHeaderWithManager: jest
        .fn()
        .mockResolvedValue({ workOrder: order, isNew: false }),
      updateWorkOrderTareaWithManager: jest.fn(() => {
        taskRepo.find.mockResolvedValue([
          { id: 'tarea-1', responsables: [{ empleado_id: 'empleado-1' }] },
        ]);
        return Promise.resolve();
      }),
      // Detener la prueba tras la validación; evita simular correos y efectos posteriores.
      assertMaterialShortfallAcknowledged: jest
        .fn()
        .mockRejectedValue(new BadRequestException('materiales-pendientes')),
    });
    await expect(
      service.saveWorkOrderBundle('ot-1', {
        header: {},
        tareas_editadas: [{ id: 'tarea-1' }],
      }),
    ).rejects.toThrow(/materiales-pendientes/);
    expect(service.assertMaterialShortfallAcknowledged).toHaveBeenCalled();
    expect(
      service.updateWorkOrderTareaWithManager.mock.invocationCallOrder[0],
    ).toBeLessThan(taskRepo.find.mock.invocationCallOrder[0]);
  });

  it('el conjunto sin responsables se rechaza antes de liberar reservas', async () => {
    const { service, manager } = fixture([{ id: 'tarea-1', responsables: [] }]);
    Object.assign(service, {
      dataSource: {
        transaction: jest.fn(
          (callback: (transaction: typeof manager) => Promise<unknown>) =>
            callback(manager),
        ),
      },
      logger: { error: jest.fn() },
      saveWorkOrderHeaderWithManager: jest.fn().mockResolvedValue({
        workOrder: { id: 'ot-1', status_workflow: 'CLOSED' },
        isNew: false,
      }),
      releaseOpenReservationsForWorkOrder: jest.fn(),
      assertMaterialShortfallAcknowledged: jest.fn(),
    });
    await expect(
      service.saveWorkOrderBundle('ot-1', { header: {} }),
    ).rejects.toThrow(/Asigna al menos un responsable/);
    expect(service.releaseOpenReservationsForWorkOrder).not.toHaveBeenCalled();
    expect(service.assertMaterialShortfallAcknowledged).not.toHaveBeenCalled();
  });

  it.each(['PLANNED', 'REVIEW'])('rechaza finalizar desde %s sin impresión, por ambos guardados', async status => {
    const { service, manager } = fixture([]);
    const order = { id: 'ot-1', status_workflow: status, valor_json: {} };
    const lockedManager = {
      ...manager,
      findOne: jest.fn().mockResolvedValue(order),
      save: jest.fn(),
    };
    Object.assign(service, {
      dataSource: { transaction: jest.fn(async callback => callback(lockedManager)) },
      findOneOrFail: jest.fn().mockResolvedValue(order),
      assertOperatorAssignedToWorkOrder: jest.fn(),
      assertWorkOrderNotBlockedByActiveAnnex: jest.fn(),
      logger: { error: jest.fn() },
    });
    const header = { status_workflow: 'CLOSED', valor_json: { execution_start: { issue_document_ids: ['supuesto-egreso'] } } };
    await expect(service.updateWorkOrder('ot-1', header)).rejects.toThrow(/confirmar la impresión/);
    await expect(service.saveWorkOrderBundle('ot-1', { header })).rejects.toThrow(/confirmar la impresión/);
    expect(lockedManager.save).not.toHaveBeenCalled();
    expect(order.status_workflow).toBe(status);
  });

  it('la actualización directa valida responsables con la cabecera bloqueada antes de persistir el cierre', async () => {
    const { service, manager, taskRepo } = fixture([
      { id: 'tarea-1', responsables: [] },
    ]);
    const order = {
      id: 'ot-1',
      status_workflow: 'IN_PROGRESS',
      maintenance_kind: 'CORRECTIVO',
      valor_json: {
        causa: 'Desgaste',
        accion: 'Cambio',
        prevencion: 'Inspección',
        execution_start: { issue_document_ids: ['egreso'] },
      },
    };
    const lockedManager = {
      ...manager,
      findOne: jest.fn().mockResolvedValue({ ...order }),
      save: jest.fn(),
    };
    Object.assign(service, {
      WORK_ORDER_MAINTENANCE_KIND_VALUES: [
        'CORRECTIVO',
        'PREVENTIVO',
        'PREDICTIVO',
        'CEBADO',
        'PROYECTO',
        'SSA',
      ],
      dataSource: {
        transaction: jest.fn(
          (callback: (transaction: typeof lockedManager) => Promise<unknown>) =>
            callback(lockedManager),
        ),
      },
      findOneOrFail: jest.fn().mockResolvedValue({ ...order }),
      assertCanCloseOrVoidWorkOrder: jest.fn(),
      assertOperatorAssignedToWorkOrder: jest.fn(),
      assertWorkOrderNotBlockedByActiveAnnex: jest.fn(),
      resolveProcedimientoFromPlan: jest.fn().mockResolvedValue(null),
      resolveWorkOrderComponentContexts: jest
        .fn()
        .mockResolvedValue({ snapshots: [] }),
      assertMaterialShortfallAcknowledged: jest.fn(),
      buildAutomaticWorkOrderHorometerPayload: jest.fn(
        (payload: Record<string, unknown>) => Promise.resolve(payload),
      ),
      applyBlockingRelationship: jest.fn(),
    });
    await expect(
      service.updateWorkOrder('ot-1', { status_workflow: 'CLOSED' }),
    ).rejects.toThrow(/Asigna al menos un responsable/);
    expect(lockedManager.findOne).toHaveBeenCalledWith(expect.anything(), {
      where: { id: 'ot-1', is_deleted: false },
      lock: { mode: 'pessimistic_write' },
    });
    expect(lockedManager.findOne.mock.invocationCallOrder[0]).toBeLessThan(
      taskRepo.find.mock.invocationCallOrder[0],
    );
    expect(lockedManager.save).not.toHaveBeenCalled();
  });
});
