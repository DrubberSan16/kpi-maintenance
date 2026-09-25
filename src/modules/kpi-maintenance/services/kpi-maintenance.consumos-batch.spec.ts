import {
  BadRequestException,
  ConflictException,
  HttpException,
} from '@nestjs/common';
import { KpiMaintenanceService } from './kpi-maintenance.service';

/**
 * Reservar varios materiales de una vez en una OT existente.
 *
 * La pantalla solo dejaba reservar un material por guardado. La reserva en
 * lote tiene que ser todo o nada: si un material de la lista no se puede
 * reservar, no queda reservado ninguno, y el mensaje dice cuál de la lista fue.
 */
type ServiceUnderTest = Record<string, any>;

const items = [
  { producto_id: 'prod-1', bodega_id: 'bod-1', cantidad: 2 },
  { producto_id: 'prod-2', bodega_id: 'bod-1', cantidad: 1.5, observacion: 'Urgente' },
  { producto_id: 'prod-3', bodega_id: 'bod-2', cantidad: 4 },
];

function createService(overrides: Record<string, any> = {}): ServiceUnderTest {
  const manager = { save: jest.fn(async (_entity: unknown, value: unknown) => value) };
  const workOrder = { id: 'wo-1', code: 'OT-1', status_workflow: 'PLANNED' };
  const service = Object.create(
    KpiMaintenanceService.prototype,
  ) as ServiceUnderTest;
  Object.assign(service, {
    manager,
    workOrder,
    woRepo: {},
    dataSource: { transaction: jest.fn(async (work: any) => work(manager)) },
    findOneOrFail: jest.fn().mockResolvedValue(workOrder),
    assertWorkOrderAllowsMaterialReservation: jest.fn(),
    assertOperatorAssignedToWorkOrder: jest.fn().mockResolvedValue(undefined),
    assertWorkOrderNotBlockedByActiveAnnex: jest.fn().mockResolvedValue(undefined),
    createConsumoWithManager: jest.fn(async (_manager: unknown, _wo: unknown, item: any) => ({
      saved: { id: `consumo-${item.producto_id}`, costo_unitario: '10.5' },
      producto: { id: item.producto_id },
      bodega: { id: item.bodega_id },
      subtotal: item.cantidad * 10.5,
    })),
    applyWorkOrderAuditStamp: jest.fn(),
    appendWorkOrderHistory: jest.fn().mockResolvedValue(undefined),
    resolveActorHistoryUserId: jest.fn().mockReturnValue('user-1'),
    writeSecurityLog: jest.fn().mockResolvedValue(undefined),
    queueWorkOrderConsumoEmail: jest.fn(),
    mapConsumoWithCatalogs: jest.fn((saved: any) => ({ id: saved.id })),
    ...overrides,
  });
  return service;
}

describe('KpiMaintenanceService reserva de varios materiales', () => {
  it('reserva todos en una sola transacción y avisa una sola vez', async () => {
    const service = createService();

    const result = await service.createConsumosBatch(
      'wo-1',
      { items },
      { username: 'bodega' },
    );

    expect(service.dataSource.transaction).toHaveBeenCalledTimes(1);
    expect(service.createConsumoWithManager).toHaveBeenCalledTimes(3);
    for (const [index, item] of items.entries()) {
      expect(service.createConsumoWithManager).toHaveBeenNthCalledWith(
        index + 1,
        service.manager,
        service.workOrder,
        item,
      );
    }
    expect(service.appendWorkOrderHistory).toHaveBeenCalledTimes(3);
    expect(service.queueWorkOrderConsumoEmail).toHaveBeenCalledTimes(1);
    const [, emailItems] = service.queueWorkOrderConsumoEmail.mock.calls[0];
    expect(emailItems).toEqual([
      expect.objectContaining({ producto_id: 'prod-1', costo_unitario: 10.5, subtotal: 21 }),
      expect.objectContaining({ producto_id: 'prod-2', observacion: 'Urgente', subtotal: 15.75 }),
      expect.objectContaining({ producto_id: 'prod-3', subtotal: 42 }),
    ]);
    expect(result.data).toEqual([
      { id: 'consumo-prod-1' },
      { id: 'consumo-prod-2' },
      { id: 'consumo-prod-3' },
    ]);
    expect(result.message).toBe('3 materiales reservados');
  });

  it('si un material falla no registra nada y dice cuál de la lista fue', async () => {
    const service = createService();
    service.createConsumoWithManager
      .mockImplementationOnce(async (_m: unknown, _w: unknown, item: any) => ({
        saved: { id: 'consumo-1', costo_unitario: 1 },
        producto: { id: item.producto_id },
        bodega: { id: item.bodega_id },
        subtotal: 1,
      }))
      .mockRejectedValueOnce(
        new ConflictException('El material no se puede usar en esta OT'),
      );

    const failure = service.createConsumosBatch('wo-1', { items });

    await expect(failure).rejects.toBeInstanceOf(HttpException);
    await failure.catch((error: HttpException) => {
      expect(error.getStatus()).toBe(409);
      expect(error.message).toBe(
        'No se reservó ningún material. Material 2 de 3: El material no se puede usar en esta OT',
      );
    });
    expect(service.createConsumoWithManager).toHaveBeenCalledTimes(2);
    expect(service.appendWorkOrderHistory).not.toHaveBeenCalled();
    expect(service.queueWorkOrderConsumoEmail).not.toHaveBeenCalled();
  });

  it('rechaza una lista vacía sin abrir la transacción', async () => {
    const service = createService();

    await expect(
      service.createConsumosBatch('wo-1', { items: [] }),
    ).rejects.toBeInstanceOf(BadRequestException);
    expect(service.dataSource.transaction).not.toHaveBeenCalled();
  });

  it('respeta las mismas reglas de la OT que la reserva de un material', async () => {
    const service = createService({
      assertWorkOrderAllowsMaterialReservation: jest.fn(() => {
        throw new ConflictException('La OT está cerrada');
      }),
    });

    await expect(
      service.createConsumosBatch('wo-1', { items }),
    ).rejects.toThrow('La OT está cerrada');
    expect(service.createConsumoWithManager).not.toHaveBeenCalled();
  });
});
