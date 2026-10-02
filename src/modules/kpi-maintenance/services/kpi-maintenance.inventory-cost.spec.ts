import { KpiMaintenanceService } from './kpi-maintenance.service';

function fixture(stock: any = null) {
  const product = { id: 'product', nombre: 'Filtro', ultimo_costo: 12.5, costo_promedio: 0 };
  const repo = () => ({ findOne: jest.fn(), create: jest.fn(value => value), save: jest.fn(async value => value) });
  const consumptionRepo = repo();
  const service: any = Object.create(KpiMaintenanceService.prototype);
  Object.assign(service, {
    productoRepo: { findOne: jest.fn().mockResolvedValue(product) },
    bodegaRepo: { findOne: jest.fn().mockResolvedValue({ id: 'warehouse', nombre: 'CPT' }) },
    stockRepo: { findOne: jest.fn().mockResolvedValue(stock) },
    kardexRepo: { findOne: jest.fn().mockResolvedValue(null) },
    consumoRepo: consumptionRepo,
    woRepo: repo(),
    dataSource: { query: jest.fn().mockResolvedValue([]) },
    upsertReservedMaterial: jest.fn().mockResolvedValue(undefined),
  });
  return { service, product, consumptionRepo };
}

describe('Costo automático de materiales de OT', () => {
  it('consulta un material sin fila de stock y recupera su costo de catálogo', async () => {
    const { service } = fixture();
    const result = await service.getInventoryCostReference('product', 'warehouse');
    expect(result.data.costo_unitario).toBe(12.5);
  });

  it('prioriza el costo de esa bodega sobre el costo general del material', async () => {
    const { service } = fixture({ costo_promedio_bodega: 8 });
    const result = await service.getInventoryCostReference('product', 'warehouse');
    expect(result.data.costo_unitario).toBe(8);
  });

  it('mantiene la prioridad del costo FIFO disponible', async () => {
    const { service } = fixture({ costo_promedio_bodega: 8 });
    jest.spyOn(service, 'resolveNextFifoLayerCost').mockResolvedValue(9);
    const result = await service.getInventoryCostReference('product', 'warehouse');
    expect(result.data.costo_unitario).toBe(9);
  });

  it.each([undefined, 0, 99])('el guardado transaccional toma Inventario aunque el cliente envíe %s', async (cost) => {
    const { service, product, consumptionRepo } = fixture();
    jest.spyOn(service, 'resolveReservationAvailability').mockResolvedValue({ producto: product, bodega: {} });
    jest.spyOn(service, 'resolveInventoryCostReference').mockResolvedValue({ costo_unitario: 12.5 });
    const manager = { getRepository: jest.fn().mockReturnValue(consumptionRepo) };
    const result = await service.createConsumoWithManager(manager,
      { id: 'order', status_workflow: 'PLANNED' },
      { producto_id: 'product', bodega_id: 'warehouse', cantidad: 3, costo_unitario: cost });
    expect(result.saved).toMatchObject({ costo_unitario: 12.5, subtotal: 37.5 });
  });

  it.each([undefined, 0, 99])('la reserva individual toma Inventario aunque el cliente envíe %s', async (cost) => {
    const { service, product, consumptionRepo } = fixture();
    service.findOneOrFail = jest.fn().mockResolvedValue({ id: 'order', status_workflow: 'PLANNED' });
    service.assertOperatorAssignedToWorkOrder = jest.fn();
    service.assertWorkOrderNotBlockedByActiveAnnex = jest.fn();
    service.applyWorkOrderAuditStamp = jest.fn();
    service.appendWorkOrderHistory = jest.fn();
    service.writeSecurityLog = jest.fn();
    service.queueWorkOrderConsumoEmail = jest.fn();
    jest.spyOn(service, 'resolveReservationAvailability').mockResolvedValue({ producto: product, bodega: {} });
    jest.spyOn(service, 'resolveInventoryCostReference').mockResolvedValue({ costo_unitario: 12.5 });
    await service.createConsumo('order',
      { producto_id: 'product', bodega_id: 'warehouse', cantidad: 3, costo_unitario: cost });
    expect(consumptionRepo.save).toHaveBeenCalledWith(expect.objectContaining({ costo_unitario: 12.5, subtotal: 37.5 }));
  });

  it('el costo de referencia se oculta al operador y permanece para administración', async () => {
    const { service } = fixture();
    const result = await service.getInventoryCostReference('product', 'warehouse');
    expect(service.omitirCostos(result, 'OPERADOR').data).not.toHaveProperty('costo_unitario');
    expect(service.omitirCostos(result, 'ADMINISTRADOR').data.costo_unitario).toBe(12.5);
  });
});
