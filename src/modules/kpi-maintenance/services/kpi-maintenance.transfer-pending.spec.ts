import { KpiMaintenanceService } from './kpi-maintenance.service';

function fixture(pending = { nuevo: 6, usado: 2, critico: 0, total: 8 }) {
  const service: any = Object.create(KpiMaintenanceService.prototype);
  const manager = { query: jest.fn().mockResolvedValue([pending]) };
  const stock: any = { producto_id: 'material', bodega_id: 'origen', stock_actual: 15, stock_nuevo: 10, stock_usado: 5, stock_critico: 0, es_usado: true };
  service.dataSource = manager;
  service.resolveProductoBodegaParaReserva = jest.fn().mockResolvedValue({ producto: {}, bodega: {}, stock });
  service.getActiveReservedQuantity = jest.fn().mockResolvedValue(3);
  return { service, manager, stock };
}

describe('OT: stock comprometido por transferencias', () => {
  it('reserva la necesidad y calcula faltante con stock neto de compromisos de OT y traslado', async () => {
    const { service } = fixture();
    const result = await service.resolveReservationAvailability('material', 'origen', 8);
    expect(result).toMatchObject({ stockActual: 15, reservedQty: 3, pendingTransferQty: 8, availableQty: 4, faltante: 4 });
  });

  it.each([['NUEVO', 5], ['USADO', 4]])('impide entregar %s comprometido aunque el stock registrado alcance', async (condition, quantity) => {
    const { service, manager, stock } = fixture();
    await expect(service.assertStockNotCommittedToTransfer(manager, stock, quantity, condition, 'Filtro')).rejects.toThrow('pendiente de recepción');
    expect(stock.stock_actual).toBe(15);
    expect(manager.query).toHaveBeenCalledWith(expect.stringContaining('v_transferencia_stock_pendiente'), ['origen', 'material']);
  });

  it('la aprobación parcial deja disponible solo el remanente libre de esa condición', async () => {
    const { service, manager, stock } = fixture({ nuevo: 3, usado: 2, critico: 0, total: 5 });
    await expect(service.assertStockNotCommittedToTransfer(manager, stock, 7, 'NUEVO', 'Filtro')).resolves.toBeUndefined();
    service.applyIssuedStockByCondition(stock, 7, 'NUEVO', 'Filtro');
    expect(stock).toMatchObject({ stock_actual: 8, stock_nuevo: 3, stock_usado: 5 });
  });

  it('protege el crítico cuando es la única existencia', async () => {
    const { service, manager, stock } = fixture({ nuevo: 0, usado: 0, critico: 3, total: 3 });
    Object.assign(stock, { stock_actual: 5, stock_nuevo: 0, stock_usado: 0, stock_critico: 5 });
    await expect(service.assertStockNotCommittedToTransfer(manager, stock, 3, 'CRITICO', 'Filtro')).rejects.toThrow('Disponible 2.00');
    await expect(service.assertStockNotCommittedToTransfer(manager, stock, 2, 'CRITICO', 'Filtro')).resolves.toBeUndefined();
  });

  it('al anular o completar la transferencia desaparece el compromiso y retorna la disponibilidad', async () => {
    const { service } = fixture({ nuevo: 0, usado: 0, critico: 0, total: 0 });
    expect(await service.resolveReservationAvailability('material', 'origen', 12)).toMatchObject({ availableQty: 12, faltante: 0 });
  });
});
