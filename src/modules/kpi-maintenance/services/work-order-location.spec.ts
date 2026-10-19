import { withWorkOrderLocation } from './work-order-location';
import { KpiMaintenanceService } from './kpi-maintenance.service';
describe('Ubicación de ejecución en las OT', () => {
  it('usa los nombres e identificadores conservados y no la ubicación actual', () => {
    const equipment = {
      id: 'e',
      location_id: 'new',
      equipment_location_label: 'NUEVA UBICACIÓN',
    };
    const snapshot = {
      location_id: 'old',
      location_code: 'UBI-A',
      location_name: 'Taller',
      sucursal_id: 's1',
      sucursal_name: 'Central anterior',
    };
    const result = withWorkOrderLocation(equipment, {
      equipment_location_snapshot: snapshot,
    });
    expect(result).toMatchObject({
      location_id: 'old',
      equipment_location_label: 'UBI-A - Taller - Central anterior',
    });
    expect(equipment.location_id).toBe('new');
  });
  it('conserva una ubicación vacía histórica sin sustituirla por la actual', () => {
    expect(
      withWorkOrderLocation(
        { equipment_location_label: 'Actual' },
        { equipment_location_snapshot: { location_id: null } },
      ),
    ).toMatchObject({ equipment_location_label: 'Sin ubicación registrada' });
    const equipment = { id: 'e' };
    expect(withWorkOrderLocation(equipment, {})).toBe(equipment);
  });
  it('ubica las OT ejecutadas en su sucursal histórica aunque el equipo se traslade', async () => {
    const service: any = Object.create(KpiMaintenanceService.prototype);
    const old = {
      id: 'old',
      equipment_id: 'asset',
      equipment_location_snapshot: { sucursal_id: 'A' },
    };
    const planned = { id: 'planned', equipment_id: 'asset' };
    const scopeA = {
      sucursalId: 'A',
      equipmentIds: new Set(),
      warehouseIds: new Set(),
    };
    const scopeB = {
      sucursalId: 'B',
      equipmentIds: new Set(['asset']),
      warehouseIds: new Set(),
    };
    expect(
      await service.filterWorkOrdersByScope([old, planned], scopeA),
    ).toEqual([old]);
    expect(
      await service.filterWorkOrdersByScope([old, planned], scopeB),
    ).toEqual([planned]);
  });
});
