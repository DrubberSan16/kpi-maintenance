import { BadRequestException, ForbiddenException } from '@nestjs/common';
import { KpiMaintenanceService } from './kpi-maintenance.service';
import {
  MovimientoInventarioEntity,
  TransferenciaBodegaEntity,
} from '../entities/kpi-maintenance.entity';

function fixture(overrides: Record<string, unknown> = {}) {
  const service: any = Object.create(KpiMaintenanceService.prototype);
  const movements: any[] = [];
  let nextId = 0;
  const repo = {
    create: jest.fn((value) => ({ id: `movement-${++nextId}`, ...value })),
    save: jest.fn(async (value) => {
      if (!movements.some((row) => row.id === value.id)) movements.push(value);
      return value;
    }),
  };
  const manager = {
    getRepository: jest.fn(() => repo),
    find: jest.fn(async (entity, options) => {
      if (entity === TransferenciaBodegaEntity) return [];
      return movements.filter((row) => row.work_order_id === options.where.work_order_id);
    }),
  };
  service.generateMaintenanceInventoryDocumentCode = jest.fn(async () => `EB-${nextId + 1}`);
  const workOrder: any = {
    id: 'project-1', code: 'OT-PROYECTO', maintenance_kind: 'PROYECTO',
    is_overhaul: true, is_emergency: false, status_workflow: 'IN_PROGRESS',
    ...overrides,
  };
  const issue = (date: string, warehouse = 'warehouse-1') => service.resolveWorkOrderIssueMovement(
    manager, workOrder,
    { items: [{ producto_id: 'material-1', bodega_id: warehouse, cantidad: 2 }], observacion: `Entrega ${date}` },
    'bodega', new Date(date),
  );
  return { service, movements, manager, repo, workOrder, issue };
}

describe('OT Proyecto: un EB por día y clase OVERHAUL', () => {
  it('crea Normal por defecto y permite OVERHAUL solamente en proyectos', () => {
    const { service } = fixture();
    expect(service.resolveWorkOrderOverhaulState('PROYECTO', undefined)).toBe(false);
    expect(service.resolveWorkOrderOverhaulState('PROYECTO', true)).toBe(true);
    expect(() => service.resolveWorkOrderOverhaulState('CORRECTIVO', true)).toThrow(BadRequestException);
  });

  it('conserva la clase guardada frente a cambios enviados durante la edición', () => {
    const { service, workOrder } = fixture();
    expect(service.resolveWorkOrderOverhaulState('PROYECTO', false, workOrder)).toBe(true);
    expect(service.resolveWorkOrderOverhaulState('PROYECTO', true, { ...workOrder, is_overhaul: false })).toBe(false);
    expect(service.resolveWorkOrderOverhaulState('PROYECTO', true, { id: 'legacy' })).toBe(false);
  });

  it.each(['PLANNED', 'IN_PROGRESS', 'REVIEW'])('permite salidas mientras está en %s', (state) => {
    for (const isOverhaul of [true, false]) {
      const { service, workOrder } = fixture({ status_workflow: state, is_overhaul: isOverhaul });
      expect(() => service.assertWorkOrderAllowsMaterialIssue(workOrder)).not.toThrow();
    }
  });

  it.each(['CLOSED', 'BLOCKED', 'ANNULLED'])('bloquea salidas en %s', (state) => {
    const { service, workOrder } = fixture({ status_workflow: state });
    expect(() => service.assertWorkOrderAllowsMaterialIssue(workOrder)).toThrow(ForbiddenException);
  });

  it('bloquea también una OT anulada en su auditoría aunque indique En proceso', () => {
    const { service, workOrder } = fixture({ valor_json: { approval_action: 'ANULADA' } });
    expect(() => service.assertWorkOrderAllowsMaterialIssue(workOrder)).toThrow(ForbiddenException);
  });

  it.each([
    { maintenance_kind: 'CORRECTIVO', is_overhaul: false, is_emergency: true },
    { maintenance_kind: 'PREVENTIVO', is_overhaul: false },
  ])('conserva el bloqueo de salidas en proceso para una OT que no es de proyecto: %j', (overrides) => {
    const { service, workOrder } = fixture(overrides);
    expect(() => service.assertWorkOrderAllowsMaterialIssue(workOrder)).toThrow(ForbiddenException);
  });

  it('agrupa varias entregas y bodegas del mismo día sin alterar el número ni la apertura', async () => {
    const { issue, movements, repo } = fixture();
    const first = await issue('2026-10-13T15:00:00Z');
    first.total_costos = 10;
    const second = await issue('2026-10-13T23:00:00Z', 'warehouse-2');
    expect(second.id).toBe(first.id);
    expect(second.numero_documento).toBe(first.numero_documento);
    expect(second.fecha_movimiento.toISOString()).toBe('2026-10-13T15:00:00.000Z');
    expect(second.total_costos).toBe(10);
    expect(second.bodega_origen_id).toBeNull();
    expect(second.observacion).toContain('Entrega 2026-10-13T15:00:00Z');
    expect(second.observacion).toContain('Entrega 2026-10-13T23:00:00Z');
    expect(movements).toHaveLength(1);
    expect(repo.create).toHaveBeenCalledTimes(1);
  });

  it('cambia el EB a medianoche de Guayaquil y reutiliza el del nuevo día', async () => {
    const { issue, movements } = fixture();
    const first = await issue('2026-10-14T04:59:59Z'); // 13 de octubre, 23:59:59
    const second = await issue('2026-10-14T05:00:00Z'); // 14 de octubre, 00:00:00
    const third = await issue('2026-10-14T22:00:00Z');
    expect(second.id).not.toBe(first.id);
    expect(second.numero_documento).not.toBe(first.numero_documento);
    expect(second.total_costos).toBe(0);
    expect(third.id).toBe(second.id);
    expect(movements).toHaveLength(2);
  });

  it('no divide el EB al cambiar la fecha UTC dentro del mismo día local', async () => {
    const { issue, movements } = fixture();
    const first = await issue('2026-10-13T23:59:59Z');
    const second = await issue('2026-10-14T04:59:59Z');
    expect(second.id).toBe(first.id);
    expect(movements).toHaveLength(1);
  });

  it('una OT Proyecto Normal también abre un EB por día', async () => {
    const { issue, movements } = fixture({ is_overhaul: false });
    const first = await issue('2026-10-13T15:00:00Z');
    const sameDay = await issue('2026-10-13T20:00:00Z');
    const nextDay = await issue('2026-10-14T15:00:00Z');
    expect(sameDay.id).toBe(first.id);
    expect(nextDay.id).not.toBe(first.id);
    expect(movements).toHaveLength(2);
  });

  it('mantiene un único EB entre días para una OT que no es de proyecto', async () => {
    const { issue, movements } = fixture({ maintenance_kind: 'CORRECTIVO', is_overhaul: false, is_emergency: true });
    const first = await issue('2026-10-13T15:00:00Z');
    const second = await issue('2026-10-14T15:00:00Z');
    expect(second.id).toBe(first.id);
    expect(movements).toHaveLength(1);
  });

  describe('sitios de una sola sucursal', () => {
    function sitesService(locations: any[], bodegas: any[]) {
      const service: any = Object.create(KpiMaintenanceService.prototype);
      service.locationRepo = { find: jest.fn(async () => locations) };
      service.bodegaRepo = { find: jest.fn(async () => bodegas) };
      return service;
    }

    it('acepta bodegas y ubicación del proyecto de la misma sucursal', async () => {
      const service = sitesService([{ id: 'loc-1', sucursal_id: 'suc-1' }], [
        { id: 'bod-1', sucursal_id: 'suc-1' }, { id: 'bod-2', sucursal_id: 'suc-1' },
      ]);
      await expect(service.assertProyectoSitesExist(['loc-1'], ['bod-1', 'bod-2'])).resolves.toBeUndefined();
    });

    it('rechaza sitios de dos sucursales', async () => {
      const service = sitesService([{ id: 'loc-1', sucursal_id: 'suc-1' }], [{ id: 'bod-9', sucursal_id: 'suc-2' }]);
      await expect(service.assertProyectoSitesExist(['loc-1'], ['bod-9'])).rejects.toThrow('una sola sucursal');
    });
  });

  it('mantiene independientes los egresos que pertenecen a transferencias', async () => {
    const { issue, manager, movements } = fixture();
    movements.push({ id: 'scrap', work_order_id: 'project-1', fecha_movimiento: new Date('2026-10-13T13:00:00Z') });
    manager.find.mockImplementation(async (entity: any) => entity === TransferenciaBodegaEntity
      ? [{ movimiento_salida_id: 'scrap', movimiento_ingreso_id: null }] as any
      : movements);
    const created = await issue('2026-10-13T15:00:00Z');
    expect(created.id).not.toBe('scrap');
    expect(manager.find).toHaveBeenCalledWith(MovimientoInventarioEntity, expect.anything());
  });
});
