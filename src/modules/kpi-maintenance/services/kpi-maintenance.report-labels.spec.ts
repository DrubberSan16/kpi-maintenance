import { KpiMaintenanceService } from './kpi-maintenance.service';

const CREATOR = '11111111-1111-4111-8111-111111111111';
const PROCESSOR = '22222222-2222-4222-8222-222222222222';
const APPROVER = '33333333-3333-4333-8333-333333333333';
const PRODUCT = '44444444-4444-4444-8444-444444444444';
const WAREHOUSE = '55555555-5555-4555-8555-555555555555';
const users = [
  { id: CREATOR, nameUser: 'ana', nameSurname: 'Ana Perez' },
  { id: PROCESSOR, nameUser: 'luis', nameSurname: 'Luis Torres', isDeleted: true },
  { id: APPROVER, nameUser: 'maria', nameSurname: 'Maria Lopez', status: 'INACTIVE' },
];
const createService = () => Object.create(KpiMaintenanceService.prototype) as any;

describe('Nombres en reportes de OT y OT Proyecto', () => {
  it.each(['CORRECTIVO', 'PROYECTO'])('resuelve auditoria historica %s por ID o usuario', (kind) => {
    const service = createService();
    expect(service.buildWorkOrderAuditLabels({
      maintenance_kind: kind, created_by: CREATOR, approved_by: APPROVER,
      updated_by: 'LUIS', valor_json: { processed_by_user_id: PROCESSOR },
    }, users)).toEqual({
      created_by_label: 'Ana Perez', processed_by_label: 'Luis Torres',
      approved_by_label: 'Maria Lopez', updated_by_label: 'Luis Torres',
    });
  });

  it('conserva nombres guardados y no expone IDs de usuarios ausentes', () => {
    const service = createService();
    expect(service.buildWorkOrderAuditLabels({
      created_by: CREATOR, approved_by: APPROVER, updated_by: PROCESSOR,
      valor_json: { created_by_name: 'Nombre historico', processed_by_name: PROCESSOR },
    }, [])).toEqual({
      created_by_label: 'Nombre historico', processed_by_label: null,
      approved_by_label: null, updated_by_label: null,
    });
  });

  it('solo incluye bajas del catalogo cuando la lectura historica lo pide', async () => {
    const service = createService();
    service.productoRepo = { find: jest.fn().mockResolvedValue([{ id: PRODUCT, nombre: 'Filtro', is_deleted: true }]) };
    service.bodegaRepo = { find: jest.fn().mockResolvedValue([{ id: WAREHOUSE, nombre: 'Taller', is_deleted: true }]) };
    const maps = await service.buildInventoryCatalogMaps([PRODUCT], [WAREHOUSE], true);
    expect(service.productoRepo.find.mock.calls[0][0].where).not.toHaveProperty('is_deleted');
    expect(service.bodegaRepo.find.mock.calls[0][0].where).not.toHaveProperty('is_deleted');
    expect(maps.productMap.get(PRODUCT).nombre).toBe('Filtro');
    await service.buildInventoryCatalogMaps([PRODUCT], [WAREHOUSE]);
    expect(service.productoRepo.find.mock.calls[1][0].where.is_deleted).toBe(false);
    expect(service.bodegaRepo.find.mock.calls[1][0].where.is_deleted).toBe(false);
  });

  it('consumos, salidas y chatarra conservan los nombres y descripciones', () => {
    const service = createService();
    const productMap = new Map([[PRODUCT, { id: PRODUCT, codigo: 'FIL-01', nombre: 'Filtro de aceite', descripcion: 'Motor principal' }]]);
    const warehouseMap = new Map([[WAREHOUSE, { id: WAREHOUSE, codigo: 'B-01', nombre: 'Taller' }]]);
    const row = { producto_id: PRODUCT, bodega_id: WAREHOUSE, cantidad: 2 };
    for (const result of [
      service.mapConsumoWithCatalogs(row, productMap, warehouseMap),
      service.mapIssueItemWithCatalogs(row, productMap, warehouseMap),
      service.mapScrapItemWithCatalogs(row, productMap),
    ]) {
      expect(result.producto_label).toContain('Filtro de aceite');
      expect(result.producto_descripcion).toBe('Motor principal');
      expect(result.producto_id).toBe(PRODUCT);
    }
    expect(service.mapIssueItemWithCatalogs(row, productMap, warehouseMap).bodega_label).toContain('Taller');
  });

  it('no usa IDs como etiquetas cuando falta un material o una bodega', () => {
    const service = createService();
    const row = { producto_id: PRODUCT, bodega_id: WAREHOUSE };
    expect(service.mapConsumoWithCatalogs(row, new Map(), new Map())).toMatchObject({
      producto_id: PRODUCT, producto_label: 'Material sin registro',
      bodega_id: WAREHOUSE, bodega_label: 'Bodega sin registro',
    });
    expect(service.mapIssueItemWithCatalogs(row, new Map(), new Map()).producto_label).toBe('Material sin registro');
    expect(service.mapScrapItemWithCatalogs(row, new Map()).producto_label).toBe('Material sin registro');
  });
});
