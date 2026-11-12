import { addOilDeliveryCosts, splitOilCostPeriods } from './oil-deliveries';

describe('Costo financiero del aceite por Kardex', () => {
  const order = () => ({ total_costo: 100, costo_aceite: 0, total_cantidad: 2, total_items: 1,
    _materiales: new Set(['Filtro']), _detalle: new Map() });
  const delivery = (extra = {}) => ({ work_order_id: 'ot', producto_id: 'oil', bodega_id: 'b1',
    material_codigo: 'ACT', material_label: 'Aceite', bodega_label: 'Bodega 1',
    fecha_referencia: '2026-10-31', galones: 85, costo: 1062.5, ...extra });

  it('incluye la entrega de octubre aunque la OT cierre en noviembre, sin sumar material digitado', () => {
    const rows = new Map([['ot', order()]]);
    addOilDeliveryCosts(rows, [delivery()]);
    expect(rows.get('ot')).toMatchObject({ total_costo: 1162.5, costo_aceite: 1062.5, total_cantidad: 87 });
    expect([...rows.get('ot')!._detalle.values()]).toEqual([expect.objectContaining({
      cantidad: 85, costo_total: 1062.5, costo_unitario: 12.5, fecha_referencia: '2026-10-31', fuente: 'KARDEX',
    })]);
  });

  it('conserva fecha, bodega y devolución para conciliar cada período', () => {
    const rows = new Map([['ot', order()]]);
    addOilDeliveryCosts(rows, [delivery(), delivery({ galones: -5, costo: -62.5 }),
      delivery({ bodega_id: 'b2', fecha_referencia: '2026-11-01', galones: 2, costo: 26 }),
      delivery({ work_order_id: 'fuera-del-alcance' })]);
    expect(rows.get('ot')).toMatchObject({ total_costo: 1126, costo_aceite: 1026, total_cantidad: 84 });
    const details = [...rows.get('ot')!._detalle.values()];
    expect(details).toHaveLength(2);
    expect(details[0]).toMatchObject({ cantidad: 80, costo_total: 1000, fecha_referencia: '2026-10-31' });
    expect(details[1]).toMatchObject({ cantidad: 2, costo_total: 26, fecha_referencia: '2026-11-01' });
  });
});


describe('Distribución mensual del aceite', () => {
  it('mantiene el cierre en noviembre y el aceite de octubre en octubre, incluidas devoluciones', () => {
    const periods = splitOilCostPeriods({ aceite_fuente: 'KARDEX', period_key: '2026-11',
      total_costo: 1160, costo_aceite: 1050, total_cantidad: 86, total_items: 4,
      materiales_detalle: [
        { categoria_costo: 'ACEITE', fecha_referencia: '2026-10-31', costo_total: 1062.5, cantidad: 85 },
        { categoria_costo: 'ACEITE', fecha_referencia: '2026-11-01', costo_total: -12.5, cantidad: -1 },
      ] });
    expect(periods.find(row => row.period_key === '2026-10')).toMatchObject({ total_costo: 1062.5, total_cantidad: 85 });
    expect(periods.find(row => row.period_key === '2026-11')).toMatchObject({ total_costo: 97.5, total_cantidad: 1 });
  });
});
