import { oilMaintenanceClassification } from './oil-maintenance-classification';
describe('Clasificación del control de aceite', () => {
  it('reconoce MPG 500 antiguo aunque se guardó como correctivo y la programación dice 6 horas', () => {
    expect(oilMaintenanceClassification({ maintenance_kind: 'CORRECTIVO', plan_name: 'MPG 500 HRS - CAT - CUMMINS', procedure_hours: 6, plan_frequency: 6, plan_frequency_type: 'HORAS' }))
      .toEqual({ maintenance_kind_original: 'CORRECTIVO', maintenance_kind: 'PREVENTIVO', mpg_interval_hours: 500 });
  });
  it('conserva cebados y proyectos aun si reutilizaron un plan MPG', () => {
    for (const kind of ['CEBADO', 'PROYECTO']) expect(oilMaintenanceClassification({ maintenance_kind: kind, plan_name: 'MPG 500 HORAS MTU' }))
      .toMatchObject({ maintenance_kind: kind, mpg_interval_hours: null });
  });
  it('usa la frecuencia explícita del procedimiento y no inventa frecuencia para correctivos', () => {
    expect(oilMaintenanceClassification({ maintenance_kind: 'PREVENTIVO', procedure_hours: 300, plan_name: 'MPG' }).mpg_interval_hours).toBe(300);
    expect(oilMaintenanceClassification({ maintenance_kind: 'CORRECTIVO', work_order_title: 'Reparar generador de 500 kVA' })).toMatchObject({ maintenance_kind: 'CORRECTIVO', mpg_interval_hours: null });
  });
});
