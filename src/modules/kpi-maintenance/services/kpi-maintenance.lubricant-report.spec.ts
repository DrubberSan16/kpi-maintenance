import { KpiMaintenanceService } from './kpi-maintenance.service';

/*
 * Respuestas del reporte de análisis de lubricante: lo que la pantalla
 * necesita para validar cada resultado contra los límites del laboratorio,
 * sin repetir los mismos datos dos o tres veces.
 */
const ANALISIS_ID = '11111111-1111-4111-8111-111111111111';
const ANTERIOR_ID = '22222222-2222-4222-8222-222222222222';
const EQUIPO_ID = '33333333-3333-4333-8333-333333333333';

const rangos = {
  FE: {
    warning: { raw: '≥ 100', operator: '>=', value: 100 },
    alert: { raw: '> 190', operator: '>', value: 190 },
    authoritative: true,
  },
  CU: {
    warning: { raw: '≥ 31', operator: '>=', value: 31 },
    alert: { raw: '> 50', operator: '>', value: 50 },
    authoritative: true,
  },
  TBN: {
    baseline: 10,
    warning: {
      raw: '[50% TBN -1]',
      operator: '<=',
      value: 0.5,
      expression: 'BASELINE_PERCENT_MINUS_OFFSET',
      offset: 1,
    },
    authoritative: true,
  },
};

const analisis = (id: string, codigo: string, fecha: string) => ({
  id,
  codigo,
  cliente: 'JUSTICE COMPANY',
  equipo_id: EQUIPO_ID,
  equipo_codigo: 'EQ-1',
  equipo_nombre: 'JC - UG07',
  producto_id: null,
  lubricante: 'ACEITE GULF',
  marca_lubricante: 'GULF',
  compartimento_principal: 'MOTOR',
  fecha_muestra: fecha,
  fecha_reporte: fecha,
  estado_diagnostico: 'NORMAL',
  diagnostico: null,
  payload_json: {
    sample_info: { numero_muestra: `M-${codigo}`, equipo_modelo: '3512' },
    rangos_generales: rangos,
  },
  is_deleted: false,
  created_at: new Date(`${fecha}T12:00:00Z`),
});

const detalles: Record<string, Array<Record<string, unknown>>> = {
  [ANALISIS_ID]: [
    { id: 'd1', analisis_id: ANALISIS_ID, parametro: 'Fe (Hierro)', unidad: 'ppm', resultado_numerico: 30, orden: 301 },
    { id: 'd2', analisis_id: ANALISIS_ID, parametro: 'Cu (Cobre)', unidad: 'ppm', resultado_numerico: 64, orden: 304 },
    { id: 'd3', analisis_id: ANALISIS_ID, parametro: 'T.B.N. mgKOH/gr', unidad: 'mgKOH/gr', resultado_numerico: 9.7, orden: 4 },
  ],
  [ANTERIOR_ID]: [
    { id: 'd4', analisis_id: ANTERIOR_ID, parametro: 'Fe (Hierro)', unidad: 'ppm', resultado_numerico: 120, orden: 301 },
    { id: 'd5', analisis_id: ANTERIOR_ID, parametro: 'Cu (Cobre)', unidad: 'ppm', resultado_numerico: 4, orden: 304 },
    { id: 'd6', analisis_id: ANTERIOR_ID, parametro: 'T.B.N. mgKOH/gr', unidad: 'mgKOH/gr', resultado_numerico: 3.5, orden: 4 },
  ],
};

const createService = () => {
  const service = Object.create(KpiMaintenanceService.prototype);
  const rows = [
    analisis(ANALISIS_ID, 'AL-2', '2026-09-30'),
    analisis(ANTERIOR_ID, 'AL-1', '2026-08-26'),
  ];
  service.analisisLubricanteRepo = { find: jest.fn().mockResolvedValue(rows) };
  service.analisisLubricanteDetRepo = {
    find: jest.fn(({ where }: { where: { analisis_id: string } }) =>
      Promise.resolve(
        (detalles[where.analisis_id] ?? []).map((item) => ({
          ...item,
          is_deleted: false,
        })),
      ),
    ),
  };
  service.equipoRepo = {
    find: jest.fn().mockResolvedValue([
      { id: EQUIPO_ID, codigo: 'EQ-1', nombre: 'JC - UG07', modelo: '3512' },
    ]),
  };
  service.productoRepo = { find: jest.fn().mockResolvedValue([]) };
  service.marcaRepo = { find: jest.fn().mockResolvedValue([]) };
  return service;
};

describe('Reporte de análisis de lubricante', () => {
  it('la lista completa no repite los detalles ni las reglas', async () => {
    const service = createService();
    const result = await service.listAnalisisLubricante(null);
    const latest = result.data.find((item: any) => item.id === ANALISIS_ID);

    expect(latest.detalle_grupos).toBeUndefined();
    expect(latest.detalles).toHaveLength(3);
    const cobre = latest.detalles.find((item: any) => item.parametro_key === 'CU');
    expect(cobre.rangos_generales).toBeUndefined();
    expect(cobre.grupo_label).toBeTruthy();
    // La regla viaja sin campos vacíos.
    expect(cobre.rango_anormal).toEqual({ raw: '> 50', operator: '>', value: 50 });
    expect(cobre.nivel_alerta).toBe('ANORMAL');
  });

  it('el resumen dice qué parámetros dejaron cada muestra fuera de rango', async () => {
    const service = createService();
    const result = await service.listAnalisisLubricante(null, { resumen: true });
    const latest = result.data.find((item: any) => item.id === ANALISIS_ID);
    const previous = result.data.find((item: any) => item.id === ANTERIOR_ID);

    expect(latest.detalles).toBeUndefined();
    expect(latest.hallazgos).toEqual([
      expect.objectContaining({
        parametro_key: 'CU',
        valor: 64,
        unidad: 'ppm',
        nivel: 'ANORMAL',
        limite: '> 50',
      }),
    ]);
    // Primero lo anormal; el TBN cae bajo 50 % de su base menos 1 (= 4).
    expect(previous.hallazgos.map((item: any) => [item.parametro_key, item.nivel])).toEqual([
      ['TBN', 'PRECAUCION'],
      ['FE', 'PRECAUCION'],
    ]);
  });

  it('el dashboard traza en cada punto los límites que regían en esa muestra', async () => {
    const service = createService();
    const result = await service.getAnalisisLubricanteDashboard({
      equipo_id: EQUIPO_ID,
      lubricante: 'ACEITE GULF',
    });
    const data = result.data;

    expect(data.series).toBeUndefined();
    expect(data.latest_analysis.detalles).toBeUndefined();
    expect(data.latest_analysis.detalle_grupos).toBeUndefined();
    // Los grupos en el orden del informe, no en el de llegada de los detalles.
    expect(data.detail_groups.map((group: any) => group.key)).toEqual([
      'ESTADO_LUBRICANTE',
      'DESGASTE',
    ]);

    const metrics = data.chart_sections.flatMap((section: any) => section.metrics);
    const hierro = metrics.find((item: any) => item.key === 'FE');
    expect(hierro.points.map((point: any) => point.valor)).toEqual([120, 30]);
    expect(hierro.points[0]).toEqual({
      analisis_id: ANTERIOR_ID,
      codigo: 'AL-1',
      fecha: '2026-08-26',
      numero_muestra: 'M-AL-1',
      valor: 120,
      rango_precaucion: { raw: '≥ 100', operator: '>=', value: 100 },
      rango_anormal: { raw: '> 190', operator: '>', value: 190 },
      nivel_alerta: 'PRECAUCION',
    });
    expect(hierro.fuera_de_rango).toBe(1);
    expect(hierro.ultimo_nivel).toBe('NORMAL');

    const tbn = metrics.find((item: any) => item.key === 'TBN');
    expect(tbn.points[1].rango_precaucion.resolved_value).toBe(4);
  });
});
