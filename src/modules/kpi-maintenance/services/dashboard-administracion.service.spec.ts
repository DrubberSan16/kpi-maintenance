import { DashboardAdministracionService } from './dashboard-administracion.service';

/*
 * La proyección por horómetro del Dashboard administrativo leía la columna
 * `horometro_actual`, que solo guarda la lectura del último cambio: con el
 * equipo en marcha mostraba un valor quieto mientras el Control operativo
 * seguía contando.
 */
describe('Dashboard administrativo: proyección por horómetro', () => {
  const ahora = new Date('2026-10-15T15:00:00.000Z');

  beforeEach(() => {
    jest.useFakeTimers();
    jest.setSystemTime(ahora);
  });
  afterEach(() => {
    jest.useRealTimers();
  });

  // Servicio MPG tal como lo arma `json_agg`: horómetro como texto, fecha ISO.
  const servicio = (overrides: Record<string, unknown> = {}) => ({
    work_order_code: 'OT-1',
    cerrada_en: '2026-10-01T10:00:00',
    plan_nombre: 'MPG 250 HRS - CAT-CUMMINS',
    horometro: '800',
    ...overrides,
  });

  // Así llega cada fila de `dataSource.query`: numeric como texto y timestamp como Date.
  const fila = (overrides: Record<string, unknown> = {}) => ({
    equipo_id: 'eq-1',
    equipo_codigo: 'UG-01',
    equipo_nombre: 'UG 01',
    horometro_actual: '1000.000000',
    estado_funcionamiento: 'FUNCIONAMIENTO',
    horometro_operativo_desde: new Date('2026-10-15T12:45:00.000Z'),
    frecuencia: '250',
    margen_anticipacion_pct: '10',
    margen_tolerancia_pct: '0',
    frecuencia_unidad: 'HORAS',
    ritmo_horas_dia: '24.00',
    servicios_mpg: [servicio()],
    ...overrides,
  });

  async function proyeccion(filas: Record<string, unknown>[]) {
    const query = jest.fn().mockResolvedValue(filas);
    const service = new DashboardAdministracionService({ query } as any);
    return (service as any).getProyeccion(null);
  }

  it('suma al horómetro el tiempo que el equipo lleva funcionando, como el Control operativo', async () => {
    const [row] = await proyeccion([fila()]);
    expect(row).toMatchObject({
      horometro_operativo_base: 1000,
      horometro_actual: 1002,
      estado_funcionamiento: 'FUNCIONAMIENTO',
      horometro_operativo_desde: new Date('2026-10-15T12:45:00.000Z'),
      horometro_proximo_mantenimiento: 1050,
      horas_restantes: 48,
      semaforo: { nivel: 'VERDE' },
    });
  });

  it('las horas que corrió el equipo cuentan para las restantes y el semáforo', async () => {
    // Guardado en 1020 quedarían 30 h (verde); con 10 h en marcha quedan 20 h,
    // dentro de la anticipación de 25 h.
    const [row] = await proyeccion([
      fila({ horometro_actual: '1020', horometro_operativo_desde: new Date('2026-10-15T05:00:00.000Z') }),
    ]);
    expect(row).toMatchObject({ horometro_actual: 1030, horas_restantes: 20, semaforo: { nivel: 'AMARILLO' } });
  });

  it('entrega el objetivo sin redondear para que la pantalla calcule igual las restantes', async () => {
    // Referencia y lectura con fracciones, como las deja el horómetro automático:
    // 1050.4 - 1024.6 = 25.8 -> 26 h (verde). Con el objetivo redondeado (1050)
    // saldrían 25 h y amarillo.
    const [row] = await proyeccion([
      fila({ estado_funcionamiento: 'PARADO', horometro_operativo_desde: null, horometro_actual: '1024.6', servicios_mpg: [servicio({ horometro: '800.4' })] }),
    ]);
    expect(row).toMatchObject({
      horometro_objetivo: 1050.4,
      horometro_proximo_mantenimiento: 1050,
      horas_restantes: 26,
      semaforo: { nivel: 'VERDE' },
    });
  });

  it('clasifica con los mismos umbrales que entrega, sin restos de coma flotante', async () => {
    // 1500 h al 4,6 % son 69 h de tolerancia (en coma flotante, 68,999...):
    // 69 h excedidas no la superan, como calcula también la pantalla.
    const [row] = await proyeccion([
      fila({
        estado_funcionamiento: 'PARADO', horometro_operativo_desde: null, horometro_actual: '2369',
        frecuencia: '1500', margen_tolerancia_pct: '4.60',
      }),
    ]);
    expect(row).toMatchObject({ umbral_rojo: 69, horas_restantes: -69, semaforo: { nivel: 'AMARILLO' } });
  });

  it('la proyección liviana trae las filas y los contadores del resumen', async () => {
    const query = jest.fn().mockResolvedValue([
      fila(),
      fila({ equipo_id: 'eq-2', horometro_actual: '1020', horometro_operativo_desde: new Date('2026-10-15T05:00:00.000Z') }),
      fila({ equipo_id: 'eq-3', estado_funcionamiento: 'PARADO', horometro_operativo_desde: null, horometro_actual: '1060' }),
    ]);
    const service = new DashboardAdministracionService({ query } as any);
    const { data } = (await service.getProyeccionVigente({ equipo_id: ' ' })) as { data: any };
    expect(query).toHaveBeenCalledWith(expect.any(String), [null, expect.any(Date), ahora]);
    expect(data.proyeccion).toHaveLength(3);
    expect(data).toMatchObject({ mantenimientos_vencidos: 1, mantenimientos_proximos: 1 });
  });

  it('un equipo parado conserva la lectura guardada', async () => {
    const [row] = await proyeccion([
      fila({ estado_funcionamiento: 'PARADO', horometro_operativo_desde: null }),
    ]);
    expect(row).toMatchObject({ horometro_operativo_base: 1000, horometro_actual: 1000, horas_restantes: 50 });
  });

  it('el objetivo es el último servicio más la frecuencia, nunca el horómetro actual', async () => {
    const [row] = await proyeccion([fila()]);
    expect(row).toMatchObject({
      referencia_estimada: false,
      horometro_ultimo_mantenimiento: 800,
      fecha_ultimo_mantenimiento: '2026-10-01',
      ultima_ot: 'OT-1',
      ultimo_plan: 'MPG 250 HRS - CAT-CUMMINS',
      horometro_objetivo: 1050,
    });
    expect(row.horometro_objetivo).not.toBe(row.horometro_actual);
    expect(row).not.toHaveProperty('servicios_mpg');
  });

  it('si el último servicio no registró horómetro, proyecta desde el anterior que sí lo hizo', async () => {
    // Antes la base era "actual menos frecuencia": objetivo = actual, 0 h restantes.
    const [row] = await proyeccion([
      fila({
        estado_funcionamiento: 'PARADO', horometro_operativo_desde: null,
        servicios_mpg: [servicio({ work_order_code: 'OT-9', horometro: null }), servicio({ work_order_code: 'OT-5', horometro: '780' })],
      }),
    ]);
    expect(row).toMatchObject({
      referencia_estimada: true,
      ultima_ot: 'OT-5',
      horometro_ultimo_mantenimiento: 780,
      horometro_proximo_mantenimiento: 1030,
      horas_restantes: 30,
    });
    expect(row.motivo_referencia_estimada).toContain('OT-9');
  });

  it('una lectura mayor al horómetro actual tampoco sirve de base', async () => {
    const [row] = await proyeccion([
      fila({
        estado_funcionamiento: 'PARADO', horometro_operativo_desde: null,
        servicios_mpg: [servicio({ work_order_code: 'OT-9', horometro: '4530' }), servicio({ horometro: '700' })],
      }),
    ]);
    expect(row).toMatchObject({ referencia_estimada: true, horometro_ultimo_mantenimiento: 700, horas_restantes: -50 });
  });

  it('sin ningún servicio con lectura asume el ciclo en múltiplos de la frecuencia', async () => {
    const [row] = await proyeccion([
      fila({ estado_funcionamiento: 'PARADO', horometro_operativo_desde: null, horometro_actual: '52169', frecuencia: '500', servicios_mpg: [] }),
    ]);
    expect(row).toMatchObject({
      referencia_estimada: true,
      ultima_ot: null,
      horometro_ultimo_mantenimiento: 52000,
      horometro_proximo_mantenimiento: 52500,
      horas_restantes: 331,
    });
  });

  it('las restantes quedan negativas cuando el equipo pasó el objetivo', async () => {
    const [row] = await proyeccion([
      fila({ estado_funcionamiento: 'PARADO', horometro_operativo_desde: null, horometro_actual: '1100' }),
    ]);
    expect(row).toMatchObject({ horas_restantes: -50, horas_excedidas: 50, semaforo: { nivel: 'ROJO' } });
  });

  it('la fecha del próximo mantenimiento sale de las restantes al ritmo de operación', async () => {
    // 15/10 10:00 en Guayaquil: 50 h a 12 h/día son 4 días y 4 h.
    const [enFecha] = await proyeccion([
      fila({ estado_funcionamiento: 'PARADO', horometro_operativo_desde: null, ritmo_horas_dia: '12.00' }),
    ]);
    expect(enFecha).toMatchObject({ horas_restantes: 50, ritmo_horas_dia: 12, fecha_proximo_mantenimiento: '2026-10-19' });

    // Vencido por 50 h a 24 h/día: debió hacerse hace poco más de dos días.
    const [vencido] = await proyeccion([
      fila({ estado_funcionamiento: 'PARADO', horometro_operativo_desde: null, horometro_actual: '1100' }),
    ]);
    expect(vencido.fecha_proximo_mantenimiento).toBe('2026-10-13');

    const [sinRitmo] = await proyeccion([fila({ ritmo_horas_dia: null })]);
    expect(sinRitmo.fecha_proximo_mantenimiento).toBeNull();
  });

  it('una frecuencia por calendario no se proyecta por horómetro', async () => {
    const [row] = await proyeccion([fila({ frecuencia: '20', frecuencia_unidad: 'DIAS' })]);
    expect(row).toMatchObject({ aplica: false, motivo_sin_proyeccion: 'Frecuencia por calendario (20 días)', semaforo: null });
  });

  it('solo cuentan los servicios MPG cerrados y no anulados, sin cebados ni proyectos', async () => {
    const query = jest.fn().mockResolvedValue([]);
    await (new DashboardAdministracionService({ query } as any) as any).getProyeccion(null);
    const sql = String(query.mock.calls[0][0]);
    expect(sql).toContain("UPPER(COALESCE(wo.maintenance_kind, '')) NOT IN ('CEBADO', 'PROYECTO')");
    expect(sql).toContain("p.nombre ~* '^[[:space:]]*MPG[[:space:]-]*[0-9]+[[:space:]]*H'");
    expect(sql).toMatch(/UPPER\(COALESCE\(wo\.status, ''\)\) NOT IN \('ANULADA'/);
    expect(sql).toContain("jsonb_typeof(wo.valor_json::jsonb -> 'annulment')");
  });
});
