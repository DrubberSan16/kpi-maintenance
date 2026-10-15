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
    horometro_ultimo_mantenimiento: '800',
    ultima_ot: 'OT-1',
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
      fila({ estado_funcionamiento: 'PARADO', horometro_operativo_desde: null, horometro_actual: '1024.6', horometro_ultimo_mantenimiento: '800.4' }),
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
        frecuencia: '1500', margen_tolerancia_pct: '4.60', horometro_ultimo_mantenimiento: '800',
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
    expect(query).toHaveBeenCalledWith(expect.any(String), [null]);
    expect(data.proyeccion).toHaveLength(3);
    expect(data).toMatchObject({ mantenimientos_vencidos: 1, mantenimientos_proximos: 1 });
  });

  it('un equipo parado conserva la lectura guardada', async () => {
    const [row] = await proyeccion([
      fila({ estado_funcionamiento: 'PARADO', horometro_operativo_desde: null }),
    ]);
    expect(row).toMatchObject({ horometro_operativo_base: 1000, horometro_actual: 1000, horas_restantes: 50 });
  });
});
