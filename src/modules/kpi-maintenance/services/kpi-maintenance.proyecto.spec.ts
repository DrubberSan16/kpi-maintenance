import { BadRequestException } from '@nestjs/common';
import { KpiMaintenanceService } from './kpi-maintenance.service';
import {
  WorkOrderProyectoBodegaEntity,
  WorkOrderProyectoPersonalEntity,
  WorkOrderProyectoUbicacionEntity,
} from '../entities/kpi-maintenance.entity';

/**
 * El constructor del servicio pide casi cincuenta repositorios. Para probar la
 * logica de OT de Proyecto solo hacen falta cuatro, asi que se arma la
 * instancia sobre el prototipo y se inyectan a mano los que el caso usa.
 */
const createRepo = () => ({
  find: jest.fn().mockResolvedValue([]),
  findOne: jest.fn(),
  save: jest.fn(async (value: unknown) => value),
  create: jest.fn((value: unknown) => value),
  update: jest.fn(),
});

type ServiceUnderTest = KpiMaintenanceService & Record<string, any>;

function createService(overrides: Record<string, any> = {}) {
  const service = Object.create(
    KpiMaintenanceService.prototype,
  ) as ServiceUnderTest;
  Object.assign(service, {
    // Las propiedades de clase se inicializan en el constructor, que aqui no
    // corre: se repiten los valores que la logica bajo prueba consulta.
    WORK_ORDER_MAINTENANCE_KIND_VALUES: [
      'CORRECTIVO',
      'PREVENTIVO',
      'PREDICTIVO',
      'CEBADO',
      'INSPECCION',
      'PROYECTO',
    ],
    PROCEDIMIENTO_TIPO_PROCESO_PROYECTO: 'PROYECTO',
    locationRepo: createRepo(),
    bodegaRepo: createRepo(),
    woProyectoUbicacionRepo: createRepo(),
    woProyectoBodegaRepo: createRepo(),
    woProyectoPersonalRepo: createRepo(),
    ...overrides,
  });
  return service;
}

describe('KpiMaintenanceService OT de Proyecto', () => {
  describe('tipo de mantenimiento', () => {
    it('acepta PROYECTO como tipo de OT', () => {
      const service = createService();
      expect(service.resolveWorkOrderMaintenanceKind('PROYECTO')).toBe(
        'PROYECTO',
      );
      expect(service.buildWorkOrderMaintenanceKindLabel('PROYECTO')).toBe(
        'Proyecto',
      );
      expect(service.isProyectoMaintenanceKind('proyecto')).toBe(true);
      expect(service.isProyectoMaintenanceKind('CORRECTIVO')).toBe(false);
    });

    it('sigue rechazando un tipo inventado', () => {
      const service = createService();
      expect(() => service.resolveWorkOrderMaintenanceKind('CHIRIMOYA')).toThrow(
        BadRequestException,
      );
    });
  });

  describe('plantilla de formato proyecto', () => {
    it('reconoce el tipo de proceso con acentos, espacios o guiones', () => {
      const service = createService();
      expect(service.isProyectoProcedimiento({ tipo_proceso: 'Proyecto' })).toBe(
        true,
      );
      expect(service.isProyectoProcedimiento({ tipo_proceso: ' proyecto ' })).toBe(
        true,
      );
      expect(
        service.isProyectoProcedimiento({ tipo_proceso: 'PROCEDIMIENTO_TRABAJO' }),
      ).toBe(false);
      expect(service.isProyectoProcedimiento(null)).toBe(false);
    });

    it('normaliza los roles a contratar y descarta los que no tienen cargo', () => {
      const service = createService();
      expect(
        service.normalizeProcedimientoPersonalRequerido([
          { rol: ' Soldador estructural ', cantidad: '2', valor_dia: '35.456' },
          { rol: '', cantidad: 5 },
          { nombre: 'Esmerilador', cantidad: 0 },
        ]),
      ).toEqual([
        { rol: 'Soldador estructural', cantidad: 2, valor_dia: 35.46 },
        { rol: 'Esmerilador', cantidad: 1, valor_dia: 0 },
      ]);
    });
  });

  describe('sitios donde se ejecuta el proyecto', () => {
    it('exige al menos una ubicacion o una bodega', () => {
      const service = createService();
      expect(() =>
        service.assertProyectoWorkOrderHasSites('PROYECTO', [], []),
      ).toThrow(BadRequestException);
      expect(() =>
        service.assertProyectoWorkOrderHasSites('PROYECTO', ['ubi-1'], []),
      ).not.toThrow();
      expect(() =>
        service.assertProyectoWorkOrderHasSites('PROYECTO', [], ['bod-1']),
      ).not.toThrow();
    });

    it('no aplica a una OT que no es de proyecto', () => {
      const service = createService();
      expect(() =>
        service.assertProyectoWorkOrderHasSites('CORRECTIVO', [], []),
      ).not.toThrow();
    });

    it('rechaza una ubicacion que no existe', async () => {
      const locationRepo = createRepo();
      locationRepo.find.mockResolvedValue([{ id: 'ubi-1' }]);
      const service = createService({ locationRepo });
      await expect(
        service.assertProyectoSitesExist(['ubi-1', 'ubi-2'], []),
      ).rejects.toThrow(BadRequestException);
    });

    it('rechaza una bodega que no existe', async () => {
      const bodegaRepo = createRepo();
      bodegaRepo.find.mockResolvedValue([]);
      const service = createService({ bodegaRepo });
      await expect(
        service.assertProyectoSitesExist([], ['bod-1']),
      ).rejects.toThrow(BadRequestException);
    });
  });

  describe('personal contratado', () => {
    it('normaliza importes, fechas y descarta filas sin cargo', () => {
      const service = createService();
      expect(
        service.normalizeProyectoPersonalRows([
          {
            rol: ' Soldador ',
            nombre: ' Juan Perez ',
            dias_laborados: '3.456',
            valor_dia: '40',
            fecha: '2026-09-15T00:00:00.000Z',
            observacion: ' ',
          },
          { nombre: 'Sin cargo', dias_laborados: 2 },
        ]),
      ).toEqual([
        {
          orden: 1,
          rol: 'Soldador',
          nombre: 'Juan Perez',
          dias_laborados: 3.46,
          location_id: null,
          ubicacion_texto: null,
          valor_dia: 40,
          fecha: '2026-09-15',
          observacion: null,
        },
      ]);
    });

    it('rechaza dias o valores negativos', () => {
      const service = createService();
      expect(() =>
        service.normalizeProyectoPersonalRows([
          { rol: 'Soldador', dias_laborados: -1 },
        ]),
      ).toThrow(BadRequestException);
    });

    it('rechaza una fecha que no es una fecha', () => {
      const service = createService();
      expect(() =>
        service.normalizeProyectoPersonalRows([
          { rol: 'Soldador', fecha: 'ayer' },
        ]),
      ).toThrow(BadRequestException);
    });
  });

  describe('campos obligatorios de cierre', () => {
    it('una OT de Proyecto pide objetivo general y metodologia', () => {
      const service = createService();
      expect(() =>
        service.assertRequiredWorkOrderOutcomePayload({}, 'PROYECTO'),
      ).toThrow(/Objetivo general/);
      expect(() =>
        service.assertRequiredWorkOrderOutcomePayload(
          { proyecto: { objetivo_general: 'Construir la plataforma' } },
          'PROYECTO',
        ),
      ).toThrow(/Metodología aplicable/);
      expect(() =>
        service.assertRequiredWorkOrderOutcomePayload(
          {
            proyecto: {
              objetivo_general: 'Construir la plataforma',
              metodologia: 'Medicion y soldadura',
            },
          },
          'PROYECTO',
        ),
      ).not.toThrow();
    });

    it('una OT de mantenimiento sigue pidiendo causa, accion y prevencion', () => {
      const service = createService();
      expect(() =>
        service.assertRequiredWorkOrderOutcomePayload(
          { proyecto: { objetivo_general: 'x', metodologia: 'y' } },
          'CORRECTIVO',
        ),
      ).toThrow(/Causa/);
      expect(() =>
        service.assertRequiredWorkOrderOutcomePayload(
          { causa: 'a', accion: 'b', prevencion: 'c' },
          'CORRECTIVO',
        ),
      ).not.toThrow();
    });
  });

  describe('persistencia del detalle', () => {
    const buildManager = () => {
      const repos = new Map<unknown, ReturnType<typeof createRepo>>([
        [WorkOrderProyectoUbicacionEntity, createRepo()],
        [WorkOrderProyectoBodegaEntity, createRepo()],
        [WorkOrderProyectoPersonalEntity, createRepo()],
      ]);
      return {
        repos,
        manager: { getRepository: (entity: unknown) => repos.get(entity) },
      };
    };

    it('no toca lo guardado cuando el cliente no manda el bloque', async () => {
      const service = createService();
      const { repos, manager } = buildManager();
      await service.replaceWorkOrderProyectoDetail(manager, 'wo-1', {
        ubicacionIds: null,
        bodegaIds: null,
        personal: null,
      });
      for (const repo of repos.values()) {
        expect(repo.update).not.toHaveBeenCalled();
        expect(repo.save).not.toHaveBeenCalled();
      }
    });

    it('reemplaza por completo el bloque que si llega', async () => {
      const service = createService();
      const { repos, manager } = buildManager();
      await service.replaceWorkOrderProyectoDetail(
        manager,
        'wo-1',
        {
          ubicacionIds: ['ubi-1', 'ubi-2'],
          bodegaIds: [],
          personal: [{ rol: 'Soldador', dias_laborados: 2, valor_dia: 40 }],
        },
        { username: 'tester' },
      );

      const ubicacionRepo = repos.get(WorkOrderProyectoUbicacionEntity)!;
      expect(ubicacionRepo.update).toHaveBeenCalledWith(
        { work_order_id: 'wo-1', is_deleted: false },
        expect.objectContaining({ is_deleted: true, updated_by: 'tester' }),
      );
      expect(ubicacionRepo.save).toHaveBeenCalledWith([
        expect.objectContaining({ location_id: 'ubi-1', orden: 1 }),
        expect.objectContaining({ location_id: 'ubi-2', orden: 2 }),
      ]);

      // Una lista vacia si borra: el usuario quito todas las bodegas.
      const bodegaRepo = repos.get(WorkOrderProyectoBodegaEntity)!;
      expect(bodegaRepo.update).toHaveBeenCalled();
      expect(bodegaRepo.save).not.toHaveBeenCalled();

      const personalRepo = repos.get(WorkOrderProyectoPersonalEntity)!;
      expect(personalRepo.save).toHaveBeenCalledWith([
        expect.objectContaining({
          work_order_id: 'wo-1',
          rol: 'Soldador',
          dias_laborados: 2,
          valor_dia: 40,
        }),
      ]);
    });
  });

  describe('lectura del detalle', () => {
    it('resuelve etiquetas y calcula el total por persona', async () => {
      const locationRepo = createRepo();
      locationRepo.find.mockResolvedValue([
        { id: 'ubi-1', codigo: 'UBI-A00001', nombre: 'Patio' },
      ]);
      const bodegaRepo = createRepo();
      bodegaRepo.find.mockResolvedValue([
        { id: 'bod-1', codigo: 'BOD-001', nombre: 'Bodega Coca' },
      ]);
      const woProyectoUbicacionRepo = createRepo();
      woProyectoUbicacionRepo.find.mockResolvedValue([
        { location_id: 'ubi-1', orden: 1 },
      ]);
      const woProyectoBodegaRepo = createRepo();
      woProyectoBodegaRepo.find.mockResolvedValue([
        { bodega_id: 'bod-1', orden: 1 },
      ]);
      const woProyectoPersonalRepo = createRepo();
      woProyectoPersonalRepo.find.mockResolvedValue([
        {
          id: 'per-1',
          orden: 1,
          rol: 'Soldador',
          nombre: 'Juan Perez',
          dias_laborados: '3',
          location_id: 'ubi-1',
          ubicacion_texto: null,
          valor_dia: '40.5',
          fecha: '2026-09-15',
          observacion: null,
        },
      ]);

      const service = createService({
        locationRepo,
        bodegaRepo,
        woProyectoUbicacionRepo,
        woProyectoBodegaRepo,
        woProyectoPersonalRepo,
      });

      const detalle = await service.loadWorkOrderProyectoDetail('wo-1');
      expect(detalle.proyecto_ubicacion_ids).toEqual(['ubi-1']);
      expect(detalle.proyecto_ubicaciones[0].label).toBe('UBI-A00001 - Patio');
      expect(detalle.proyecto_bodegas[0].label).toBe('BOD-001 - Bodega Coca');
      expect(detalle.proyecto_personal[0]).toMatchObject({
        rol: 'Soldador',
        ubicacion_label: 'UBI-A00001 - Patio',
        total: 121.5,
      });
    });
  });
});
