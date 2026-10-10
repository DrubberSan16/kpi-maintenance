import { BadRequestException } from '@nestjs/common';
import { KpiMaintenanceService } from './kpi-maintenance.service';
import {
  WorkOrderProyectoBodegaEntity,
  WorkOrderProyectoPersonalEntity,
  WorkOrderProyectoUbicacionEntity,
  WorkOrderEntity,
  EquipoEntity,
  EquipoFuncionamientoHistorialEntity,
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

/**
 * Las pruebas llaman a metodos privados del servicio a proposito: son la
 * logica de la OT de Proyecto y no tienen puerta publica propia. El tipo se
 * relaja para poder alcanzarlos sin abrir la clase solo para el test.
 */
type ServiceUnderTest = Record<string, any>;

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
    MATERIAL_ISSUE_ROLES: ['BODEGA', 'BODEGUERO', 'SUPER ADMINISTRADOR', 'SUPERADMINISTRADOR', 'SUPER_ADMINISTRADOR', 'SUPER ADMIN', 'SUPER_ADMIN'],
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

  describe('paso a En proceso', () => {
    it('una OT de Proyecto pasa sin programacion: no tiene equipo al cual programarla', async () => {
      const resolveProgramacionReferenceForWorkOrder = jest.fn();
      const service = createService({ resolveProgramacionReferenceForWorkOrder });
      await expect(
        service.assertWorkOrderCanMoveToInProgress({
          maintenanceKind: 'PROYECTO',
          workOrderId: 'wo-1',
        }),
      ).resolves.toBeNull();
      expect(resolveProgramacionReferenceForWorkOrder).not.toHaveBeenCalled();
    });

    it('una OT de mantenimiento sigue necesitando su programacion', async () => {
      const service = createService({
        resolveProgramacionReferenceForWorkOrder: jest.fn().mockResolvedValue(null),
      });
      await expect(
        service.assertWorkOrderCanMoveToInProgress({
          maintenanceKind: 'CORRECTIVO',
          workOrderId: 'wo-1',
        }),
      ).rejects.toThrow(BadRequestException);
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

  // La OT de Proyecto se asocia a un proyecto: un equipo cuyo tipo es
  // "Proyectos". Es un equipo solo en la base; no es una maquina, asi que nada
  // del horometro puede activarse por llevarlo.
  describe('proyecto asociado a la OT', () => {
    const tipoProyectos = {
      id: 'tipo-proyectos',
      nombre: 'PROYECTOS',
      is_deleted: false,
    };
    const tipoGeneracion = {
      id: 'tipo-generacion',
      nombre: 'UNIDAD DE GENERACION',
      is_deleted: false,
    };
    const proyecto = {
      id: 'pry-1',
      equipo_tipo_id: tipoProyectos.id,
      horometro_actual: '0.00',
    };
    const maquina = {
      id: 'eq-1',
      equipo_tipo_id: tipoGeneracion.id,
      horometro_actual: '15286.00',
    };

    const buildServiceWithTypes = () => {
      const equipoTipoRepo = createRepo();
      equipoTipoRepo.findOne.mockImplementation(
        async ({ where }: { where: { id: string } }) =>
          [tipoProyectos, tipoGeneracion].find((tipo) => tipo.id === where.id) ??
          null,
      );
      return createService({ equipoTipoRepo });
    };

    describe('validacion del equipo', () => {
      it('no aplica a una OT que no es de proyecto', async () => {
        const service = buildServiceWithTypes();
        await expect(
          service.assertProyectoWorkOrderEquipment({
            maintenanceKind: 'CORRECTIVO',
            equipment: null,
            isNew: true,
          }),
        ).resolves.toBeUndefined();
        await expect(
          service.assertProyectoWorkOrderEquipment({
            maintenanceKind: 'CORRECTIVO',
            equipment: maquina,
            isNew: true,
          }),
        ).resolves.toBeUndefined();
      });

      it('exige el proyecto al crear la OT', async () => {
        const service = buildServiceWithTypes();
        await expect(
          service.assertProyectoWorkOrderEquipment({
            maintenanceKind: 'PROYECTO',
            equipment: null,
            isNew: true,
          }),
        ).rejects.toThrow(/proyecto es obligatorio/);
      });

      it('deja seguir editando una OT guardada antes de esta regla, sin proyecto', async () => {
        const service = buildServiceWithTypes();
        await expect(
          service.assertProyectoWorkOrderEquipment({
            maintenanceKind: 'PROYECTO',
            equipment: null,
            isNew: false,
          }),
        ).resolves.toBeUndefined();
      });

      it('acepta un equipo cuyo tipo es Proyectos', async () => {
        const service = buildServiceWithTypes();
        await expect(
          service.assertProyectoWorkOrderEquipment({
            maintenanceKind: 'PROYECTO',
            equipment: proyecto,
            isNew: true,
          }),
        ).resolves.toBeUndefined();
      });

      it('no vuelve a comprobar el tipo de un proyecto que la OT ya tenia', async () => {
        const service = buildServiceWithTypes();
        // El tipo se renombro despues de crear la OT: guardarla de nuevo no debe fallar.
        const renombrado = { id: 'pry-1', equipo_tipo_id: 'tipo-renombrado' };
        await expect(
          service.assertProyectoWorkOrderEquipment({
            maintenanceKind: 'PROYECTO',
            equipment: renombrado,
            isNew: false,
            previousEquipmentId: 'pry-1',
          }),
        ).resolves.toBeUndefined();
        // Cambiar a otro equipo sigue exigiendo que sea un proyecto.
        await expect(
          service.assertProyectoWorkOrderEquipment({
            maintenanceKind: 'PROYECTO',
            equipment: maquina,
            isNew: false,
            previousEquipmentId: 'pry-1',
          }),
        ).rejects.toThrow(BadRequestException);
      });

      it('rechaza una maquina que no es un proyecto', async () => {
        const service = buildServiceWithTypes();
        await expect(
          service.assertProyectoWorkOrderEquipment({
            maintenanceKind: 'PROYECTO',
            equipment: maquina,
            isNew: true,
          }),
        ).rejects.toThrow(BadRequestException);
      });

      it('rechaza un equipo sin tipo', async () => {
        const service = buildServiceWithTypes();
        await expect(
          service.assertProyectoWorkOrderEquipment({
            maintenanceKind: 'PROYECTO',
            equipment: { id: 'sin-tipo', equipo_tipo_id: null },
            isNew: false,
          }),
        ).rejects.toThrow(BadRequestException);
      });
    });

    describe('horometro', () => {
      it('una OT de Proyecto no copia la lectura del proyecto ni exige que avance', () => {
        const service = createService();
        // Un proyecto nunca tiene horómetro, aunque el cliente envíe una lectura.
        const result = service.buildWorkOrderHorometerPayload(
          { horometro_actual: 0 },
          proyecto,
          null,
          { requireIncrease: true, maintenanceKind: 'PROYECTO' },
        );
        expect(result.horometro_actual).toBeNull();
        expect(result.horometro_anterior).toBeNull();

        const sinLectura = service.buildWorkOrderHorometerPayload(
          {},
          proyecto,
          null,
          { requireIncrease: true, maintenanceKind: 'PROYECTO' },
        );
        expect(sinLectura.horometro_actual).toBeNull();
        expect(sinLectura.horometro_anterior).toBeNull();
      });

      it('consultar una OT antes de iniciar no inventa una captura del equipo', () => {
        const service = createService();
        const result = service.buildWorkOrderHorometerPayload(
          {},
          maquina,
          null,
          { requireIncrease: true, maintenanceKind: 'CORRECTIVO' },
        );
        expect(result.horometro_actual).toBeNull();
      });

      it('leer una captura guardada no exige que supere el contador actual del equipo', () => {
        const service = createService();
        expect(() =>
          service.buildWorkOrderHorometerPayload(
            { horometro_actual: 15286 },
            maquina,
            null,
            { requireIncrease: true, maintenanceKind: 'CORRECTIVO' },
          ),
        ).not.toThrow();
      });

      it('la OT de Proyecto no actualiza el horometro del proyecto ni deja notas', async () => {
        const equipoRepo = createRepo();
        const equipoHorometroHistorialRepo = createRepo();
        const service = createService({
          equipoRepo,
          equipoHorometroHistorialRepo,
        });
        const result = await service.syncEquipmentHorometerFromWorkOrder({
          id: 'wo-1',
          code: 'OT-A00300',
          equipment_id: proyecto.id,
          maintenance_kind: 'PROYECTO',
          valor_json: { horometro_actual: 25 },
        });
        expect(result).toEqual({ notes: [], equipmentUpdated: false });
        expect(equipoRepo.findOne).not.toHaveBeenCalled();
        expect(equipoRepo.save).not.toHaveBeenCalled();
        expect(equipoHorometroHistorialRepo.save).not.toHaveBeenCalled();
      });

      it('guardar una captura de mantenimiento no modifica el reloj del equipo', async () => {
        const equipoRepo = createRepo();
        equipoRepo.findOne.mockResolvedValue({ ...maquina });
        const equipoHorometroHistorialRepo = createRepo();
        const service = createService({
          equipoRepo,
          equipoHorometroHistorialRepo,
        });
        const result = await service.syncEquipmentHorometerFromWorkOrder({
          id: 'wo-2',
          code: 'OT-A00301',
          equipment_id: maquina.id,
          maintenance_kind: 'CORRECTIVO',
          valor_json: { horometro_actual: 15300 },
        });
        expect(result.equipmentUpdated).toBe(false);
        expect(equipoRepo.findOne).not.toHaveBeenCalled();
        expect(equipoRepo.save).not.toHaveBeenCalled();
        expect(equipoHorometroHistorialRepo.save).not.toHaveBeenCalled();
      });
    });
  });

  describe('captura del horómetro al iniciar la ejecución', () => {
    const now = new Date('2026-10-10T14:00:00.000Z');
    const equipment = () => ({
      id: 'eq-automatico',
      horometro_actual: 15286,
      estado_funcionamiento: 'FUNCIONAMIENTO',
      horometro_operativo_desde: new Date('2026-10-10T12:00:00.000Z'),
      estado_funcionamiento_actualizado_en: new Date('2026-10-10T12:00:00.000Z'),
    });
    const actor = { roleName: 'BODEGA', username: 'bodega', displayName: 'Usuario Bodega' };

    beforeEach(() => { jest.useFakeTimers(); jest.setSystemTime(now); });
    afterEach(() => { jest.useRealTimers(); });

    function executionFixture(overrides: Record<string, unknown> = {}) {
      const machine = equipment();
      const workOrder: any = {
        id: 'wo-automatico', code: 'OT-A00302', equipment_id: machine.id,
        maintenance_kind: 'CORRECTIVO', status_workflow: 'PLANNED',
        valor_json: { horometro_actual: null, horometro_anterior: 15200 },
        started_at: null, is_deleted: false, ...overrides,
      };
      const manager = {
        findOne: jest.fn(async () => workOrder),
        find: jest.fn(async () => [{ id: 'entrega-1' }]),
        count: jest.fn(async () => 1),
        save: jest.fn(async (_entity: unknown, value: unknown) => value),
        create: jest.fn((_entity: unknown, value: unknown) => value),
      };
      const service = createService({
        dataSource: { transaction: jest.fn(async (callback: any) => callback(manager)) },
        assertWorkOrderVisibleForSucursal: jest.fn().mockResolvedValue(undefined),
        lockWorkOrderEquipmentForIssue: jest.fn().mockResolvedValue(machine),
        assertWorkOrderNotBlockedByActiveAnnex: jest.fn().mockResolvedValue(undefined),
        assertWorkOrderCanMoveToInProgress: jest.fn().mockResolvedValue(undefined),
        findWorkOrderIssueMovements: jest.fn().mockResolvedValue([{ id: 'egreso-1', numero_documento: 'EB-000001' }]),
        syncProgramacionExecutionFromLinkedWorkOrder: jest.fn().mockResolvedValue(undefined),
        syncAlertsForWorkOrder: jest.fn().mockResolvedValue(undefined),
        listWorkOrderIssueDocuments: jest.fn().mockResolvedValue([]),
      });
      return { service, machine, workOrder, manager };
    }

    it('crear y editar una OT planificada conserva el horómetro sin captura hasta iniciar', async () => {
      const woRepo = createRepo();
      woRepo.findOne.mockResolvedValue({ code: 'OT-ANTERIOR', valor_json: { horometro_actual: 15200 } });
      const service = createService({ woRepo });
      const machine = equipment();
      const created = await service.buildAutomaticWorkOrderHorometerPayload(
        { horometro_actual: 99999 }, machine, null, { maintenanceKind: 'CORRECTIVO' },
      );
      expect(created.horometro_actual).toBeNull();
      expect(created.horometro_capturado_en ?? null).toBeNull();
      expect(created.horometro_anterior).toBe(15200);

      jest.advanceTimersByTime(3600000);
      const edited = await service.buildAutomaticWorkOrderHorometerPayload(
        { observacion: 'Editar título', horometro_actual: 77777 }, machine, null,
        { maintenanceKind: 'CORRECTIVO', storedPayload: created },
      );
      expect(edited.horometro_actual).toBeNull();
      expect(edited.horometro_capturado_en ?? null).toBeNull();
      expect(service.operationalHorometer(machine)).toBe(15289);
    });

    it('EN PROCESO captura una vez y el reloj automático del equipo continúa', async () => {
      const { service, machine, workOrder, manager } = executionFixture();
      const originalEquipment = { ...machine };
      await service.confirmWorkOrderIssue(workOrder.id, actor);
      expect(workOrder.status_workflow).toBe('IN_PROGRESS');
      expect(workOrder.valor_json.horometro_actual).toBe(15288);
      expect(workOrder.valor_json.horometro_inicio_ejecucion).toBe(15288);
      expect(manager.save).toHaveBeenCalledWith(WorkOrderEntity, expect.objectContaining({ status_workflow: 'IN_PROGRESS' }));
      expect(manager.save.mock.calls.some(([entity]) => entity === EquipoEntity || entity === EquipoFuncionamientoHistorialEntity)).toBe(false);
      expect(machine).toEqual(originalEquipment);

      jest.advanceTimersByTime(3600000);
      expect(service.operationalHorometer(machine)).toBe(15289);
      expect(workOrder.valor_json.horometro_actual).toBe(15288);
      const callsBeforeReprint = manager.save.mock.calls.length;
      await service.confirmWorkOrderIssue(workOrder.id, actor);
      expect(manager.save.mock.calls).toHaveLength(callsBeforeReprint);
      expect(workOrder.valor_json.horometro_actual).toBe(15288);
    });

    it('editar una OT iniciada ignora lecturas del cliente y conserva su captura y fecha', async () => {
      const woRepo = createRepo();
      const service = createService({ woRepo });
      const stored = { horometro_actual: 15288, horometro_anterior: 15200,
        horometro_inicio_ejecucion: 15288, horometro_capturado_en: now.toISOString() };
      jest.advanceTimersByTime(3600000);
      const payload = await service.buildAutomaticWorkOrderHorometerPayload(
        { horometro_actual: 99999, horometro_capturado_en: '2099-01-01', observacion: 'Cambio de observación' }, equipment(), null,
        { storedPayload: stored, maintenanceKind: 'CORRECTIVO' },
      );
      expect(payload).toMatchObject(stored);
      expect(service.buildWorkOrderHorometerPayload(payload, equipment(), null, { maintenanceKind: 'CORRECTIVO' }).horometro_actual).toBe(15288);
      expect(woRepo.findOne).not.toHaveBeenCalled();
    });

    it('reanudar desde revisión conserva el inicio original sin recapturar ni detener equipo', async () => {
      const start = new Date('2026-10-10T13:00:00.000Z');
      const { service, machine, workOrder, manager } = executionFixture({
        status_workflow: 'REVIEW', started_at: start,
        valor_json: { horometro_actual: 15287, horometro_inicio_ejecucion: 15287, horometro_capturado_en: start.toISOString() },
      });
      await service.confirmWorkOrderIssue(workOrder.id, actor);
      expect(workOrder.status_workflow).toBe('IN_PROGRESS');
      expect(workOrder.started_at).toEqual(start);
      expect(workOrder.valor_json.horometro_actual).toBe(15287);
      expect(workOrder.valor_json.horometro_capturado_en).toBe(start.toISOString());
      expect(machine.estado_funcionamiento).toBe('FUNCIONAMIENTO');
      expect(manager.save.mock.calls.some(([entity]) => entity === EquipoEntity)).toBe(false);
    });

    it('una OT de proyecto puede iniciar sin capturar ni activar un horómetro', async () => {
      const { service, workOrder } = executionFixture({ maintenance_kind: 'PROYECTO',
        valor_json: { horometro_actual: null, horometro_anterior: null } });
      await service.confirmWorkOrderIssue(workOrder.id, actor);
      expect(workOrder.status_workflow).toBe('IN_PROGRESS');
      expect(workOrder.valor_json.horometro_actual).toBeNull();
      expect(workOrder.valor_json.horometro_inicio_ejecucion ?? null).toBeNull();
    });

    it('reprogramar una OT iniciada conserva la captura original', async () => {
      const machine = equipment();
      const workOrder = { id: 'wo-programacion', equipment_id: machine.id, status_workflow: 'IN_PROGRESS',
        valor_json: { horometro_actual: 15287, horometro_capturado_en: '2026-10-10T13:00:00.000Z' } };
      const woRepo = createRepo();
      woRepo.findOne.mockResolvedValue(workOrder);
      const equipoRepo = createRepo();
      equipoRepo.findOne.mockResolvedValue(machine);
      const service = createService({ woRepo, equipoRepo, appendWorkOrderHistory: jest.fn().mockResolvedValue(undefined) });
      await service.syncReprogrammingHorometer({ workOrderId: workOrder.id, actor });
      expect(workOrder.valor_json.horometro_actual).toBe(15287);
      expect(workOrder.valor_json.horometro_capturado_en).toBe('2026-10-10T13:00:00.000Z');
      expect(equipoRepo.save).not.toHaveBeenCalled();
    });
  });
});
