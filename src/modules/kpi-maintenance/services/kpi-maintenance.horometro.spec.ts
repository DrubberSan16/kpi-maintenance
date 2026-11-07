import { BadRequestException, ForbiddenException } from '@nestjs/common';
import { KpiMaintenanceService } from './kpi-maintenance.service';
import { EquipoEntity } from '../entities/kpi-maintenance.entity';

/**
 * Bajar el horometro desde el control operativo del Dashboard.
 *
 * El horometro es un contador fisico: solo avanza. Que baje casi siempre es un
 * error de tecleo, y ese error contamina el par "anterior -> actual" de todos
 * los informes. Por eso corregirlo hacia atras esta reservado a Administrador y
 * Super Administrador, y exige decir por que.
 */
type ServiceUnderTest = Pick<
  KpiMaintenanceService,
  'updateEquipo' | 'updateEquipoHorometro' | 'listHorometroAjustesDirectos'
> & {
  puedeRegistrarHorometroMenor: (role?: string | null) => boolean;
  hasEquipmentEditPermission: jest.Mock<
    Promise<boolean>,
    [actor?: object | null]
  >;
  findEquipoOrFail: jest.Mock<Promise<Partial<EquipoEntity>>, [id: string]>;
  getJson: jest.Mock<Promise<unknown>, [url: string]>;
  logger: { warn: jest.Mock<void, [message: string]> };
  operationalHorometer: (equipment: EquipoEntity, now: Date) => number;
  buildHorometerAdjustmentNote: (
    backward: boolean,
    previous: number,
    reason: string | null,
  ) => string;
  extractHorometerAdjustmentReason: (note: string | null) => string | null;
};

function createService(
  overrides: Record<string, unknown> = {},
): ServiceUnderTest {
  const service = Object.create(
    KpiMaintenanceService.prototype,
  ) as ServiceUnderTest;
  Object.assign(service, {
    // Las propiedades de clase se inicializan en el constructor, que aqui no
    // corre: se repiten las que la logica bajo prueba consulta.
    HOROMETRO_FUENTE_AJUSTE_DIRECTO: 'AJUSTE_DIRECTO',
    HOROMETRO_FUENTE_MANUAL: 'MANUAL_EQUIPOS',
    hasEquipmentEditPermission: jest.fn().mockResolvedValue(true),
    findEquipoOrFail: jest.fn().mockResolvedValue({ horometro_actual: 1000 }),
    updateEquipo: jest.fn().mockResolvedValue({ data: {} }),
    ...overrides,
  });
  return service;
}

describe('KpiMaintenanceService horometro del control operativo', () => {
  describe('quien puede registrar una lectura menor', () => {
    it('admite administrador y super administrador, con o sin acentos', () => {
      const service = createService();
      for (const rol of [
        'Administrador',
        'ADMINISTRADOR DEL SISTEMA',
        'Súper Administrador',
        'SUPER_ADMIN',
      ]) {
        expect(service.puedeRegistrarHorometroMenor(rol)).toBe(true);
      }
    });

    it('rechaza al resto de perfiles', () => {
      const service = createService();
      for (const rol of ['Operador', 'Supervisor', 'Tecnico', 'Bodega', '', null]) {
        expect(service.puedeRegistrarHorometroMenor(rol)).toBe(false);
      }
    });
  });

  describe('updateEquipoHorometro', () => {
    it('rechaza una petición directa sin permiso aunque la lectura avance', async () => {
      const service = createService({
        hasEquipmentEditPermission: jest.fn().mockResolvedValue(false),
      });
      await expect(
        service.updateEquipoHorometro(
          'eq-1',
          { horometro_actual: 1200 },
          { userId: 'sin-permiso' },
        ),
      ).rejects.toThrow(ForbiddenException);
      expect(service.findEquipoOrFail).not.toHaveBeenCalled();
      expect(service.updateEquipo).not.toHaveBeenCalled();
    });
    it('deja subir la lectura sin pedir motivo', async () => {
      const service = createService();
      await service.updateEquipoHorometro(
        'eq-1',
        { horometro_actual: 1200 },
        { roleName: 'Operador' },
      );
      expect(service.updateEquipo).toHaveBeenCalledWith(
        'eq-1',
        { horometro_actual: 1200 },
        { roleName: 'Operador' },
      );
    });

    it('rechaza repetir la lectura vigente', async () => {
      const service = createService();
      await expect(
        service.updateEquipoHorometro(
          'eq-1',
          { horometro_actual: 1000 },
          { roleName: 'Administrador' },
        ),
      ).rejects.toThrow(BadRequestException);
      expect(service.updateEquipo).not.toHaveBeenCalled();
    });

    it('no deja bajar la lectura a un perfil que no es administrativo', async () => {
      const service = createService();
      await expect(
        service.updateEquipoHorometro(
          'eq-1',
          { horometro_actual: 800, motivo: 'se tecleo mal' },
          { roleName: 'Supervisor' },
        ),
      ).rejects.toThrow(ForbiddenException);
      expect(service.updateEquipo).not.toHaveBeenCalled();
    });

    it('exige motivo cuando la lectura baja', async () => {
      const service = createService();
      await expect(
        service.updateEquipoHorometro(
          'eq-1',
          { horometro_actual: 800 },
          { roleName: 'Administrador' },
        ),
      ).rejects.toThrow(/motivo/i);
      await expect(
        service.updateEquipoHorometro(
          'eq-1',
          { horometro_actual: 800, motivo: '   ' },
          { roleName: 'Administrador' },
        ),
      ).rejects.toThrow(BadRequestException);
      expect(service.updateEquipo).not.toHaveBeenCalled();
    });

    it('baja la lectura y traslada el motivo cuando el perfil corresponde', async () => {
      const service = createService();
      await service.updateEquipoHorometro(
        'eq-1',
        { horometro_actual: 800, motivo: '  Se registro 1000 por error de tecleo  ' },
        { roleName: 'Súper Administrador' },
      );
      expect(service.updateEquipo).toHaveBeenCalledWith(
        'eq-1',
        {
          horometro_actual: 800,
          horometro_motivo: 'Se registro 1000 por error de tecleo',
        },
        { roleName: 'Súper Administrador' },
      );
    });

    it('acepta la primera lectura de un equipo que aun no tiene horometro', async () => {
      const service = createService({
        findEquipoOrFail: jest.fn().mockResolvedValue({ horometro_actual: null }),
      });
      await service.updateEquipoHorometro(
        'eq-1',
        { horometro_actual: 50 },
        { roleName: 'Operador' },
      );
      expect(service.updateEquipo).toHaveBeenCalled();
    });
  });

  describe('permiso de Equipos en seguridad', () => {
    function permissionService(payload: unknown) {
      const service = createService({
        securityServiceUrl: 'http://security',
        getJson: jest.fn().mockResolvedValue(payload),
        logger: { warn: jest.fn() },
      });
      Reflect.deleteProperty(service, 'hasEquipmentEditPermission');
      return service;
    }

    it('consulta la identidad del gateway y busca el componente aunque cambie el nombre', async () => {
      const service = permissionService({
        data: [
          {
            children: [
              {
                nombre: 'Unidades de Generación',
                urlComponent: '/app/equipos',
                permissions: { isReaded: true, isEdited: true },
              },
            ],
          },
        ],
      });
      await expect(
        service.hasEquipmentEditPermission({
          userId: 'usuario-1',
          roleName: 'Supervisor',
        }),
      ).resolves.toBe(true);
      expect(service.getJson).toHaveBeenCalledWith(
        'http://security/menu-users/tree/by-user/usuario-1',
      );
    });

    it.each([
      {
        nombre: 'Equipos',
        urlComponent: 'otro',
        permissions: { isReaded: true, isEdited: true },
      },
      {
        urlComponent: 'equipos',
        permissions: { isReaded: true, isEdited: false },
      },
      {
        urlComponent: 'equipos',
        permissions: { isReaded: false, isEdited: true },
      },
    ])(
      'no concede acceso por nombre, solo lectura o edición sin lectura: %o',
      async (node) => {
        const service = permissionService([node]);
        await expect(
          service.hasEquipmentEditPermission({ userId: 'usuario-1' }),
        ).resolves.toBe(false);
      },
    );

    it('mantiene el acceso del Super Administrador sin consultar seguridad', async () => {
      const service = permissionService([]);
      await expect(
        service.hasEquipmentEditPermission({ roleName: 'Super Administrador' }),
      ).resolves.toBe(true);
      expect(service.getJson).not.toHaveBeenCalled();
    });

    it('deniega sin identidad o cuando seguridad no responde', async () => {
      const service = permissionService([]);
      await expect(service.hasEquipmentEditPermission(null)).resolves.toBe(
        false,
      );
      expect(service.getJson).not.toHaveBeenCalled();
      service.getJson.mockRejectedValueOnce(new Error('sin conexión'));
      await expect(
        service.hasEquipmentEditPermission({ userId: 'usuario-1' }),
      ).resolves.toBe(false);
      expect(service.logger.warn).toHaveBeenCalled();
    });
  });

  describe('fila que queda en el historial', () => {
    /**
     * `updateEquipo` escribe la fila dentro de una transaccion. Se sustituye por
     * un doble que ejecuta el callback con repositorios de mentira, para poder
     * mirar exactamente lo que se guarda.
     */
    function createServiceForHistory(current: Partial<EquipoEntity>) {
      let stored = { ...current };
      const historyRepo = {
        create: jest.fn((value: unknown) => value),
        save: jest.fn((value: unknown) => Promise.resolve(value)),
      };
      const equipoRepo = {
        findOne: jest.fn(() => Promise.resolve({ ...stored })),
        save: jest.fn((value: Partial<EquipoEntity>) => {
          stored = { ...value };
          return Promise.resolve(value);
        }),
      };
      const service = createService({
        findEquipoOrFail: jest.fn().mockResolvedValue({ ...current }),
        triggerAlertRecalculation: jest.fn(),
        dataSource: {
          transaction: jest.fn(
            (
              cb: (manager: {
                getRepository: (entity: { name?: string }) => unknown;
              }) => Promise<unknown>,
            ) =>
            cb({
                getRepository: (entity: { name?: string }) =>
                entity?.name === 'EquipoHorometroHistorialEntity'
                  ? historyRepo
                  : equipoRepo,
            }),
          ),
        },
      });
      Reflect.deleteProperty(service, 'updateEquipo');
      return { service, historyRepo, equipoRepo };
    }

    it('marca AJUSTE_DIRECTO y guarda el motivo cuando la lectura baja', async () => {
      const { service, historyRepo } = createServiceForHistory({
        id: 'eq-1',
        horometro_actual: 1000,
      });
      await service.updateEquipo(
        'eq-1',
        { horometro_actual: 800, horometro_motivo: 'Error de tecleo' },
        { userId: null, username: 'admin', roleName: 'Administrador' },
      );
      expect(historyRepo.save).toHaveBeenCalledWith(
        expect.objectContaining({
          fuente: 'AJUSTE_DIRECTO',
          horometro_nuevo: 800,
          observacion: expect.stringContaining('Error de tecleo'),
        }),
      );
    });

    it('una lectura que avanza sigue siendo MANUAL_EQUIPOS', async () => {
      const { service, historyRepo } = createServiceForHistory({
        id: 'eq-1',
        horometro_actual: 1000,
      });
      await service.updateEquipo(
        'eq-1',
        { horometro_actual: 1200 },
        { userId: null, username: 'operador' },
      );
      expect(historyRepo.save).toHaveBeenCalledWith(
        expect.objectContaining({ fuente: 'MANUAL_EQUIPOS', horometro_nuevo: 1200 }),
      );
    });

    it('la API general de Equipos también exige permiso cuando cambia el horómetro', async () => {
      const { service, equipoRepo } = createServiceForHistory({
        id: 'eq-1',
        horometro_actual: 1000,
      });
      service.hasEquipmentEditPermission.mockResolvedValue(false);
      await expect(
        service.updateEquipo(
          'eq-1',
          { horometro_actual: 1200 },
          { userId: 'sin-permiso' },
        ),
      ).rejects.toThrow(ForbiddenException);
      expect(equipoRepo.save).not.toHaveBeenCalled();
    });

    it('el perfil no administrativo no puede saltarse la restricción descendente por la API general', async () => {
      const { service, equipoRepo } = createServiceForHistory({
        id: 'eq-1',
        horometro_actual: 1000,
      });
      await expect(
        service.updateEquipo(
          'eq-1',
          { horometro_actual: 800, horometro_motivo: 'Error' },
          { roleName: 'Supervisor' },
        ),
      ).rejects.toThrow(ForbiddenException);
      await expect(
        service.updateEquipo(
          'eq-1',
          { horometro_actual: 800 },
          { roleName: 'Administrador' },
        ),
      ).rejects.toThrow(/motivo/);
      expect(equipoRepo.save).not.toHaveBeenCalled();
    });

    it('compara contra el contador proyectado bajo bloqueo y reinicia su referencia al registrar lectura', async () => {
      jest.useFakeTimers().setSystemTime(new Date('2026-11-06T15:00:00Z'));
      try {
        const { service, equipoRepo, historyRepo } = createServiceForHistory({
          id: 'eq-1',
          horometro_actual: 1000,
          estado_funcionamiento: 'FUNCIONAMIENTO',
          horometro_operativo_desde: new Date('2026-11-06T13:00:00Z'),
        });
        // 1001 supera la base almacenada, pero baja respecto del contador vigente: 1002.
        await expect(
          service.updateEquipo(
            'eq-1',
            { horometro_actual: 1001 },
            { roleName: 'Supervisor' },
          ),
        ).rejects.toThrow(ForbiddenException);
        expect(equipoRepo.save).not.toHaveBeenCalled();
        const result = await service.updateEquipo(
          'eq-1',
          { horometro_actual: 1200 },
          { roleName: 'Supervisor' },
        );
        expect(result.data).toMatchObject({
          horometro_actual: 1200,
          horometro_operativo_desde: new Date('2026-11-06T15:00:00Z'),
        });
        expect(historyRepo.save).toHaveBeenCalledWith(
          expect.objectContaining({
            horometro_anterior: 1002,
            horometro_nuevo: 1200,
          }),
        );
        expect(
          service.operationalHorometer(
            result.data,
            new Date('2026-11-06T15:00:00Z'),
          ),
        ).toBe(1200);
        expect(
          service.operationalHorometer(
            result.data,
            new Date('2026-11-06T16:00:00Z'),
          ),
        ).toBe(1201);
      } finally {
        jest.useRealTimers();
      }
    });

    it('editar la ficha con la lectura vigente conserva el reloj y no exige permiso adicional de horómetro', async () => {
      jest.useFakeTimers().setSystemTime(new Date('2026-11-06T15:00:00Z'));
      try {
        const since = new Date('2026-11-06T13:00:00Z');
        const { service, historyRepo } = createServiceForHistory({
          id: 'eq-1',
          horometro_actual: 1000,
          estado_funcionamiento: 'FUNCIONAMIENTO',
          horometro_operativo_desde: since,
        });
        service.hasEquipmentEditPermission.mockResolvedValue(false);
        const result = await service.updateEquipo('eq-1', {
          nombre: 'Nombre corregido',
          horometro_actual: 1002,
        });
        expect(result.data).toMatchObject({
          nombre: 'Nombre corregido',
          horometro_actual: 1000,
          horometro_operativo_desde: since,
        });
        expect(historyRepo.save).not.toHaveBeenCalled();
      } finally {
        jest.useRealTimers();
      }
    });
  });

  describe('observacion del historial', () => {
    it('pone el motivo por delante cuando la lectura baja', () => {
      const service = createService();
      expect(
        service.buildHorometerAdjustmentNote(true, 1000, 'Error de tecleo'),
      ).toBe(
        'Error de tecleo · Correccion manual descendente desde 1000; la nueva lectura se establece como base anterior.',
      );
    });

    it('mantiene la nota de siempre cuando no hay motivo', () => {
      const service = createService();
      expect(service.buildHorometerAdjustmentNote(false, 1000, null)).toBe(
        'Actualizacion manual desde el modulo Equipos.',
      );
    });
  });

  describe('motivo de un ajuste directo', () => {
    it('recupera el motivo que se escribio delante de la nota', () => {
      const service = createService();
      const nota = service.buildHorometerAdjustmentNote(
        true,
        1000,
        'Error de tecleo',
      );
      expect(service.extractHorometerAdjustmentReason(nota)).toBe(
        'Error de tecleo',
      );
    });

    it('conserva un motivo que lleva el mismo separador', () => {
      const service = createService();
      const nota = service.buildHorometerAdjustmentNote(
        true,
        1000,
        'Tablero cambiado · lectura del nuevo',
      );
      expect(service.extractHorometerAdjustmentReason(nota)).toBe(
        'Tablero cambiado · lectura del nuevo',
      );
    });

    it('no presenta la nota automatica de las filas antiguas como motivo', () => {
      const service = createService();
      expect(
        service.extractHorometerAdjustmentReason(
          'Correccion manual descendente desde 1000; la nueva lectura se establece como base anterior.',
        ),
      ).toBeNull();
      expect(service.extractHorometerAdjustmentReason(null)).toBeNull();
    });
  });

  describe('ajustes directos de todos los equipos', () => {
    function createServiceForAdjustments(rows: unknown[]) {
      const qb: Record<string, jest.Mock> = {};
      for (const method of ['innerJoin', 'where', 'andWhere', 'orderBy']) {
        qb[method] = jest.fn(() => qb);
      }
      qb.getMany = jest.fn(async () => rows);
      const service = createService({
        equipoHorometroHistorialRepo: { createQueryBuilder: jest.fn(() => qb) },
      });
      return { service, qb };
    }

    it('lista solo AJUSTE_DIRECTO de equipos vigentes', async () => {
      const { service, qb } = createServiceForAdjustments([]);
      await service.listHorometroAjustesDirectos({});
      expect(qb.innerJoin).toHaveBeenCalledWith(
        expect.anything(),
        'e',
        'e.id = h.equipo_id AND e.is_deleted = false',
      );
      expect(qb.where).toHaveBeenCalledWith('UPPER(h.fuente) = :fuente', {
        fuente: 'AJUSTE_DIRECTO',
      });
      expect(qb.andWhere).not.toHaveBeenCalled();
    });

    it('una fecha de formulario cubre el dia completo', async () => {
      const { service, qb } = createServiceForAdjustments([]);
      await service.listHorometroAjustesDirectos({
        from: '2026-09-01',
        to: '2026-09-30',
      });
      expect(qb.andWhere).toHaveBeenCalledWith('h.changed_at >= :from', {
        from: '2026-09-01 00:00:00',
      });
      expect(qb.andWhere).toHaveBeenCalledWith('h.changed_at <= :to', {
        to: '2026-09-30 23:59:59.999',
      });
    });

    it('respeta una fecha que ya trae hora', async () => {
      const { service, qb } = createServiceForAdjustments([]);
      await service.listHorometroAjustesDirectos({
        to: '2026-09-30T12:00:00',
      });
      expect(qb.andWhere).toHaveBeenCalledWith('h.changed_at <= :to', {
        to: '2026-09-30T12:00:00',
      });
    });

    it('devuelve cada ajuste con su motivo aparte', async () => {
      const { service } = createServiceForAdjustments([
        {
          id: 'h-1',
          equipo_id: 'eq-1',
          horometro_anterior: '1000.00',
          horometro_nuevo: '800.00',
          fuente: 'AJUSTE_DIRECTO',
          observacion:
            'Error de tecleo · Correccion manual descendente desde 1000; la nueva lectura se establece como base anterior.',
        },
      ]);
      const result = await service.listHorometroAjustesDirectos({});
      expect(result.data).toEqual([
        expect.objectContaining({
          id: 'h-1',
          es_ajuste_directo: true,
          motivo: 'Error de tecleo',
        }),
      ]);
    });
  });
});
