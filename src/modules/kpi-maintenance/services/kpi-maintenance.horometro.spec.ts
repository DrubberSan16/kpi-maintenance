import { BadRequestException, ForbiddenException } from '@nestjs/common';
import { KpiMaintenanceService } from './kpi-maintenance.service';

/**
 * Bajar el horometro desde el control operativo del Dashboard.
 *
 * El horometro es un contador fisico: solo avanza. Que baje casi siempre es un
 * error de tecleo, y ese error contamina el par "anterior -> actual" de todos
 * los informes. Por eso corregirlo hacia atras esta reservado a Administrador y
 * Super Administrador, y exige decir por que.
 */
type ServiceUnderTest = Record<string, any>;

function createService(overrides: Record<string, any> = {}): ServiceUnderTest {
  const service = Object.create(
    KpiMaintenanceService.prototype,
  ) as ServiceUnderTest;
  Object.assign(service, {
    // Las propiedades de clase se inicializan en el constructor, que aqui no
    // corre: se repiten las que la logica bajo prueba consulta.
    HOROMETRO_FUENTE_AJUSTE_DIRECTO: 'AJUSTE_DIRECTO',
    HOROMETRO_FUENTE_MANUAL: 'MANUAL_EQUIPOS',
    findEquipoOrFail: jest.fn().mockResolvedValue({ horometro_actual: 1000 }),
    updateEquipo: jest.fn(async () => ({ data: {} })),
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

  describe('fila que queda en el historial', () => {
    /**
     * `updateEquipo` escribe la fila dentro de una transaccion. Se sustituye por
     * un doble que ejecuta el callback con repositorios de mentira, para poder
     * mirar exactamente lo que se guarda.
     */
    function createServiceForHistory(current: Record<string, any>) {
      const historyRepo = {
        create: jest.fn((value: unknown) => value),
        save: jest.fn(async (value: unknown) => value),
      };
      const equipoRepo = {
        findOne: jest.fn(async () => ({ ...current })),
        save: jest.fn(async (value: any) => value),
      };
      const service = createService({
        findEquipoOrFail: jest.fn().mockResolvedValue({ ...current }),
        triggerAlertRecalculation: jest.fn(),
        dataSource: {
          transaction: jest.fn(async (cb: any) =>
            cb({
              getRepository: (entity: any) =>
                entity?.name === 'EquipoHorometroHistorialEntity'
                  ? historyRepo
                  : equipoRepo,
            }),
          ),
        },
      });
      delete (service as any).updateEquipo;
      return { service, historyRepo };
    }

    it('marca AJUSTE_DIRECTO y guarda el motivo cuando la lectura baja', async () => {
      const { service, historyRepo } = createServiceForHistory({
        id: 'eq-1',
        horometro_actual: 1000,
      });
      await service.updateEquipo(
        'eq-1',
        { horometro_actual: 800, horometro_motivo: 'Error de tecleo' },
        { userId: null, username: 'admin' },
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
});
