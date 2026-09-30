/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access --
   La prueba llama a metodos privados del servicio a proposito (no tienen puerta
   publica propia); por eso el servicio se maneja como `Record<string, any>`. */
import { BadRequestException } from '@nestjs/common';
import { KpiMaintenanceService } from './kpi-maintenance.service';
import { buildEmployeeDirectory } from './work-order-responsables.util';

/**
 * Responsables de las tareas y de las plantillas: el pegamento entre el servicio
 * y `work-order-responsables.util` (cuya logica pura ya tiene su propia prueba).
 * Aqui se comprueba lo que solo el servicio hace: leer el directorio de empleados
 * de la base, el nombre de los usuarios, y decidir cuando un fallo es un error y
 * cuando no.
 *
 * Como en las demas pruebas del servicio, se arma la instancia sobre el prototipo
 * (el constructor pide casi cincuenta repositorios) y se inyecta lo que el caso usa.
 */
type ServiceUnderTest = Record<string, any>;

const ABRIL = '11111111-1111-4111-8111-111111111111'; // sin usuario
const BENITEZ = '22222222-2222-4222-8222-222222222222'; // con usuario
const CEDENO = '33333333-3333-4333-8333-333333333333'; // de baja
const USUARIO_BENITEZ = 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb';
const USUARIO_SIN_EMPLEADO = 'cccccccc-cccc-4ccc-8ccc-cccccccccccc';

const FILAS_EMPLEADOS = [
  {
    id: ABRIL,
    user_id: null,
    nombres_apellidos: 'ABRIL BILVAO VICTOR',
    valor_hora: '2.9167',
    status: 'ACTIVE',
    is_deleted: false,
  },
  {
    id: BENITEZ,
    user_id: USUARIO_BENITEZ,
    nombres_apellidos: 'BENITEZ MORA ANA',
    valor_hora: '5.0000',
    status: 'ACTIVE',
    is_deleted: false,
  },
  {
    id: CEDENO,
    user_id: null,
    nombres_apellidos: 'CEDENO TRIVINO JEAN',
    valor_hora: '6.2500',
    status: 'INACTIVE',
    is_deleted: false,
  },
];

const USUARIOS = [
  {
    id: USUARIO_BENITEZ,
    nameUser: 'ana.benitez',
    nameSurname: 'Ana Benitez',
    status: 'ACTIVE',
    isDeleted: false,
  },
  {
    id: USUARIO_SIN_EMPLEADO,
    nameUser: 'priscila.alarcon',
    nameSurname: 'ALARCON OSIO PRISCILA',
    status: 'ACTIVE',
    isDeleted: false,
  },
];

function createService(
  options: { filas?: unknown[]; consultaFalla?: boolean } = {},
) {
  const query = options.consultaFalla
    ? jest.fn().mockRejectedValue(new Error('la base no responde'))
    : jest.fn().mockResolvedValue(options.filas ?? FILAS_EMPLEADOS);
  const logger = { warn: jest.fn(), debug: jest.fn() };
  const service = Object.create(
    KpiMaintenanceService.prototype,
  ) as ServiceUnderTest;
  Object.assign(service, {
    dataSource: { query },
    logger,
    employeeDirectoryCache: null,
    fetchSecurityUsers: jest.fn().mockResolvedValue(USUARIOS),
  });
  return { service, query, logger };
}

describe('KpiMaintenanceService responsables por empleado', () => {
  describe('directorio de empleados', () => {
    it('lo lee de la base, con las bajas, y lo guarda unos segundos', async () => {
      const { service, query } = createService();
      const primero = await service.loadEmployeeDirectory();
      const segundo = await service.loadEmployeeDirectory();

      expect(primero.byId.size).toBe(3);
      expect(primero.byId.get(ABRIL).valor_hora).toBe(2.9167);
      // el usuario de un empleado lo lleva a su empleado
      expect(primero.byUserId.get(USUARIO_BENITEZ).id).toBe(BENITEZ);
      expect(segundo).toBe(primero);
      expect(query).toHaveBeenCalledTimes(1);
    });

    it('quien guarda lo pide fresco: de ahi sale el costo que se congela', async () => {
      const { service, query } = createService();
      await service.loadEmployeeDirectory();
      await service.loadEmployeeDirectory({ fresh: true });
      expect(query).toHaveBeenCalledTimes(2);
    });

    it('al leer, un fallo de la base deja el directorio vacio y avisa; no rompe la pantalla', async () => {
      const { service, logger } = createService({ consultaFalla: true });
      const directorio = await service.loadEmployeeDirectory();
      expect(directorio.byId.size).toBe(0);
      expect(logger.warn).toHaveBeenCalled();
    });

    it('al guardar, un fallo de la base es un error: no se aceptan responsables sin validarlos', async () => {
      const { service } = createService({ consultaFalla: true });
      await expect(
        service.normalizeWorkOrderTaskResponsables([
          { empleado_id: ABRIL, horas: 1 },
        ]),
      ).rejects.toThrow('la base no responde');
    });
  });

  describe('responsables de una tarea al guardar', () => {
    it('sin responsables no consulta nada', async () => {
      const { service, query } = createService();
      expect(await service.normalizeWorkOrderTaskResponsables([])).toEqual([]);
      expect(await service.normalizeWorkOrderTaskResponsables(null)).toEqual(
        [],
      );
      expect(query).not.toHaveBeenCalled();
    });

    it('un empleado sin usuario queda por su empleado, con el costo de su hora', async () => {
      const { service } = createService();
      const guardados = await service.normalizeWorkOrderTaskResponsables([
        { empleado_id: ABRIL, horas: 2 },
      ]);
      expect(guardados).toEqual([
        {
          empleado_id: ABRIL,
          user_id: null,
          username: null,
          display_name: 'ABRIL BILVAO VICTOR',
          horas: 2,
          costo_hora: 2.9167,
        },
      ]);
    });

    it('un empleado con usuario lleva tambien el usuario y su nombre de usuario', async () => {
      const { service } = createService();
      const [guardado] = await service.normalizeWorkOrderTaskResponsables([
        { empleado_id: BENITEZ, horas: 1 },
      ]);
      expect(guardado.user_id).toBe(USUARIO_BENITEZ);
      expect(guardado.username).toBe('ana.benitez');
      expect(guardado.costo_hora).toBe(5);
    });

    it('el costo congelado no se recalcula cuando el empleado cambia de valor por hora', async () => {
      const { service } = createService();
      const yaGuardado = [
        {
          empleado_id: ABRIL,
          user_id: null,
          display_name: 'ABRIL BILVAO VICTOR',
          horas: 1,
          costo_hora: 2.5, // lo que valia la hora cuando se asigno
        },
      ];
      const [guardado] = await service.normalizeWorkOrderTaskResponsables(
        [{ empleado_id: ABRIL, horas: 3 }],
        yaGuardado,
      );
      expect(guardado.horas).toBe(3);
      expect(guardado.costo_hora).toBe(2.5);
    });

    it('quien se quita y se vuelve a agregar toma el valor por hora de hoy', async () => {
      const { service } = createService();
      const [guardado] = await service.normalizeWorkOrderTaskResponsables(
        [{ empleado_id: ABRIL, horas: 1 }],
        [], // la tarea ya no lo tenia
      );
      expect(guardado.costo_hora).toBe(2.9167);
    });

    it('un usuario con empleado vinculado (pestana vieja) se guarda como su empleado', async () => {
      const { service } = createService();
      const [guardado] = await service.normalizeWorkOrderTaskResponsables([
        { user_id: USUARIO_BENITEZ, horas: 2 },
      ]);
      expect(guardado.empleado_id).toBe(BENITEZ);
      expect(guardado.costo_hora).toBe(5);
    });

    it('un usuario que no es empleado se rechaza con el nombre de quien es', async () => {
      const { service } = createService();
      await expect(
        service.normalizeWorkOrderTaskResponsables([
          { user_id: USUARIO_SIN_EMPLEADO, horas: 1 },
        ]),
      ).rejects.toThrow(BadRequestException);
      await expect(
        service.normalizeWorkOrderTaskResponsables([
          { user_id: USUARIO_SIN_EMPLEADO, horas: 1 },
        ]),
      ).rejects.toThrow(/ALARCON OSIO PRISCILA no es un empleado/);
    });

    it('pero si ya estaba en la tarea se conserva: cerrar una OT antigua no debe fallar', async () => {
      const { service } = createService();
      const yaGuardado = [
        {
          user_id: USUARIO_SIN_EMPLEADO,
          username: 'priscila.alarcon',
          display_name: 'ALARCON OSIO PRISCILA',
          horas: 2,
        },
      ];
      const [guardado] = await service.normalizeWorkOrderTaskResponsables(
        [{ user_id: USUARIO_SIN_EMPLEADO, horas: 4 }],
        yaGuardado,
      );
      expect(guardado.user_id).toBe(USUARIO_SIN_EMPLEADO);
      expect(guardado.horas).toBe(4);
      expect(guardado.empleado_id).toBeNull();
    });

    it('un empleado de baja no se puede agregar, pero si ya estaba se conserva', async () => {
      const { service } = createService();
      await expect(
        service.normalizeWorkOrderTaskResponsables([
          { empleado_id: CEDENO, horas: 1 },
        ]),
      ).rejects.toThrow(/no está activo/);

      const [guardado] = await service.normalizeWorkOrderTaskResponsables(
        [{ empleado_id: CEDENO, horas: 2 }],
        [
          {
            empleado_id: CEDENO,
            display_name: 'CEDENO TRIVINO JEAN',
            horas: 1,
            costo_hora: 6,
          },
        ],
      );
      expect(guardado.costo_hora).toBe(6);
    });

    it('unas horas invalidas se rechazan con un mensaje claro', async () => {
      const { service } = createService();
      await expect(
        service.normalizeWorkOrderTaskResponsables([
          { empleado_id: ABRIL, horas: -2 },
        ]),
      ).rejects.toThrow(/deben ser numéricas y mayores o iguales a cero/);
    });
  });

  describe('lectura de lo guardado', () => {
    it('lo guardado por usuario aparece con su empleado y con su costo de hoy si no tenia', () => {
      const { service } = createService();
      const [leido] = service.mapStoredWorkOrderTaskResponsables(
        [{ user_id: USUARIO_BENITEZ, username: 'ana.benitez', horas: 3 }],
        undefined,
        buildEmployeeDirectory(FILAS_EMPLEADOS),
      );
      expect(leido.empleado_id).toBe(BENITEZ);
      expect(leido.display_name).toBe('BENITEZ MORA ANA');
      expect(leido.costo_hora).toBe(5);
    });
  });

  describe('responsables de una plantilla', () => {
    it('un id de usuario con empleado vinculado se guarda como el id de su empleado', async () => {
      const { service } = createService();
      const ids = await service.normalizeProcedimientoResponsabilidades([
        USUARIO_BENITEZ,
        ABRIL,
      ]);
      expect(ids).toEqual([BENITEZ, ABRIL]);
    });

    it('un usuario que no es empleado se rechaza salvo que la plantilla ya lo tuviera', async () => {
      const { service } = createService();
      await expect(
        service.normalizeProcedimientoResponsabilidades([USUARIO_SIN_EMPLEADO]),
      ).rejects.toThrow(BadRequestException);
      const ids = await service.normalizeProcedimientoResponsabilidades(
        [USUARIO_SIN_EMPLEADO, ABRIL],
        [USUARIO_SIN_EMPLEADO],
      );
      expect(ids).toEqual([USUARIO_SIN_EMPLEADO, ABRIL]);
    });

    it('sin responsables devuelve una lista vacia sin consultar', async () => {
      const { service, query } = createService();
      expect(await service.normalizeProcedimientoResponsabilidades([])).toEqual(
        [],
      );
      expect(
        await service.normalizeProcedimientoResponsabilidades(null),
      ).toEqual([]);
      expect(query).not.toHaveBeenCalled();
    });

    it('el detalle trae el nombre de cada uno y marca lo que ya no es elegible', async () => {
      const { service } = createService();
      const detalle = await service.buildProcedimientoResponsabilidadesDetalle([
        USUARIO_BENITEZ,
        CEDENO,
        USUARIO_SIN_EMPLEADO,
      ]);
      expect(detalle.map((d: { id: string }) => d.id)).toEqual([
        BENITEZ,
        CEDENO,
        USUARIO_SIN_EMPLEADO,
      ]);
      expect(detalle[0]).toMatchObject({
        empleado_id: BENITEZ,
        label: 'BENITEZ MORA ANA',
        status: 'ACTIVE',
        is_deleted: false,
      });
      expect(detalle[1]).toMatchObject({ status: 'INACTIVE' });
      expect(detalle[2]).toMatchObject({
        empleado_id: null,
        user_id: USUARIO_SIN_EMPLEADO,
        label: 'ALARCON OSIO PRISCILA',
      });
    });
  });
});
