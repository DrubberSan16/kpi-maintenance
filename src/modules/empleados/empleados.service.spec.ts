import { plainToInstance } from 'class-transformer';
import { Repository } from 'typeorm';
import { validate } from 'class-validator';
import {
  BadRequestException,
  ConflictException,
  NotFoundException,
} from '@nestjs/common';
import { CreateEmpleadoDto, UpdateEmpleadoDto } from './empleado.dto';
import { EmpleadoEntity } from './empleado.entity';
import { claveTexto } from './empleado.utils';
import { EmpleadosService } from './empleados.service';

const USUARIO_A = '11111111-1111-4111-8111-111111111111';
const USUARIO_B = '22222222-2222-4222-8222-222222222222';
const USUARIO_FANTASMA = '99999999-9999-4999-8999-999999999999';

/**
 * Repositorio en memoria: lo justo de TypeORM para que el servicio corra de
 * verdad (buscar por cédula, guardar, agrupar cargos) y las pruebas comprueben
 * lo que queda guardado y no cómo se llamó a un mock.
 */
class RepoEnMemoria {
  filas: EmpleadoEntity[] = [];
  usuarios = new Map<
    string,
    { id: string; name_user: string; name_surname: string }
  >();
  private secuencia = 0;

  /** Como la base: las columnas con DEFAULT quedan llenas al guardar. */
  create = (valor: Partial<EmpleadoEntity>) =>
    ({ is_deleted: false, status: 'ACTIVE', ...valor }) as EmpleadoEntity;

  save = (fila: EmpleadoEntity) => {
    if (!fila.id) {
      this.secuencia += 1;
      fila.id = `emp-${this.secuencia}`;
    }
    if (!this.filas.includes(fila)) this.filas.push(fila);
    return Promise.resolve(fila);
  };

  find = ({ where }: { where: Partial<EmpleadoEntity> }) =>
    Promise.resolve(this.filas.filter((f) => this.coincide(f, where)));

  findOne = ({ where }: { where: Partial<EmpleadoEntity> }) =>
    Promise.resolve(this.filas.find((f) => this.coincide(f, where)) ?? null);

  /** Solo se usa la consulta de usuarios de kpi_security. */
  query = (_sql: string, [ids]: [string[]]) =>
    Promise.resolve(ids.map((id) => this.usuarios.get(id)).filter(Boolean));

  /** Solo se usa el agrupado de cargos. */
  createQueryBuilder = () => {
    const constructor = {
      select: () => constructor,
      addSelect: () => constructor,
      where: () => constructor,
      groupBy: () => constructor,
      getRawMany: () => {
        const conteo = new Map<string, number>();
        for (const f of this.filas.filter((x) => !x.is_deleted)) {
          conteo.set(f.cargo, (conteo.get(f.cargo) ?? 0) + 1);
        }
        return Promise.resolve(
          [...conteo].map(([cargo, total]) => ({
            cargo,
            total: String(total),
          })),
        );
      },
    };
    return constructor;
  };

  private coincide(fila: EmpleadoEntity, where: Partial<EmpleadoEntity>) {
    const datos = fila as unknown as Record<string, unknown>;
    return Object.entries(where).every(
      ([clave, valor]) => datos[clave] === valor,
    );
  }
}

function crearServicio() {
  const repo = new RepoEnMemoria();
  repo.usuarios.set(USUARIO_A, {
    id: USUARIO_A,
    name_user: 'usuario.a',
    name_surname: 'A APELLIDO',
  });
  repo.usuarios.set(USUARIO_B, {
    id: USUARIO_B,
    name_user: 'usuario.b',
    name_surname: 'B APELLIDO',
  });
  const service = new EmpleadosService(
    repo as unknown as Repository<EmpleadoEntity>,
  );
  return { repo, service };
}

const base = (extra: Partial<CreateEmpleadoDto> = {}): CreateEmpleadoDto => ({
  nombres_apellidos: 'ABRIL BILVAO VICTOR ALFONSO',
  cedula: '2200243042',
  sueldo: 700,
  cargo: 'TRABAJADOR EN GENERAL',
  ...extra,
});

describe('EmpleadosService', () => {
  describe('crear', () => {
    it('calcula el valor por hora con el sueldo y guarda quién lo creó', async () => {
      const { service, repo } = crearServicio();
      const creado = await service.crear(base(), 'dsanchez');

      expect(creado.valor_hora).toBe(2.9167);
      expect(creado.valor_hora_manual).toBe(false);
      expect(creado.status).toBe('ACTIVE');
      expect(creado.usuario).toBeNull();
      expect(repo.filas[0].created_by).toBe('dsanchez');
    });

    it('acepta un valor por hora distinto del calculado y lo marca como manual', async () => {
      const { service } = crearServicio();
      const creado = await service.crear(base({ valor_hora: 3.1 }), null);
      expect(creado.valor_hora).toBe(3.1);
      expect(creado.valor_hora_manual).toBe(true);
    });

    it('un valor por hora igual al calculado no cuenta como manual', async () => {
      const { service } = crearServicio();
      const creado = await service.crear(base({ valor_hora: 2.9167 }), null);
      expect(creado.valor_hora_manual).toBe(false);
    });

    it('normaliza la cédula, el nombre y reutiliza el cargo ya registrado', async () => {
      const { service } = crearServicio();
      await service.crear(base(), null);
      const segundo = await service.crear(
        base({
          nombres_apellidos: '  AGUIRRE   ZAMORA JEAN PIERRE ',
          cedula: 802531988 as unknown as string,
          cargo: 'trabajador en  general ',
        }),
        null,
      );
      expect(segundo.cedula).toBe('0802531988');
      expect(segundo.nombres_apellidos).toBe('AGUIRRE ZAMORA JEAN PIERRE');
      expect(segundo.cargo).toBe('TRABAJADOR EN GENERAL');
    });

    it('rechaza una cédula que no tiene diez dígitos', async () => {
      const { service } = crearServicio();
      await expect(
        service.crear(base({ cedula: '12345' }), null),
      ).rejects.toThrow(BadRequestException);
    });

    it('rechaza una cédula ya registrada y dice de quién es', async () => {
      const { service } = crearServicio();
      await service.crear(base(), null);
      await expect(
        service.crear(base({ nombres_apellidos: 'OTRA PERSONA' }), null),
      ).rejects.toThrow(/ABRIL BILVAO VICTOR ALFONSO/);
    });

    it('la cédula de un empleado eliminado queda libre', async () => {
      const { service } = crearServicio();
      const primero = await service.crear(base(), null);
      await service.eliminar(primero.id, 'dsanchez');
      await expect(service.crear(base(), null)).resolves.toBeDefined();
    });

    it('vincula el usuario del sistema y devuelve sus datos', async () => {
      const { service } = crearServicio();
      const creado = await service.crear(base({ user_id: USUARIO_A }), null);
      expect(creado.user_id).toBe(USUARIO_A);
      expect(creado.usuario).toEqual({
        id: USUARIO_A,
        name_user: 'usuario.a',
        name_surname: 'A APELLIDO',
      });
    });

    it('rechaza un usuario que no existe', async () => {
      const { service } = crearServicio();
      await expect(
        service.crear(base({ user_id: USUARIO_FANTASMA }), null),
      ).rejects.toThrow(BadRequestException);
    });

    it('rechaza un usuario que ya es de otro empleado', async () => {
      const { service } = crearServicio();
      await service.crear(base({ user_id: USUARIO_A }), null);
      await expect(
        service.crear(base({ cedula: '0802531988', user_id: USUARIO_A }), null),
      ).rejects.toThrow(ConflictException);
    });
  });

  describe('actualizar', () => {
    it('un valor por hora calculado sigue al sueldo', async () => {
      const { service } = crearServicio();
      const creado = await service.crear(base({ sueldo: 1200 }), null);
      const cambiado = await service.actualizar(
        creado.id,
        { sueldo: 2400 },
        null,
      );
      expect(cambiado.valor_hora).toBe(10);
      expect(cambiado.valor_hora_manual).toBe(false);
    });

    it('un valor por hora fijado a mano se respeta cuando cambia el sueldo', async () => {
      const { service } = crearServicio();
      const creado = await service.crear(
        base({ sueldo: 1200, valor_hora: 4.5 }),
        null,
      );
      const cambiado = await service.actualizar(
        creado.id,
        { sueldo: 2400 },
        null,
      );
      expect(cambiado.valor_hora).toBe(4.5);
      expect(cambiado.valor_hora_manual).toBe(true);
    });

    it('null devuelve el valor por hora al cálculo', async () => {
      const { service } = crearServicio();
      const creado = await service.crear(base({ valor_hora: 4.5 }), null);
      const vuelto = await service.actualizar(
        creado.id,
        { valor_hora: null },
        null,
      );
      expect(vuelto.valor_hora).toBe(2.9167);
      expect(vuelto.valor_hora_manual).toBe(false);
    });

    it('un valor nuevo fijado a mano reemplaza al anterior', async () => {
      const { service } = crearServicio();
      const creado = await service.crear(base(), null);
      const fijado = await service.actualizar(
        creado.id,
        { valor_hora: 1.3 },
        null,
      );
      expect(fijado.valor_hora).toBe(1.3);
      expect(fijado.valor_hora_manual).toBe(true);
    });

    it('un valor manual que llega a coincidir con el calculado deja de ser manual', async () => {
      const { service } = crearServicio();
      const creado = await service.crear(
        base({ sueldo: 700, valor_hora: 3 }),
        null,
      );
      const igualado = await service.actualizar(
        creado.id,
        { sueldo: 720 },
        null,
      );
      // 720 / 240 = 3: el valor fijado ya es el calculado.
      expect(igualado.valor_hora).toBe(3);
      expect(igualado.valor_hora_manual).toBe(false);
    });

    it('permite cambiar la cédula solo si no es de otro empleado', async () => {
      const { service } = crearServicio();
      const a = await service.crear(base(), null);
      await service.crear(
        base({ nombres_apellidos: 'OTRO', cedula: '0802531988' }),
        null,
      );
      await expect(
        service.actualizar(a.id, { cedula: '0802531988' }, null),
      ).rejects.toThrow(ConflictException);
      await expect(
        service.actualizar(a.id, { cedula: '2200243042' }, null),
      ).resolves.toBeDefined();
    });

    it('cambia y quita el usuario vinculado', async () => {
      const { service } = crearServicio();
      const creado = await service.crear(base({ user_id: USUARIO_A }), null);
      const cambiado = await service.actualizar(
        creado.id,
        { user_id: USUARIO_B },
        null,
      );
      expect(cambiado.usuario?.name_user).toBe('usuario.b');
      const sinUsuario = await service.actualizar(
        creado.id,
        { user_id: null },
        null,
      );
      expect(sinUsuario.user_id).toBeNull();
      expect(sinUsuario.usuario).toBeNull();
    });

    it('un empleado que no existe da 404', async () => {
      const { service } = crearServicio();
      await expect(
        service.actualizar('no-existe', { cargo: 'X' }, null),
      ).rejects.toThrow(NotFoundException);
    });
  });

  describe('cargos', () => {
    it('lista cada cargo una sola vez aunque esté escrito de varias formas', async () => {
      const { service, repo } = crearServicio();
      const cargos = [
        'TRABAJADOR EN GENERAL',
        'SUPERVISOR DE SSA',
        'Supervisor de SSA ',
        'Técnico Mecánico',
        'TECNICO MECANICO',
        'BODEGUERO',
      ];
      cargos.forEach((cargo, i) =>
        repo.filas.push({
          id: `x${i}`,
          cargo,
          is_deleted: false,
        } as EmpleadoEntity),
      );
      const lista = await service.listarCargos();
      expect(lista).toHaveLength(4);
      expect(lista.map((c) => claveTexto(c))).toEqual([
        'BODEGUERO',
        'SUPERVISOR DE SSA',
        'TECNICO MECANICO',
        'TRABAJADOR EN GENERAL',
      ]);
    });

    it('filtra por lo escrito sin importar tildes ni mayúsculas', async () => {
      const { service, repo } = crearServicio();
      ['Técnico Eléctrico', 'TECNICO MECANICO', 'BODEGUERO'].forEach(
        (cargo, i) =>
          repo.filas.push({
            id: `y${i}`,
            cargo,
            is_deleted: false,
          } as EmpleadoEntity),
      );
      expect(await service.listarCargos('tecnico')).toHaveLength(2);
      expect(await service.listarCargos('bod')).toEqual(['BODEGUERO']);
      expect(await service.listarCargos('zzz')).toEqual([]);
    });

    it('un empleado eliminado ya no aporta su cargo', async () => {
      const { service } = crearServicio();
      const creado = await service.crear(base({ cargo: 'CARGO ÚNICO' }), null);
      await service.eliminar(creado.id, null);
      expect(await service.listarCargos()).toEqual([]);
    });
  });

  describe('listarParaSeleccion', () => {
    it('trae a los activos ordenados por nombre y sin datos de sueldo', async () => {
      const { service, repo } = crearServicio();
      await service.crear(
        base({ nombres_apellidos: 'ZAMBRANO LUIS', cedula: '0802531988' }),
        null,
      );
      await service.crear(
        base({
          nombres_apellidos: 'ÁLVAREZ ANA',
          cedula: '1716621311',
          user_id: USUARIO_A,
        }),
        null,
      );
      await service.crear(
        base({ nombres_apellidos: 'MORA PEDRO', cedula: '1205870940' }),
        null,
      );
      const inactivo = repo.filas.find((f) => f.cedula === '1205870940');
      if (inactivo) inactivo.status = 'INACTIVE';

      const lista = await service.listarParaSeleccion();

      expect(lista.map((e) => e.nombres_apellidos)).toEqual([
        'ÁLVAREZ ANA',
        'ZAMBRANO LUIS',
      ]);
      expect(lista[0].user_id).toBe(USUARIO_A);
      expect(lista[1].user_id).toBeNull();
      for (const empleado of lista) {
        expect(Object.keys(empleado).sort()).toEqual([
          'cargo',
          'id',
          'nombres_apellidos',
          'user_id',
        ]);
      }
    });

    it('un empleado eliminado no se puede elegir', async () => {
      const { service } = crearServicio();
      const creado = await service.crear(base(), null);
      await service.eliminar(creado.id, null);
      expect(await service.listarParaSeleccion()).toEqual([]);
    });
  });

  describe('importar', () => {
    const fila = (n: number, extra: Record<string, unknown> = {}) => ({
      fila: n,
      nombres_apellidos: `PERSONA ${n}`,
      cedula: `08025319${String(80 + n)}`,
      sueldo: 1200,
      cargo: 'OPERADOR DE CENTRAL',
      ...extra,
    });

    it('crea a los nuevos con el valor por hora calculado', async () => {
      const { service, repo } = crearServicio();
      const resultado = await service.importar(
        [fila(1, { sueldo: 700 }), fila(2)],
        'dsanchez',
      );
      expect(resultado).toMatchObject({
        total: 2,
        creados: 2,
        actualizados: 0,
        sin_cambios: 0,
        omitidos: 0,
        errores: [],
      });
      expect(repo.filas.map((f) => f.valor_hora)).toEqual([2.9167, 5]);
      expect(repo.filas.every((f) => f.created_by === 'dsanchez')).toBe(true);
    });

    it('actualiza por cédula, y no toca lo que no cambió', async () => {
      const { service, repo } = crearServicio();
      await service.importar([fila(1), fila(2)], null);
      const resultado = await service.importar(
        [fila(1, { sueldo: 2400 }), fila(2)],
        'dsanchez',
      );
      expect(resultado).toMatchObject({
        creados: 0,
        actualizados: 1,
        sin_cambios: 1,
      });
      expect(repo.filas[0].sueldo).toBe(2400);
      expect(repo.filas[0].valor_hora).toBe(10);
      expect(repo.filas[0].updated_by).toBe('dsanchez');
      expect(repo.filas).toHaveLength(2);
    });

    it('respeta el valor por hora fijado a mano', async () => {
      const { service, repo } = crearServicio();
      await service.importar([fila(1)], null);
      await service.actualizar(repo.filas[0].id, { valor_hora: 9.99 }, null);
      const resultado = await service.importar(
        [fila(1, { sueldo: 2400 })],
        null,
      );
      expect(resultado.actualizados).toBe(1);
      expect(repo.filas[0].sueldo).toBe(2400);
      expect(repo.filas[0].valor_hora).toBe(9.99);
      expect(repo.filas[0].valor_hora_manual).toBe(true);
    });

    it('unifica un mismo cargo escrito de varias formas', async () => {
      const { service, repo } = crearServicio();
      await service.importar(
        [
          fila(1, { cargo: 'TECNICO MECANICO ' }),
          fila(2, { cargo: 'Técnico Mecánico' }),
          fila(3, { cargo: 'técnico  mecanico' }),
        ],
        null,
      );
      expect(new Set(repo.filas.map((f) => f.cargo)).size).toBe(1);
      expect(repo.filas[0].cargo).toBe('TECNICO MECANICO');
    });

    it('reutiliza los cargos que ya estaban registrados', async () => {
      const { service, repo } = crearServicio();
      await service.crear(base({ cargo: 'JEFE DE CAMPO' }), null);
      await service.importar([fila(1, { cargo: 'jefe de campo' })], null);
      expect(repo.filas.map((f) => f.cargo)).toEqual([
        'JEFE DE CAMPO',
        'JEFE DE CAMPO',
      ]);
    });

    it('lee la cédula que Excel dejó como número sin el cero inicial', async () => {
      const { service, repo } = crearServicio();
      await service.importar([fila(1, { cedula: 604621326 })], null);
      expect(repo.filas[0].cedula).toBe('0604621326');
    });

    it('omite las filas malas con su motivo y guarda las buenas', async () => {
      const { service, repo } = crearServicio();
      const resultado = await service.importar(
        [
          fila(2),
          fila(3, { nombres_apellidos: '  ' }),
          fila(4, { cedula: '123' }),
          fila(5, { sueldo: 'mucho' }),
          fila(6, { sueldo: 0 }),
          fila(7, { cargo: '' }),
          fila(8, { cedula: fila(2).cedula }),
        ],
        null,
      );
      expect(resultado.creados).toBe(1);
      expect(resultado.omitidos).toBe(6);
      expect(repo.filas).toHaveLength(1);
      const motivos = Object.fromEntries(
        resultado.errores.map((e) => [e.fila, e.mensaje]),
      );
      expect(motivos[3]).toMatch(/nombre/i);
      expect(motivos[4]).toMatch(/10 dígitos/);
      expect(motivos[5]).toMatch(/sueldo/i);
      expect(motivos[6]).toMatch(/sueldo/i);
      expect(motivos[7]).toMatch(/cargo/i);
      expect(motivos[8]).toMatch(/repetida en el archivo \(fila 2\)/);
    });

    it('vincula el usuario cuando el empleado no tenía uno', async () => {
      const { service, repo } = crearServicio();
      await service.importar([fila(1)], null);
      const resultado = await service.importar(
        [fila(1, { user_id: USUARIO_A })],
        null,
      );
      expect(resultado.actualizados).toBe(1);
      expect(repo.filas[0].user_id).toBe(USUARIO_A);
    });

    it('no deshace el vínculo con un usuario que ya tenía', async () => {
      const { service, repo } = crearServicio();
      await service.importar([fila(1, { user_id: USUARIO_A })], null);
      const resultado = await service.importar(
        [fila(1, { user_id: USUARIO_B })],
        null,
      );
      expect(resultado.sin_cambios).toBe(1);
      expect(repo.filas[0].user_id).toBe(USUARIO_A);
    });

    it('rechaza un usuario que no existe o que es de otro empleado', async () => {
      const { service, repo } = crearServicio();
      await service.importar([fila(1, { user_id: USUARIO_A })], null);
      const resultado = await service.importar(
        [
          fila(2, { user_id: USUARIO_A }),
          fila(3, { user_id: USUARIO_FANTASMA }),
          fila(4, { user_id: 'no-es-un-uuid' }),
        ],
        null,
      );
      expect(resultado.creados).toBe(0);
      expect(resultado.omitidos).toBe(3);
      expect(resultado.errores[0].mensaje).toMatch(/PERSONA 1/);
      expect(resultado.errores[1].mensaje).toMatch(/no existe/);
      expect(resultado.errores[2].mensaje).toMatch(/no existe/);
      expect(repo.filas).toHaveLength(1);
    });

    it('usa la posición de la fila si el archivo no la trae', async () => {
      const { service } = crearServicio();
      const resultado = await service.importar(
        [{ nombres_apellidos: '', cedula: '', sueldo: 1, cargo: 'X' }],
        null,
      );
      expect(resultado.errores[0].fila).toBe(1);
    });

    it('un error inesperado al guardar una fila no detiene a las demás', async () => {
      const { service, repo } = crearServicio();
      const guardar = repo.save;
      let llamadas = 0;
      repo.save = async (f: EmpleadoEntity) => {
        llamadas += 1;
        if (llamadas === 1) throw new Error('falló la base');
        return guardar(f);
      };
      const resultado = await service.importar([fila(1), fila(2)], null);
      expect(resultado.creados).toBe(1);
      expect(resultado.errores).toEqual([
        expect.objectContaining({ fila: 1, mensaje: 'falló la base' }),
      ]);
    });
  });

  describe('eliminar', () => {
    it('da de baja al empleado sin borrar la fila', async () => {
      const { service, repo } = crearServicio();
      const creado = await service.crear(base(), null);
      await service.eliminar(creado.id, 'dsanchez');
      expect(repo.filas[0].is_deleted).toBe(true);
      expect(repo.filas[0].deleted_by).toBe('dsanchez');
      await expect(service.obtener(creado.id)).rejects.toThrow(
        NotFoundException,
      );
    });
  });
});

describe('DTO de empleados', () => {
  it('acepta la cédula como número y el valor por hora nulo', async () => {
    const dto = plainToInstance(CreateEmpleadoDto, {
      nombres_apellidos: 'ABRIL BILVAO VICTOR ALFONSO',
      cedula: 2200243042,
      sueldo: '700',
      valor_hora: null,
      cargo: 'TRABAJADOR EN GENERAL',
    });
    expect(await validate(dto)).toHaveLength(0);
    expect(dto.cedula).toBe('2200243042');
    expect(dto.sueldo).toBe(700);
    expect(dto.valor_hora).toBeNull();
  });

  it('rechaza un sueldo en cero o con tres decimales', async () => {
    const malo = (sueldo: number) =>
      validate(
        plainToInstance(CreateEmpleadoDto, {
          nombres_apellidos: 'X',
          cedula: '2200243042',
          sueldo,
          cargo: 'X',
        }),
      );
    expect(await malo(0)).not.toHaveLength(0);
    expect(await malo(700.123)).not.toHaveLength(0);
    expect(await malo(700.12)).toHaveLength(0);
  });

  it('la actualización es parcial y distingue "no viene" de "null"', async () => {
    const sinValor = plainToInstance(UpdateEmpleadoDto, { cargo: 'X' });
    expect(await validate(sinValor)).toHaveLength(0);
    expect('valor_hora' in sinValor).toBe(false);
    const conNull = plainToInstance(UpdateEmpleadoDto, { valor_hora: null });
    expect(await validate(conNull)).toHaveLength(0);
    expect(conNull.valor_hora).toBeNull();
  });
});
