import {
  BadRequestException,
  ConflictException,
  Injectable,
  NotFoundException,
} from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Brackets, QueryFailedError, Repository } from 'typeorm';
import {
  CreateEmpleadoDto,
  EmpleadoQueryDto,
  ResultadoImportacion,
  UpdateEmpleadoDto,
} from './empleado.dto';
import { EmpleadoEntity } from './empleado.entity';
import {
  armarCatalogoCargos,
  calcularValorHora,
  CatalogoCargos,
  claveTexto,
  DECIMALES_SUELDO,
  DECIMALES_VALOR_HORA,
  esCedulaConFormato,
  leerNumero,
  aTexto,
  normalizarCedula,
  normalizarEspacios,
  redondear,
  resolverCargo,
} from './empleado.utils';

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

type UsuarioDelSistema = {
  id: string;
  name_user: string;
  name_surname: string;
};

@Injectable()
export class EmpleadosService {
  constructor(
    @InjectRepository(EmpleadoEntity)
    private readonly repo: Repository<EmpleadoEntity>,
  ) {}

  // ---------------------------------------------------------------- consulta

  async listar(query: EmpleadoQueryDto) {
    const page = Math.max(1, Number(query.page) || 1);
    const limit = Math.min(100, Math.max(1, Number(query.limit) || 10));
    const busqueda = claveTexto(query.search).toLowerCase();

    const qb = this.repo.createQueryBuilder('e').where('e.is_deleted = false');

    if (query.status)
      qb.andWhere('e.status = :status', { status: query.status });

    if (busqueda) {
      // La búsqueda ignora mayúsculas y tildes: "peña" encuentra "PEÑA".
      const sinTildes = (columna: string) =>
        `translate(lower(${columna}), 'áéíóúüñ', 'aeiouun')`;
      qb.andWhere(
        new Brackets((w) => {
          w.where(`${sinTildes('e.nombres_apellidos')} LIKE :busqueda`)
            .orWhere(`${sinTildes('e.cargo')} LIKE :busqueda`)
            .orWhere('e.cedula LIKE :busqueda');
        }),
        { busqueda: `%${busqueda}%` },
      );
    }

    const [filas, total] = await qb
      .orderBy('e.nombres_apellidos', 'ASC')
      .skip((page - 1) * limit)
      .take(limit)
      .getManyAndCount();

    return {
      data: await this.conUsuario(filas),
      pagination: {
        page,
        limit,
        total,
        totalPages: Math.max(1, Math.ceil(total / limit)),
      },
    };
  }

  async obtener(id: string) {
    const fila = await this.buscarVivo(id);
    const [conUsuario] = await this.conUsuario([fila]);
    return conUsuario;
  }

  /**
   * Cargos ya registrados, sin repetir: dos escrituras que solo se distinguen
   * por mayúsculas, tildes o espacios cuentan como el mismo cargo.
   */
  async listarCargos(search?: string): Promise<string[]> {
    const catalogo = await this.cargarCatalogoCargos();
    const filtro = claveTexto(search);
    return [...catalogo.entries()]
      .filter(([clave]) => !filtro || clave.includes(filtro))
      .map(([, cargo]) => cargo)
      .sort((a, b) => a.localeCompare(b, 'es', { sensitivity: 'base' }));
  }

  /**
   * Empleados que se pueden elegir como responsables de una tarea o de una
   * plantilla: los activos, sin sueldo ni valor por hora. Cualquier rol que
   * arma una OT los pide, y esos importes no le corresponden.
   */
  async listarParaSeleccion() {
    const filas = await this.repo.find({
      select: { id: true, user_id: true, nombres_apellidos: true, cargo: true },
      where: { is_deleted: false, status: 'ACTIVE' },
    });
    return filas
      .map((fila) => ({
        id: fila.id,
        user_id: fila.user_id ?? null,
        nombres_apellidos: fila.nombres_apellidos,
        cargo: fila.cargo,
      }))
      .sort((a, b) =>
        a.nombres_apellidos.localeCompare(b.nombres_apellidos, 'es', {
          sensitivity: 'base',
        }),
      );
  }

  // -------------------------------------------------------------- escritura

  async crear(dto: CreateEmpleadoDto, actor: string | null) {
    const nombres = this.leerNombres(dto.nombres_apellidos);
    const cedula = this.leerCedula(dto.cedula);
    const sueldo = this.leerSueldo(dto.sueldo);
    const cargo = await this.leerCargo(dto.cargo);
    const userId = dto.user_id ?? null;

    await this.exigirCedulaLibre(cedula);
    if (userId) await this.exigirUsuarioDisponible(userId);

    const valor = this.resolverValorHora(sueldo, dto.valor_hora);
    const fila = this.repo.create({
      user_id: userId,
      nombres_apellidos: nombres,
      cedula,
      sueldo,
      valor_hora: valor.valor,
      valor_hora_manual: valor.manual,
      cargo,
      status: dto.status ?? 'ACTIVE',
      created_by: actor,
      updated_by: actor,
    });
    const guardada = await this.guardar(fila);
    return this.obtener(guardada.id);
  }

  async actualizar(id: string, dto: UpdateEmpleadoDto, actor: string | null) {
    const actual = await this.buscarVivo(id);

    if (dto.nombres_apellidos !== undefined) {
      actual.nombres_apellidos = this.leerNombres(dto.nombres_apellidos);
    }
    if (dto.cedula !== undefined) {
      const cedula = this.leerCedula(dto.cedula);
      if (cedula !== actual.cedula) await this.exigirCedulaLibre(cedula, id);
      actual.cedula = cedula;
    }
    if (dto.sueldo !== undefined) actual.sueldo = this.leerSueldo(dto.sueldo);
    if (dto.cargo !== undefined) actual.cargo = await this.leerCargo(dto.cargo);
    if (dto.status !== undefined) actual.status = dto.status;
    if (dto.user_id !== undefined) {
      if (dto.user_id && dto.user_id !== actual.user_id) {
        await this.exigirUsuarioDisponible(dto.user_id, id);
      }
      actual.user_id = dto.user_id ?? null;
    }

    // Valor por hora: `null` pide volver al cálculo; un número lo fija a mano;
    // si no viene, un valor fijado a mano se respeta y uno calculado sigue al
    // sueldo.
    if (dto.valor_hora === undefined && actual.valor_hora_manual) {
      actual.valor_hora_manual =
        redondear(actual.valor_hora, DECIMALES_VALOR_HORA) !==
        calcularValorHora(actual.sueldo);
    } else {
      const valor = this.resolverValorHora(actual.sueldo, dto.valor_hora);
      actual.valor_hora = valor.valor;
      actual.valor_hora_manual = valor.manual;
    }

    actual.updated_by = actor;
    await this.guardar(actual);
    return this.obtener(id);
  }

  async eliminar(id: string, actor: string | null) {
    const actual = await this.buscarVivo(id);
    actual.is_deleted = true;
    actual.deleted_at = new Date();
    actual.deleted_by = actor;
    actual.updated_by = actor;
    await this.repo.save(actual);
    return { message: `Empleado ${actual.nombres_apellidos} eliminado.` };
  }

  // ------------------------------------------------------------ importación

  /**
   * Carga masiva desde el Excel. Cada fila se valida por su cuenta: las
   * buenas se guardan y las malas se devuelven con su motivo, así que un error
   * de tipeo no tira el archivo entero.
   *
   * La cédula es la llave: si ya hay un empleado con ella se actualiza, y si
   * no, se crea. El valor por hora se calcula con el sueldo, salvo que el
   * empleado tenga uno fijado a mano, que no se toca.
   */
  async importar(
    filas: Array<Record<string, unknown>>,
    actor: string | null,
  ): Promise<ResultadoImportacion> {
    const resultado: ResultadoImportacion = {
      total: filas.length,
      creados: 0,
      actualizados: 0,
      sin_cambios: 0,
      omitidos: 0,
      errores: [],
    };

    const catalogo = await this.cargarCatalogoCargos();
    const vivos = await this.repo.find({ where: { is_deleted: false } });
    const porCedula = new Map(vivos.map((e) => [e.cedula, e]));
    const usuariosOcupados = new Map(
      vivos.filter((e) => e.user_id).map((e) => [e.user_id as string, e]),
    );
    const usuariosExistentes = await this.buscarUsuarios(
      filas.map((f) => aTexto(f.user_id).trim()).filter((id) => UUID.test(id)),
    );
    const cedulasVistas = new Map<string, number>();

    for (let indice = 0; indice < filas.length; indice += 1) {
      const bruta = filas[indice] ?? {};
      const fila = Number(bruta.fila) > 0 ? Number(bruta.fila) : indice + 1;
      const nombres = normalizarEspacios(bruta.nombres_apellidos);
      const cedula = normalizarCedula(bruta.cedula);
      const fallo = (mensaje: string) => {
        resultado.errores.push({
          fila,
          cedula,
          nombres_apellidos: nombres,
          mensaje,
        });
      };

      try {
        if (!nombres) {
          fallo('Falta el nombre.');
          continue;
        }
        if (nombres.length > 200) {
          fallo('El nombre supera los 200 caracteres.');
          continue;
        }
        if (!esCedulaConFormato(cedula)) {
          fallo('La cédula debe tener 10 dígitos.');
          continue;
        }
        const anterior = cedulasVistas.get(cedula);
        if (anterior !== undefined) {
          fallo(`La cédula está repetida en el archivo (fila ${anterior}).`);
          continue;
        }
        cedulasVistas.set(cedula, fila);

        const sueldoLeido = leerNumero(bruta.sueldo);
        if (sueldoLeido === null || sueldoLeido <= 0 || sueldoLeido > 1000000) {
          fallo('El sueldo debe ser un número mayor que cero.');
          continue;
        }
        const sueldo = redondear(sueldoLeido, DECIMALES_SUELDO);

        const cargoTexto = normalizarEspacios(bruta.cargo);
        if (!cargoTexto) {
          fallo('Falta el cargo.');
          continue;
        }
        if (cargoTexto.length > 150) {
          fallo('El cargo supera los 150 caracteres.');
          continue;
        }
        const cargo = resolverCargo(cargoTexto, catalogo);

        const existente = porCedula.get(cedula);
        let userId: string | null = null;
        const userIdTexto = aTexto(bruta.user_id).trim();
        if (userIdTexto) {
          if (!UUID.test(userIdTexto) || !usuariosExistentes.has(userIdTexto)) {
            fallo('El usuario indicado no existe.');
            continue;
          }
          const ocupante = usuariosOcupados.get(userIdTexto);
          if (ocupante && ocupante.id !== existente?.id) {
            fallo(
              `El usuario ya está vinculado al empleado ${ocupante.nombres_apellidos}.`,
            );
            continue;
          }
          userId = userIdTexto;
        }

        if (!existente) {
          const creada = await this.guardar(
            this.repo.create({
              user_id: userId,
              nombres_apellidos: nombres,
              cedula,
              sueldo,
              valor_hora: calcularValorHora(sueldo),
              valor_hora_manual: false,
              cargo,
              status: 'ACTIVE',
              created_by: actor,
              updated_by: actor,
            }),
          );
          porCedula.set(cedula, creada);
          if (userId) usuariosOcupados.set(userId, creada);
          resultado.creados += 1;
          continue;
        }

        // Ya existe: se actualiza lo que cambió. El usuario solo se asigna si
        // todavía no tenía uno, para no deshacer un vínculo hecho a mano.
        const valorHora = existente.valor_hora_manual
          ? existente.valor_hora
          : calcularValorHora(sueldo);
        const nuevoUsuario = existente.user_id ?? userId;
        const cambio =
          existente.nombres_apellidos !== nombres ||
          existente.sueldo !== sueldo ||
          existente.cargo !== cargo ||
          existente.valor_hora !== valorHora ||
          (existente.user_id ?? null) !== nuevoUsuario;
        if (!cambio) {
          resultado.sin_cambios += 1;
          continue;
        }
        existente.nombres_apellidos = nombres;
        existente.sueldo = sueldo;
        existente.cargo = cargo;
        existente.valor_hora = valorHora;
        existente.user_id = nuevoUsuario;
        existente.updated_by = actor;
        await this.guardar(existente);
        if (nuevoUsuario) usuariosOcupados.set(nuevoUsuario, existente);
        resultado.actualizados += 1;
      } catch (error) {
        fallo(
          error instanceof Error && error.message
            ? error.message
            : 'No se pudo guardar la fila.',
        );
      }
    }

    resultado.omitidos = resultado.errores.length;
    return resultado;
  }

  // --------------------------------------------------------------- utilidades

  private async buscarVivo(id: string) {
    const fila = await this.repo.findOne({
      where: { id, is_deleted: false },
    });
    if (!fila) throw new NotFoundException(`Empleado ${id} no encontrado.`);
    return fila;
  }

  private leerNombres(valor: unknown) {
    const nombres = normalizarEspacios(valor);
    if (!nombres) throw new BadRequestException('Falta el nombre.');
    if (nombres.length > 200) {
      throw new BadRequestException('El nombre supera los 200 caracteres.');
    }
    return nombres;
  }

  private leerCedula(valor: unknown) {
    const cedula = normalizarCedula(valor);
    if (!esCedulaConFormato(cedula)) {
      throw new BadRequestException('La cédula debe tener 10 dígitos.');
    }
    return cedula;
  }

  private leerSueldo(valor: unknown) {
    const sueldo = leerNumero(valor);
    if (sueldo === null || sueldo <= 0 || sueldo > 1000000) {
      throw new BadRequestException(
        'El sueldo debe ser un número mayor que cero.',
      );
    }
    return redondear(sueldo, DECIMALES_SUELDO);
  }

  /** Reutiliza la escritura de un cargo ya registrado si es el mismo. */
  private async leerCargo(valor: unknown) {
    const catalogo = await this.cargarCatalogoCargos();
    const cargo = resolverCargo(valor, catalogo);
    if (!cargo) throw new BadRequestException('Falta el cargo.');
    if (cargo.length > 150) {
      throw new BadRequestException('El cargo supera los 150 caracteres.');
    }
    return cargo;
  }

  /**
   * Valor por hora de un empleado. `fijado` es lo que pidió el usuario: un
   * número lo fija a mano (salvo que coincida con el cálculo, en cuyo caso
   * sigue siendo el calculado) y `null` o ausente pide el cálculo.
   */
  private resolverValorHora(sueldo: number, fijado?: number | null) {
    const calculado = calcularValorHora(sueldo);
    if (fijado === undefined || fijado === null) {
      return { valor: calculado, manual: false };
    }
    const valor = redondear(Number(fijado), DECIMALES_VALOR_HORA);
    return { valor, manual: valor !== calculado };
  }

  private async cargarCatalogoCargos(): Promise<CatalogoCargos> {
    const filas: Array<{ cargo: string; total: string }> = await this.repo
      .createQueryBuilder('e')
      .select('e.cargo', 'cargo')
      .addSelect('COUNT(*)', 'total')
      .where('e.is_deleted = false')
      .groupBy('e.cargo')
      .getRawMany();
    return armarCatalogoCargos(
      filas.map((f) => ({ cargo: f.cargo, total: Number(f.total) })),
    );
  }

  private async exigirCedulaLibre(cedula: string, excepto?: string) {
    const otro = await this.repo.findOne({
      where: { cedula, is_deleted: false },
    });
    if (otro && otro.id !== excepto) {
      throw new ConflictException(
        `Ya existe un empleado con la cédula ${cedula}: ${otro.nombres_apellidos}.`,
      );
    }
  }

  private async exigirUsuarioDisponible(userId: string, excepto?: string) {
    const existentes = await this.buscarUsuarios([userId]);
    if (!existentes.has(userId)) {
      throw new BadRequestException('El usuario indicado no existe.');
    }
    const ocupante = await this.repo.findOne({
      where: { user_id: userId, is_deleted: false },
    });
    if (ocupante && ocupante.id !== excepto) {
      throw new ConflictException(
        `Ese usuario ya está vinculado al empleado ${ocupante.nombres_apellidos}.`,
      );
    }
  }

  /** Usuarios vivos del sistema entre los ids dados. */
  private async buscarUsuarios(
    ids: string[],
  ): Promise<Map<string, UsuarioDelSistema>> {
    const unicos = [...new Set(ids.filter(Boolean))];
    if (!unicos.length) return new Map();
    const filas: UsuarioDelSistema[] = await this.repo.query(
      `SELECT id, name_user, name_surname
         FROM kpi_security.tb_user
        WHERE id = ANY($1::uuid[]) AND COALESCE(is_deleted, false) = false`,
      [unicos],
    );
    return new Map(filas.map((u) => [u.id, u]));
  }

  /** Agrega a cada empleado los datos de su usuario, si lo tiene. */
  private async conUsuario(filas: EmpleadoEntity[]) {
    const usuarios = await this.buscarUsuarios(
      filas.map((f) => f.user_id).filter((id): id is string => !!id),
    );
    return filas.map((fila) => ({
      ...fila,
      usuario: fila.user_id ? (usuarios.get(fila.user_id) ?? null) : null,
    }));
  }

  /** Traduce los errores de unicidad de la base a un mensaje entendible. */
  private async guardar(fila: EmpleadoEntity) {
    try {
      return await this.repo.save(fila);
    } catch (error) {
      if (error instanceof QueryFailedError) {
        const detalle = error.driverError as {
          code?: string;
          constraint?: string;
        };
        if (detalle?.code === '23505') {
          if (detalle.constraint === 'uq_empleado_user') {
            throw new ConflictException(
              'Ese usuario ya está vinculado a otro empleado.',
            );
          }
          throw new ConflictException(
            `Ya existe un empleado con la cédula ${fila.cedula}.`,
          );
        }
        if (detalle?.code === '23503') {
          throw new BadRequestException('El usuario indicado no existe.');
        }
      }
      throw error;
    }
  }
}
