/**
 * Responsables de una tarea de OT y de una plantilla: quién trabajó, cuántas
 * horas y a cuánto la hora.
 *
 * Desde el 2026-09-30 se eligen de Empleados y no de Usuarios: una persona
 * puede trabajar en una OT sin tener usuario en el sistema. Lo que sigue guardado
 * por usuario (las OT y plantillas anteriores) se sigue leyendo: si el usuario
 * está vinculado a un empleado, la persona es ese empleado.
 *
 * Todo lo que no toca la base de datos vive aquí, para poder probarlo sin armar
 * el servicio de mantenimiento entero.
 */

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export type EmployeeDirectoryItem = {
  id: string;
  user_id: string | null;
  nombres_apellidos: string;
  valor_hora: number;
  status: string;
  is_deleted: boolean;
};

export type EmployeeDirectory = {
  byId: Map<string, EmployeeDirectoryItem>;
  /**
   * Solo empleados vivos: dar de baja a un empleado libera su usuario, que
   * puede pasar a ser de otro.
   */
  byUserId: Map<string, EmployeeDirectoryItem>;
};

export type TaskResponsible = {
  /** Nulo solo en lo guardado por usuario cuando ese usuario no es un empleado. */
  empleado_id: string | null;
  user_id: string | null;
  username: string | null;
  display_name: string;
  horas: number;
  /**
   * Costo de la hora de esta persona, congelado cuando se le asignó la tarea:
   * un cambio posterior de su valor por hora no toca lo ya registrado. Es lo que
   * llega a los informes; el nombre lleva "costo" para que el filtro de importes
   * lo quite a los roles que no ven costos.
   */
  costo_hora: number | null;
};

export type UserLabel = {
  username: string | null;
  displayName: string | null;
};

export type UserLabelResolver = (userId: string) => UserLabel | null;

const TEXTO_HORAS_INVALIDAS =
  'Las horas registradas por responsable deben ser numéricas y mayores o iguales a cero.';

function text(value: unknown): string | null {
  if (typeof value !== 'string' && typeof value !== 'number') return null;
  const trimmed = String(value).trim();
  return trimmed || null;
}

function round4(value: number) {
  return Number(value.toFixed(4));
}

/** Horas leídas de lo guardado: un dato sucio cuenta como cero, no rompe el informe. */
function readHours(value: unknown) {
  const hours = Number(value ?? 0);
  return Number.isFinite(hours) && hours >= 0 ? round4(hours) : 0;
}

function readCost(value: unknown): number | null {
  if (value === null || value === undefined || value === '') return null;
  const cost = Number(value);
  return Number.isFinite(cost) && cost >= 0 ? round4(cost) : null;
}

/** La primera etiqueta que sea un nombre: un id no se imprime nunca. */
function pickLabel(...values: unknown[]) {
  for (const value of values) {
    const label = text(value);
    if (label && !UUID.test(label)) return label;
  }
  return null;
}

export function buildEmployeeDirectory(
  rows: Array<Record<string, unknown>>,
): EmployeeDirectory {
  const byId = new Map<string, EmployeeDirectoryItem>();
  const byUserId = new Map<string, EmployeeDirectoryItem>();
  for (const row of rows ?? []) {
    const id = text(row.id);
    if (!id) continue;
    const item: EmployeeDirectoryItem = {
      id,
      user_id: text(row.user_id),
      nombres_apellidos: text(row.nombres_apellidos) ?? 'Empleado',
      valor_hora: readCost(row.valor_hora) ?? 0,
      status: text(row.status) ?? 'ACTIVE',
      is_deleted: row.is_deleted === true || row.is_deleted === 'true',
    };
    byId.set(id, item);
    if (item.user_id && !item.is_deleted) byUserId.set(item.user_id, item);
  }
  return { byId, byUserId };
}

function resolveEmployee(
  directory: EmployeeDirectory | undefined,
  empleadoId: string | null,
  userId: string | null,
) {
  if (!directory) return null;
  if (empleadoId) return directory.byId.get(empleadoId) ?? null;
  // Un id que no es de ningun usuario pero si de un empleado es un empleado que
  // llego por el campo del usuario: pasa con una pestaña abierta antes del
  // cambio, que arma los responsables de una plantilla (que ya trae ids de
  // empleado) como si fueran usuarios.
  if (userId) {
    return directory.byUserId.get(userId) ?? directory.byId.get(userId) ?? null;
  }
  return null;
}

/** Clave de una persona: el empleado si lo hay y, si no, el usuario. */
export function responsibleKey(
  entry: Pick<TaskResponsible, 'empleado_id' | 'user_id'>,
) {
  if (entry.empleado_id) return `E:${entry.empleado_id}`;
  return entry.user_id ? `U:${entry.user_id}` : null;
}

type Accumulator = {
  entry: TaskResponsible;
  costHours: number;
  costAmount: number;
  firstCost: number | null;
};

/**
 * Lee los responsables guardados en una tarea (JSONB) con la forma actual, sea
 * cual sea la que traigan.
 *
 * - Lo guardado por empleado usa su nombre de hoy: si el empleado corrige su
 *   nombre, se corrige en todas partes.
 * - Lo guardado por usuario se resuelve por el vínculo empleado-usuario, así que
 *   una OT anterior aparece con su empleado en cuanto el vínculo existe.
 * - Una misma persona repetida suma sus horas. El costo de la hora es el
 *   guardado; si no se guardó (lo anterior a este cambio) se toma el valor por
 *   hora que tiene el empleado.
 */
export function readResponsables(
  values: unknown,
  context: {
    directory?: EmployeeDirectory;
    userLabel?: UserLabelResolver;
  } = {},
): TaskResponsible[] {
  if (!Array.isArray(values)) return [];
  const grouped = new Map<string, Accumulator>();

  for (const raw of values) {
    const item = (raw && typeof raw === 'object' ? raw : {}) as Record<
      string,
      unknown
    >;
    const storedEmpleadoId = text(item.empleado_id);
    const storedUserId = text(item.user_id);
    const employee = resolveEmployee(
      context.directory,
      storedEmpleadoId,
      storedUserId,
    );
    const empleadoId = employee?.id ?? storedEmpleadoId;
    const userId = employee ? employee.user_id : storedUserId;
    const key = responsibleKey({ empleado_id: empleadoId, user_id: userId });
    if (!key) continue;

    const userLabel = userId ? (context.userLabel?.(userId) ?? null) : null;
    const hours = readHours(item.horas);
    const cost =
      readCost(item.costo_hora) ?? (employee ? employee.valor_hora : null);
    const current = grouped.get(key);
    if (!current) {
      grouped.set(key, {
        entry: {
          empleado_id: empleadoId,
          user_id: userId,
          username: pickLabel(userLabel?.username, item.username),
          display_name:
            employee?.nombres_apellidos ??
            pickLabel(
              userLabel?.displayName,
              userLabel?.username,
              item.display_name,
              item.username,
            ) ??
            (empleadoId ? 'Empleado' : 'Usuario asignado'),
          horas: hours,
          costo_hora: cost,
        },
        costHours: cost !== null ? hours : 0,
        costAmount: cost !== null ? hours * cost : 0,
        firstCost: cost,
      });
      continue;
    }
    current.entry.horas = round4(current.entry.horas + hours);
    if (cost !== null) {
      current.costHours += hours;
      current.costAmount += hours * cost;
      current.firstCost ??= cost;
    }
  }

  return [...grouped.values()].map(
    ({ entry, costHours, costAmount, firstCost }) => ({
      ...entry,
      // Promedio ponderado por horas si la persona quedó repetida con costos
      // distintos; con un solo costo es ese costo.
      costo_hora: costHours > 0 ? round4(costAmount / costHours) : firstCost,
    }),
  );
}

export type PrepareResult = {
  entries: TaskResponsible[];
  problems: string[];
};

/**
 * Responsables que llegan a guardarse, listos para el JSONB.
 *
 * Los nuevos salen de Empleados: activos y vivos. El costo de la hora lo pone el
 * servidor —el usuario no lo teclea— con el valor por hora del empleado en ese
 * momento, y una vez guardado no se vuelve a tocar mientras la persona siga en
 * la tarea. Quien ya estaba (por empleado o por usuario, aunque hoy esté de
 * baja) se conserva tal cual: cerrar una OT antigua no debe fallar porque
 * alguien dejó la empresa.
 *
 * Un usuario que llega solo (una pestaña vieja) se acepta únicamente si tiene
 * empleado vinculado —y se guarda como ese empleado— o si ya estaba en la tarea.
 */
export function prepareResponsablesForSave(
  incoming: unknown,
  stored: unknown,
  directory: EmployeeDirectory,
  context: { userLabel?: UserLabelResolver } = {},
): PrepareResult {
  const problems: string[] = [];
  const raw: Array<Record<string, unknown>> = [];
  const alreadyThere = new Map(
    readResponsables(stored, { directory, userLabel: context.userLabel }).map(
      (entry) => [responsibleKey(entry) as string, entry] as const,
    ),
  );
  const report = (message: string) => {
    if (!problems.includes(message)) problems.push(message);
  };

  for (const value of Array.isArray(incoming) ? incoming : []) {
    const item = (value && typeof value === 'object' ? value : {}) as Record<
      string,
      unknown
    >;
    const empleadoId = text(item.empleado_id);
    const userId = text(item.user_id);
    const hours = Number(item.horas ?? 0);
    if (!Number.isFinite(hours) || hours < 0) {
      report(TEXTO_HORAS_INVALIDAS);
      continue;
    }
    if (!empleadoId && !userId) {
      report('Cada responsable de la tarea debe ser un empleado.');
      continue;
    }

    const employee = resolveEmployee(directory, empleadoId, userId);
    if (employee) {
      const key = responsibleKey({
        empleado_id: employee.id,
        user_id: employee.user_id,
      }) as string;
      const previous = alreadyThere.get(key);
      const active = !employee.is_deleted && employee.status === 'ACTIVE';
      if (!active && !previous) {
        report(`El empleado ${employee.nombres_apellidos} no está activo.`);
        continue;
      }
      raw.push({
        empleado_id: employee.id,
        user_id: employee.user_id,
        horas: hours,
        costo_hora: previous?.costo_hora ?? employee.valor_hora,
      });
      continue;
    }

    if (empleadoId) {
      // El empleado ya no está en el directorio: solo se conserva si ya estaba.
      const previous = alreadyThere.get(`E:${empleadoId}`);
      if (!previous) {
        report('El empleado indicado no existe.');
        continue;
      }
      raw.push({ ...previous, horas: hours });
      continue;
    }

    const previous = alreadyThere.get(`U:${userId}`);
    if (previous) {
      raw.push({ ...previous, horas: hours });
      continue;
    }
    const label = context.userLabel?.(userId as string);
    report(
      `${pickLabel(label?.displayName, label?.username) ?? 'Ese usuario'} no es un empleado. Regístralo en Configuración > Empleados o vincúlalo a su usuario.`,
    );
  }

  return {
    entries: readResponsables(raw, { directory, userLabel: context.userLabel }),
    problems,
  };
}

/** Horas de una lista de responsables. */
export function sumHours(entries: Array<Pick<TaskResponsible, 'horas'>>) {
  return round4(entries.reduce((sum, entry) => sum + entry.horas, 0));
}

/** Costo de la mano de obra de una lista de responsables: horas × costo por hora. */
export function sumLaborCost(
  entries: Array<Pick<TaskResponsible, 'horas' | 'costo_hora'>>,
) {
  return round4(
    entries.reduce(
      (sum, entry) => sum + entry.horas * (entry.costo_hora ?? 0),
      0,
    ),
  );
}

export type TemplateResponsibleDetail = {
  /** El empleado; en lo guardado por usuario sin empleado, el usuario. */
  id: string;
  empleado_id: string | null;
  user_id: string | null;
  nameUser: string | null;
  nameSurname: string | null;
  label: string;
  status: string | null;
  is_deleted: boolean;
};

function readIds(values: unknown) {
  const seen = new Set<string>();
  const ids: string[] = [];
  for (const value of Array.isArray(values) ? values : []) {
    const id = text(value);
    if (!id || seen.has(id)) continue;
    seen.add(id);
    ids.push(id);
  }
  return ids;
}

/**
 * Responsables por defecto de una plantilla (lista de ids), listos para guardar.
 *
 * Se guardan ids de empleado. Un id de usuario con empleado vinculado se cambia
 * por el de su empleado; uno sin empleado solo se conserva si ya estaba en la
 * plantilla.
 */
export function prepareTemplateResponsibles(
  incoming: unknown,
  stored: unknown,
  directory: EmployeeDirectory,
  context: { userLabel?: UserLabelResolver } = {},
): { ids: string[]; problems: string[] } {
  const previous = new Set(readIds(stored));
  const problems: string[] = [];
  const ids: string[] = [];
  const add = (id: string) => {
    if (!ids.includes(id)) ids.push(id);
  };

  for (const id of readIds(incoming)) {
    const employee =
      directory.byId.get(id) ?? directory.byUserId.get(id) ?? null;
    if (employee) {
      const wasThere = previous.has(id) || previous.has(employee.id);
      const active = !employee.is_deleted && employee.status === 'ACTIVE';
      if (!active && !wasThere) {
        problems.push(
          `El empleado ${employee.nombres_apellidos} no está activo.`,
        );
        continue;
      }
      add(employee.id);
      continue;
    }
    if (previous.has(id)) {
      add(id);
      continue;
    }
    const label = context.userLabel?.(id);
    problems.push(
      `${pickLabel(label?.displayName, label?.username) ?? 'Ese responsable'} no es un empleado activo.`,
    );
  }

  return { ids, problems: [...new Set(problems)] };
}

/** Los responsables de una plantilla con su nombre, para mostrarlos. */
export function describeTemplateResponsibles(
  values: unknown,
  directory: EmployeeDirectory,
  context: { userLabel?: UserLabelResolver } = {},
): TemplateResponsibleDetail[] {
  const result: TemplateResponsibleDetail[] = [];
  const seen = new Set<string>();
  for (const id of readIds(values)) {
    const employee =
      directory.byId.get(id) ?? directory.byUserId.get(id) ?? null;
    if (employee) {
      if (seen.has(employee.id)) continue;
      seen.add(employee.id);
      const label = employee.user_id
        ? (context.userLabel?.(employee.user_id) ?? null)
        : null;
      result.push({
        id: employee.id,
        empleado_id: employee.id,
        user_id: employee.user_id,
        nameUser: label?.username ?? null,
        nameSurname: employee.nombres_apellidos,
        label: employee.nombres_apellidos,
        status: employee.status,
        is_deleted: employee.is_deleted,
      });
      continue;
    }
    if (seen.has(id)) continue;
    seen.add(id);
    const label = context.userLabel?.(id) ?? null;
    result.push({
      id,
      empleado_id: null,
      user_id: id,
      nameUser: label?.username ?? null,
      nameSurname: label?.displayName ?? null,
      label:
        pickLabel(label?.displayName, label?.username) ?? 'Usuario asignado',
      status: null,
      is_deleted: false,
    });
  }
  return result;
}
