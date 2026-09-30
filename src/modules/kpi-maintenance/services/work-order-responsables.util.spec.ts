import {
  buildEmployeeDirectory,
  describeTemplateResponsibles,
  prepareResponsablesForSave,
  prepareTemplateResponsibles,
  readResponsables,
  sumHours,
  sumLaborCost,
  type UserLabelResolver,
} from './work-order-responsables.util';

const EMP_ANA = 'e1000000-0000-4000-8000-000000000001';
const EMP_LUIS = 'e2000000-0000-4000-8000-000000000002';
const EMP_SIN_USUARIO = 'e3000000-0000-4000-8000-000000000003';
const EMP_BAJA = 'e4000000-0000-4000-8000-000000000004';
const EMP_INACTIVO = 'e5000000-0000-4000-8000-000000000005';
const USR_ANA = 'a1000000-0000-4000-8000-000000000001';
const USR_LUIS = 'a2000000-0000-4000-8000-000000000002';
const USR_BAJA = 'a4000000-0000-4000-8000-000000000004';
const USR_SUELTO = 'a9000000-0000-4000-8000-000000000009';

const directory = () =>
  buildEmployeeDirectory([
    {
      id: EMP_ANA,
      user_id: USR_ANA,
      nombres_apellidos: 'ANA PEREZ',
      valor_hora: '2.9167',
      status: 'ACTIVE',
      is_deleted: false,
    },
    {
      id: EMP_LUIS,
      user_id: USR_LUIS,
      nombres_apellidos: 'LUIS GOMEZ',
      valor_hora: '5.0000',
      status: 'ACTIVE',
      is_deleted: false,
    },
    {
      id: EMP_SIN_USUARIO,
      user_id: null,
      nombres_apellidos: 'CARLOS SIN USUARIO',
      valor_hora: '3.75',
      status: 'ACTIVE',
      is_deleted: false,
    },
    {
      id: EMP_BAJA,
      user_id: USR_BAJA,
      nombres_apellidos: 'MARTA DE BAJA',
      valor_hora: '4',
      status: 'ACTIVE',
      is_deleted: true,
    },
    {
      id: EMP_INACTIVO,
      user_id: null,
      nombres_apellidos: 'PEDRO INACTIVO',
      valor_hora: '4',
      status: 'INACTIVE',
      is_deleted: false,
    },
  ]);

const userLabel: UserLabelResolver = (userId) =>
  userId === USR_SUELTO
    ? { username: 'priscila.alarcon', displayName: 'ALARCON PRISCILA' }
    : null;

describe('leer responsables guardados', () => {
  it('lo guardado por usuario se resuelve por el empleado vinculado', () => {
    const [entry] = readResponsables(
      [{ user_id: USR_ANA, username: 'ana', display_name: 'Ana', horas: 2 }],
      { directory: directory() },
    );
    expect(entry).toEqual({
      empleado_id: EMP_ANA,
      user_id: USR_ANA,
      username: 'ana',
      display_name: 'ANA PEREZ',
      horas: 2,
      // Sin costo guardado, se toma el valor por hora del empleado.
      costo_hora: 2.9167,
    });
  });

  it('conserva el costo congelado aunque el empleado ya valga otra cosa', () => {
    const [entry] = readResponsables(
      [{ empleado_id: EMP_ANA, user_id: USR_ANA, horas: 1.5, costo_hora: 2.5 }],
      { directory: directory() },
    );
    expect(entry.costo_hora).toBe(2.5);
  });

  it('un empleado sin usuario es un responsable como cualquiera', () => {
    const [entry] = readResponsables(
      [{ empleado_id: EMP_SIN_USUARIO, horas: 3, costo_hora: 3.75 }],
      { directory: directory() },
    );
    expect(entry).toMatchObject({
      empleado_id: EMP_SIN_USUARIO,
      user_id: null,
      display_name: 'CARLOS SIN USUARIO',
      horas: 3,
      costo_hora: 3.75,
    });
  });

  it('un usuario sin empleado se lee como antes y no tiene costo', () => {
    const [entry] = readResponsables([{ user_id: USR_SUELTO, horas: 2 }], {
      directory: directory(),
      userLabel,
    });
    expect(entry).toEqual({
      empleado_id: null,
      user_id: USR_SUELTO,
      username: 'priscila.alarcon',
      display_name: 'ALARCON PRISCILA',
      horas: 2,
      costo_hora: null,
    });
  });

  it('un empleado dado de baja sigue dando su nombre a lo ya registrado', () => {
    const [entry] = readResponsables(
      [{ empleado_id: EMP_BAJA, horas: 1, costo_hora: 4 }],
      { directory: directory() },
    );
    expect(entry.display_name).toBe('MARTA DE BAJA');
  });

  it('la baja de un empleado no arrastra a otro con su usuario', () => {
    // El usuario de la empleada de baja quedó libre: sin empleado vivo, el
    // usuario se lee como usuario.
    const [entry] = readResponsables([{ user_id: USR_BAJA, horas: 1 }], {
      directory: directory(),
      userLabel: () => ({ username: 'marta', displayName: 'Marta' }),
    });
    expect(entry.empleado_id).toBeNull();
    expect(entry.costo_hora).toBeNull();
  });

  it('suma las horas de una persona repetida y pondera su costo', () => {
    const [entry] = readResponsables(
      [
        { empleado_id: EMP_LUIS, horas: 1, costo_hora: 4 },
        { empleado_id: EMP_LUIS, horas: 3, costo_hora: 8 },
      ],
      { directory: directory() },
    );
    expect(entry.horas).toBe(4);
    expect(entry.costo_hora).toBe(7);
  });

  it('descarta lo que no dice quién es y no imprime ids como nombre', () => {
    const entries = readResponsables(
      [
        { horas: 2 },
        null,
        {
          user_id: USR_SUELTO,
          display_name: USR_SUELTO,
          username: USR_SUELTO,
          horas: 1,
        },
      ],
      { directory: directory() },
    );
    expect(entries).toHaveLength(1);
    expect(entries[0].display_name).toBe('Usuario asignado');
  });

  it('un dato de horas sucio cuenta como cero', () => {
    const entries = readResponsables(
      [
        { empleado_id: EMP_ANA, horas: 'mucho' },
        { empleado_id: EMP_LUIS, horas: -3 },
      ],
      { directory: directory() },
    );
    expect(entries.map((e) => e.horas)).toEqual([0, 0]);
  });

  it('lo que no es una lista no da responsables', () => {
    expect(readResponsables(null)).toEqual([]);
    expect(readResponsables({ a: 1 })).toEqual([]);
  });
});

describe('guardar responsables', () => {
  it('un empleado nuevo entra con el costo de la hora de hoy, sin que se lo pidan al usuario', () => {
    const { entries, problems } = prepareResponsablesForSave(
      [{ empleado_id: EMP_ANA, horas: 2 }],
      [],
      directory(),
    );
    expect(problems).toEqual([]);
    expect(entries).toEqual([
      {
        empleado_id: EMP_ANA,
        user_id: USR_ANA,
        username: null,
        display_name: 'ANA PEREZ',
        horas: 2,
        costo_hora: 2.9167,
      },
    ]);
  });

  it('quien ya estaba conserva su costo aunque el empleado valga otra cosa hoy', () => {
    const stored = [
      { empleado_id: EMP_ANA, user_id: USR_ANA, horas: 1, costo_hora: 2 },
    ];
    const { entries } = prepareResponsablesForSave(
      [{ empleado_id: EMP_ANA, horas: 5 }],
      stored,
      directory(),
    );
    expect(entries[0].horas).toBe(5);
    expect(entries[0].costo_hora).toBe(2);
  });

  it('quien se quita y se vuelve a agregar toma el costo de ese día', () => {
    const stored = [
      { empleado_id: EMP_LUIS, user_id: USR_LUIS, horas: 1, costo_hora: 2 },
    ];
    const quitado = prepareResponsablesForSave([], stored, directory());
    expect(quitado.entries).toEqual([]);
    const vuelto = prepareResponsablesForSave(
      [{ empleado_id: EMP_LUIS, horas: 1 }],
      quitado.entries,
      directory(),
    );
    expect(vuelto.entries[0].costo_hora).toBe(5);
  });

  it('el costo que manda el cliente se ignora', () => {
    const { entries } = prepareResponsablesForSave(
      [{ empleado_id: EMP_ANA, horas: 1, costo_hora: 999 }],
      [],
      directory(),
    );
    expect(entries[0].costo_hora).toBe(2.9167);
  });

  it('no acepta un empleado de baja o inactivo que no estaba, pero sí uno que ya estaba', () => {
    const nuevo = prepareResponsablesForSave(
      [
        { empleado_id: EMP_BAJA, horas: 1 },
        { empleado_id: EMP_INACTIVO, horas: 1 },
      ],
      [],
      directory(),
    );
    expect(nuevo.entries).toEqual([]);
    expect(nuevo.problems).toEqual([
      'El empleado MARTA DE BAJA no está activo.',
      'El empleado PEDRO INACTIVO no está activo.',
    ]);
    const previo = prepareResponsablesForSave(
      [{ empleado_id: EMP_BAJA, horas: 2 }],
      [{ empleado_id: EMP_BAJA, horas: 1, costo_hora: 4 }],
      directory(),
    );
    expect(previo.problems).toEqual([]);
    expect(previo.entries[0]).toMatchObject({ horas: 2, costo_hora: 4 });
  });

  it('una pestaña vieja que manda el usuario se guarda como su empleado', () => {
    const { entries, problems } = prepareResponsablesForSave(
      [{ user_id: USR_LUIS, horas: 1 }],
      [],
      directory(),
    );
    expect(problems).toEqual([]);
    expect(entries[0]).toMatchObject({
      empleado_id: EMP_LUIS,
      user_id: USR_LUIS,
      costo_hora: 5,
    });
  });

  it('un usuario sin empleado se rechaza si es nuevo y se conserva si ya estaba', () => {
    const nuevo = prepareResponsablesForSave(
      [{ user_id: USR_SUELTO, horas: 1 }],
      [],
      directory(),
      { userLabel },
    );
    expect(nuevo.entries).toEqual([]);
    expect(nuevo.problems).toEqual([
      'ALARCON PRISCILA no es un empleado. Regístralo en Configuración > Empleados o vincúlalo a su usuario.',
    ]);
    const previo = prepareResponsablesForSave(
      [{ user_id: USR_SUELTO, horas: 3 }],
      [{ user_id: USR_SUELTO, horas: 2 }],
      directory(),
      { userLabel },
    );
    expect(previo.problems).toEqual([]);
    expect(previo.entries[0]).toMatchObject({
      empleado_id: null,
      user_id: USR_SUELTO,
      horas: 3,
      costo_hora: null,
    });
  });

  it('un usuario que ya estaba y hoy tiene empleado pasa a ser ese empleado', () => {
    const { entries } = prepareResponsablesForSave(
      [{ user_id: USR_ANA, horas: 4 }],
      [{ user_id: USR_ANA, horas: 1 }],
      directory(),
    );
    expect(entries).toHaveLength(1);
    expect(entries[0]).toMatchObject({ empleado_id: EMP_ANA, horas: 4 });
  });

  it('dice qué falla: horas inválidas, sin id o empleado inexistente', () => {
    const { entries, problems } = prepareResponsablesForSave(
      [
        { empleado_id: EMP_ANA, horas: -1 },
        { horas: 2 },
        { empleado_id: 'e9999999-0000-4000-8000-000000000009', horas: 1 },
      ],
      [],
      directory(),
    );
    expect(entries).toEqual([]);
    expect(problems).toEqual([
      'Las horas registradas por responsable deben ser numéricas y mayores o iguales a cero.',
      'Cada responsable de la tarea debe ser un empleado.',
      'El empleado indicado no existe.',
    ]);
  });

  it('agrupa a la misma persona repetida', () => {
    const { entries } = prepareResponsablesForSave(
      [
        { empleado_id: EMP_ANA, horas: 1 },
        { empleado_id: EMP_ANA, horas: 2.5 },
      ],
      [],
      directory(),
    );
    expect(entries).toHaveLength(1);
    expect(entries[0].horas).toBe(3.5);
  });
});

describe('costo de la mano de obra', () => {
  const entries = [
    { horas: 2, costo_hora: 3 },
    { horas: 1.5, costo_hora: 4 },
    { horas: 4, costo_hora: null },
  ];

  it('suma las horas y multiplica cada una por su costo; sin costo no suma importe', () => {
    expect(sumHours(entries)).toBe(7.5);
    expect(sumLaborCost(entries)).toBe(12);
  });

  it('no arrastra el error de los decimales', () => {
    expect(
      sumLaborCost([
        { horas: 0.1, costo_hora: 0.2 },
        { horas: 0.2, costo_hora: 0.1 },
      ]),
    ).toBe(0.04);
  });
});

describe('responsables por defecto de las plantillas', () => {
  it('cambia el id de un usuario con empleado por el id de su empleado', () => {
    const { ids, problems } = prepareTemplateResponsibles(
      [USR_ANA, EMP_LUIS, USR_ANA],
      [],
      directory(),
    );
    expect(problems).toEqual([]);
    expect(ids).toEqual([EMP_ANA, EMP_LUIS]);
  });

  it('rechaza un usuario sin empleado si es nuevo y lo conserva si ya estaba', () => {
    const nuevo = prepareTemplateResponsibles([USR_SUELTO], [], directory(), {
      userLabel,
    });
    expect(nuevo.ids).toEqual([]);
    expect(nuevo.problems).toEqual([
      'ALARCON PRISCILA no es un empleado activo.',
    ]);
    const previo = prepareTemplateResponsibles(
      [USR_SUELTO, EMP_ANA],
      [USR_SUELTO],
      directory(),
    );
    expect(previo.problems).toEqual([]);
    expect(previo.ids).toEqual([USR_SUELTO, EMP_ANA]);
  });

  it('un empleado de baja o inactivo solo se conserva si ya estaba', () => {
    expect(
      prepareTemplateResponsibles([EMP_BAJA], [], directory()).problems,
    ).toEqual(['El empleado MARTA DE BAJA no está activo.']);
    expect(
      prepareTemplateResponsibles([EMP_BAJA], [EMP_BAJA], directory()).ids,
    ).toEqual([EMP_BAJA]);
  });

  it('describe cada responsable con su nombre, sea empleado o usuario suelto', () => {
    const detail = describeTemplateResponsibles(
      [EMP_ANA, USR_LUIS, USR_SUELTO, EMP_ANA],
      directory(),
      {
        userLabel: (id) =>
          id === USR_LUIS
            ? { username: 'luis', displayName: 'Luis' }
            : userLabel(id),
      },
    );
    expect(detail).toHaveLength(3);
    expect(detail[0]).toMatchObject({
      id: EMP_ANA,
      empleado_id: EMP_ANA,
      label: 'ANA PEREZ',
    });
    // Un id de usuario con empleado vinculado se muestra como su empleado.
    expect(detail[1]).toMatchObject({
      id: EMP_LUIS,
      user_id: USR_LUIS,
      nameUser: 'luis',
      label: 'LUIS GOMEZ',
    });
    expect(detail[2]).toMatchObject({
      id: USR_SUELTO,
      empleado_id: null,
      label: 'ALARCON PRISCILA',
    });
  });
});
