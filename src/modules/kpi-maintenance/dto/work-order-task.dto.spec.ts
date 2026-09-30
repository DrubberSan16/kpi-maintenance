import { plainToInstance } from 'class-transformer';
import { validate } from 'class-validator';
import {
  CreateWorkOrderTareaDto,
  UpdateWorkOrderTareaDto,
} from './work-order-task.dto';

const EMPLEADO = '11111111-1111-4111-8111-111111111111';
const USUARIO = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa';

/**
 * Los responsables llegan como los arma la pantalla: por empleado, o por usuario
 * (una pestaña vieja o lo guardado antes de que se eligieran de Empleados). El
 * costo de la hora nunca lo manda el cliente.
 */
describe('responsables de una tarea (DTO)', () => {
  const convertir = (responsables: unknown[]) =>
    plainToInstance(UpdateWorkOrderTareaDto, { responsables });

  it('acepta un responsable por empleado', async () => {
    const dto = convertir([{ empleado_id: EMPLEADO, horas: 1.5 }]);
    expect(await validate(dto)).toEqual([]);
    expect(dto.responsables?.[0].empleado_id).toBe(EMPLEADO);
  });

  it('acepta un responsable por usuario', async () => {
    const dto = convertir([{ user_id: USUARIO, horas: 2 }]);
    expect(await validate(dto)).toEqual([]);
    expect(dto.responsables?.[0].user_id).toBe(USUARIO);
  });

  it('rechaza un id que no es un uuid', async () => {
    const dto = convertir([{ empleado_id: 'no-es-un-uuid', horas: 1 }]);
    expect((await validate(dto)).length).toBeGreaterThan(0);
  });

  it('las horas siguen siendo numericas y no negativas', async () => {
    expect(
      (await validate(convertir([{ empleado_id: EMPLEADO, horas: -1 }])))
        .length,
    ).toBeGreaterThan(0);
    expect(
      (await validate(convertir([{ empleado_id: EMPLEADO, horas: 'abc' }])))
        .length,
    ).toBeGreaterThan(0);
  });

  it('la validacion global (whitelist) descarta el costo que el cliente intente mandar', async () => {
    const dto = plainToInstance(CreateWorkOrderTareaDto, {
      plan_id: USUARIO,
      responsables: [
        { empleado_id: EMPLEADO, horas: 1, costo_hora: 999, display_name: 'X' },
      ],
    });
    const errores = await validate(dto, { whitelist: true });
    expect(errores).toEqual([]);
    const responsable = dto.responsables?.[0] as unknown as Record<
      string,
      unknown
    >;
    expect(responsable.costo_hora).toBeUndefined();
    expect(responsable.display_name).toBeUndefined();
    expect(responsable.empleado_id).toBe(EMPLEADO);
  });
});
